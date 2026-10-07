import { AsyncLocalStorage } from "node:async_hooks";
import type { Result } from "@openclaw/normalization-core/result";
import { cloneEnvWithPlatformSemantics } from "../config/config-env-vars.js";
import { retainSqliteWorkerErrorCode } from "../infra/sqlite-worker-contract.js";
import {
  assertExistingDatabaseIdentity,
  readDatabasePathIdentitySync,
  type DatabasePathIdentity,
} from "../infra/sqlite-worker-identity.js";
import { createDeferredCore } from "../shared/deferred.js";
import { AgentDatabaseExecutionAdmissionClosedError } from "./agent-database-admission-error.js";
import { getAgentDeletionDatabaseCleanup } from "./agent-deletion-cleanup.js";
import type { OpenClawAgentDatabaseOptions } from "./openclaw-agent-db-contract.js";
import { hasAgentDatabaseMaintenanceAuthority } from "./openclaw-agent-db-lease.js";
import {
  isIncognitoOpenClawAgentSqlitePath,
  resolveOpenClawAgentSqlitePath,
} from "./openclaw-agent-db.paths.js";
import type {
  AgentDatabaseExecutionFileIdentity,
  AgentDatabaseExecutionScope,
  AgentDatabaseGenerationClaim,
  AgentDatabaseNativeGeneration,
  AgentDatabaseRequestExecutionSource,
} from "./openclaw-agent-execution-contract.js";
import { getOpenClawDatabaseMaintenanceScope } from "./openclaw-state-db-async-lifecycle.js";
import { resolveOpenClawStateSqlitePath } from "./openclaw-state-db.paths.js";
import { captureOpenClawStateReadContext } from "./openclaw-state-worker-context.js";

/** Retain entered callbacks, not reusable references, through physical generation settlement. */
export function createAgentDatabaseAcceptedOperationScope(params: {
  assertCurrent(): void;
  readConfigGeneration(): number;
  readGeneration(): AgentDatabaseNativeGeneration | undefined;
  reportCleanupFailure: (error: unknown) => void;
}) {
  type AcceptedOperation = {
    borrower: object;
    generation: AgentDatabaseNativeGeneration;
    configGeneration: number;
    active: boolean;
    settled: Promise<void>;
  };
  const context = new AsyncLocalStorage<AcceptedOperation>();
  const operations = new Set<AcceptedOperation>();
  const matchesCurrent = (
    generation: AgentDatabaseNativeGeneration | undefined,
    configGeneration?: number,
    borrower?: object,
  ) => {
    const accepted = context.getStore();
    return Boolean(
      accepted?.active &&
      accepted.generation === generation &&
      (configGeneration === undefined || accepted.configGeneration === configGeneration) &&
      (borrower === undefined || accepted.borrower === borrower),
    );
  };
  const assertConfigCurrent = (captured: number, accepted = false) => {
    params.assertCurrent();
    if (captured !== params.readConfigGeneration() && !accepted) {
      throw new AgentDatabaseExecutionAdmissionClosedError(
        "Agent database execution admission is closed",
      );
    }
  };
  return {
    assertConfigCurrent(captured: number) {
      assertConfigCurrent(captured);
    },
    assertNativeConfigCurrent(captured: number) {
      assertConfigCurrent(captured, matchesCurrent(params.readGeneration(), captured));
    },
    assertBorrowerConfigCurrent(captured: number, borrower: object) {
      assertConfigCurrent(captured, matchesCurrent(params.readGeneration(), captured, borrower));
    },
    async run<T>(
      generation: AgentDatabaseNativeGeneration,
      borrower: object | undefined,
      source: AgentDatabaseRequestExecutionSource,
      operation: (scope: AgentDatabaseExecutionScope) => Promise<T>,
      assertCallerCurrent: (identity?: AgentDatabaseExecutionFileIdentity) => void,
      createIfMissing: boolean,
      signal: AbortSignal | undefined,
      readmitSchema: boolean,
    ): Promise<{
      outcome: Result<T | undefined, unknown>;
      nativeFailure: ReturnType<AgentDatabaseNativeGeneration["failure"]>;
      entered: boolean;
    }> {
      let entered = false;
      let release: (() => void) | undefined;
      const enter = (scope: AgentDatabaseExecutionScope): Promise<T> => {
        entered = true;
        if (!borrower) {
          return operation(scope);
        }
        const completion = createDeferredCore();
        const accepted: AcceptedOperation = {
          borrower,
          generation,
          configGeneration: params.readConfigGeneration(),
          active: true,
          settled: completion.promise,
        };
        operations.add(accepted);
        release = () => {
          accepted.active = false;
          operations.delete(accepted);
          completion.resolve();
        };
        return context.run(accepted, () => operation(scope));
      };
      let outcome: Result<T | undefined, unknown>;
      try {
        // Opening, schema admission, and nested calls cannot inherit an accepted parent.
        const value = await context.exit(() =>
          generation.run(
            source,
            enter,
            assertCallerCurrent,
            createIfMissing,
            signal,
            readmitSchema,
          ),
        );
        outcome = { ok: true, value };
      } catch (error) {
        outcome = { ok: false, error };
      }
      // The broker has settled its commands. Capture failure before releasing a pending close.
      const nativeFailure = generation.failure();
      release?.();
      return { outcome, nativeFailure, entered };
    },
    closeAfterSettlement(generation: AgentDatabaseNativeGeneration): Promise<void> {
      const settling = [...operations]
        .filter((operation) => operation.generation === generation)
        .map((operation) => operation.settled);
      // Native close seals the generation immediately; accepted scopes must finish first.
      return settling.length
        ? Promise.allSettled(settling).then(() => generation.close())
        : generation.close();
    },
    async joinClose(generation: AgentDatabaseNativeGeneration | undefined, close: Promise<void>) {
      if (matchesCurrent(generation)) {
        void close.catch(params.reportCleanupFailure);
        throw new AgentDatabaseExecutionAdmissionClosedError(
          "Agent database execution admission is closed",
        );
      }
      await close;
    },
    async settleCleanup(
      generation: AgentDatabaseNativeGeneration,
      cleanup: Promise<void>,
      failure?: { error: unknown; message: string },
    ) {
      try {
        if (matchesCurrent(generation)) {
          // Cleanup still owns the close; an enclosing callback cannot await its own settlement.
          void cleanup.catch(params.reportCleanupFailure);
        } else {
          await cleanup;
        }
      } catch (cleanupError) {
        if (!failure) {
          throw cleanupError;
        }
        throw retainSqliteWorkerErrorCode(
          new AggregateError([failure.error, cleanupError], failure.message, {
            cause: failure.error,
          }),
          failure.error,
        );
      }
    },
    dispose() {
      context.disable();
    },
  };
}

export function assertAgentDatabaseExecutionSharedState(
  options: OpenClawAgentDatabaseOptions,
  sharedDatabaseKey: string,
): void {
  const env =
    process.platform === "win32"
      ? cloneEnvWithPlatformSemantics(options.env ?? process.env)
      : options.env;
  const state = captureOpenClawStateReadContext(resolveOpenClawStateSqlitePath(env));
  if (sharedDatabaseKey !== state.admission.identity.key) {
    throw new Error(
      "Agent database execution belongs to another shared-state database; drain its existing resources before changing the state directory.",
    );
  }
}

export function supportsAgentDatabaseExecutionScope(
  options: OpenClawAgentDatabaseOptions,
): boolean {
  return (
    getOpenClawDatabaseMaintenanceScope()?.ownsSchemaMaintenance !== true &&
    !hasAgentDatabaseMaintenanceAuthority() &&
    !getAgentDeletionDatabaseCleanup(options)
  );
}

/** These native-only scopes still need their complete owning caller cutover. */
export function supportsOpenClawAgentDatabaseExecution(
  options: OpenClawAgentDatabaseOptions,
): boolean {
  return (
    !isIncognitoOpenClawAgentSqlitePath(resolveOpenClawAgentSqlitePath(options), options) &&
    supportsAgentDatabaseExecutionScope(options)
  );
}

/** Each alias and retained file receipt must still name the borrower's original store. */
export function assertBorrowedAgentDatabaseFileIdentity({
  borrowedPath,
  identity,
  creatingTarget,
  fileIdentity,
  expectedIdentity,
  nativeIdentity,
}: {
  borrowedPath: string;
  identity: DatabasePathIdentity;
  creatingTarget: DatabasePathIdentity | undefined;
  fileIdentity: AgentDatabaseExecutionFileIdentity | undefined;
  expectedIdentity: AgentDatabaseExecutionFileIdentity | undefined;
  nativeIdentity: AgentDatabaseExecutionFileIdentity | undefined;
}): void {
  if (!fileIdentity || creatingTarget) {
    const current = readDatabasePathIdentitySync(borrowedPath);
    if (
      current.canonicalPath !== identity.canonicalPath ||
      (creatingTarget?.key.startsWith("file:") &&
        (current.key !== creatingTarget.key || current.birthtime !== creatingTarget.birthtime))
    ) {
      throw new Error("Agent database borrower changed its originally observed target");
    }
  }
  if (
    fileIdentity &&
    expectedIdentity &&
    (fileIdentity.physicalIdentity !== expectedIdentity.physicalIdentity ||
      (fileIdentity.birthtime !== undefined &&
        expectedIdentity.birthtime !== undefined &&
        fileIdentity.birthtime !== expectedIdentity.birthtime))
  ) {
    throw new Error("Agent database borrower belongs to another physical file");
  }
  const file = fileIdentity ?? expectedIdentity;
  const birthtime = fileIdentity?.birthtime ?? expectedIdentity?.birthtime;
  if (file) {
    if (
      nativeIdentity &&
      (nativeIdentity.physicalIdentity !== file.physicalIdentity ||
        (birthtime !== undefined && nativeIdentity.birthtime !== birthtime))
    ) {
      throw new Error("Agent database borrower belongs to another physical file");
    }
    // The native owner validates its own path last; a borrowed alias has a separate lifetime.
    if (!nativeIdentity || borrowedPath !== nativeIdentity.nativeLocation) {
      assertExistingDatabaseIdentity(borrowedPath, `file:${file.physicalIdentity}`, birthtime);
    }
  }
}

/** Bind a native claim to the same borrower and logical generation that captured it. */
export function captureBorrowedAgentDatabaseGenerationClaim(
  assertBorrowed: () => void,
  readGeneration: () => AgentDatabaseNativeGeneration | undefined,
): AgentDatabaseGenerationClaim {
  assertBorrowed();
  const captured = readGeneration();
  if (!captured) {
    throw new Error("Agent database execution has no admitted generation");
  }
  const claim = captured.captureClaim();
  return {
    identity: claim.identity,
    incarnation: claim.incarnation,
    assertCurrent() {
      assertBorrowed();
      if (readGeneration() !== captured) {
        throw new Error("Agent database execution generation was replaced");
      }
      claim.assertCurrent();
    },
  };
}
