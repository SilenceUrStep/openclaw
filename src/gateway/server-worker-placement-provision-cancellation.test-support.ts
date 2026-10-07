import { vi } from "vitest";
import { getRuntimeConfig } from "../config/config.js";
import { createDeferredCore } from "../shared/deferred.js";
import { createTestGatewayScheduler } from "../test-utils/gateway-scheduler-clock.js";
import type { WorkerPlacementSessionWorkCancellation } from "./server-worker-placement-cancel.js";
import { createGatewayWorkerPlacementRuntime } from "./server-worker-placement-startup.js";
import type { createWorkerSessionPlacementStore } from "./worker-environments/placement-store.js";
import type * as support from "./worker-environments/service.test-support.js";

export function createHeldWorkspacePreflight() {
  const entered = createDeferredCore();
  const aborted = createDeferredCore();
  const release = createDeferredCore();
  let signal: AbortSignal | undefined;
  return {
    entered: entered.promise,
    aborted: aborted.promise,
    release: release.resolve,
    get signal() {
      return signal;
    },
    async run(this: void, request: { signal?: AbortSignal }) {
      signal = request.signal;
      signal?.addEventListener("abort", () => aborted.resolve(), { once: true });
      entered.resolve();
      await release.promise;
      signal?.throwIfAborted();
    },
  };
}

export function createRuntime(
  placements: ReturnType<typeof createWorkerSessionPlacementStore>,
  environments: ReturnType<typeof support.createService>,
  cancelSessionWork: WorkerPlacementSessionWorkCancellation = vi.fn(async () => {}),
) {
  return createGatewayWorkerPlacementRuntime({
    scheduler: createTestGatewayScheduler(),
    getCommittedRuntimeConfig: getRuntimeConfig,
    placements,
    environments,
    gatewayNamespace: "gateway-test",
    warn: vi.fn(),
    cancelSessionWork,
    revokeSessionAuthority: vi.fn(),
  });
}
