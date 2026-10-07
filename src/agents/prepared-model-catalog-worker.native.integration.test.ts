import fs from "node:fs";
import path from "node:path";
import { BroadcastChannel, threadId } from "node:worker_threads";
import { describe, expect, it, vi } from "vitest";
import {
  awaitGateBeforeSettlement,
  createDeferred,
  withinTest,
} from "../../test/helpers/promise.js";
import { loadPluginMetadataSnapshot } from "../plugins/plugin-metadata-snapshot.js";
import {
  createPreparedModelCatalogWorker,
  PREPARED_MODEL_CATALOG_WORKER_TIMEOUT_MS,
} from "./prepared-model-catalog-worker.js";
import {
  createCatalogFixture,
  expectNativeHarnessModelsPublishedFromWorker,
  PROVIDER_ID,
} from "./prepared-model-catalog-worker.test-support.js";
import { AuthStorage } from "./sessions/auth-storage.js";
import { expectLegacyWorkerCatalogRetention } from "./test-helpers/prepared-model-catalog-legacy-fixture.js";
import { usePreparedCatalogWorkerFixtures } from "./test-helpers/prepared-model-catalog-worker-fixture.js";

const { makeTempDir, retireAfterTest, readCatalogWorkers } = usePreparedCatalogWorkerFixtures();

describe("prepared native model catalog worker boundary", () => {
  it("retains configured dynamic models alongside native harness models after full refresh", async () => {
    await expectNativeHarnessModelsPublishedFromWorker({ makeTempDir, retireAfterTest });
  });

  it.each([
    { catalogReturnsRows: true, aliasOnly: false },
    { catalogReturnsRows: false, aliasOnly: false },
    { catalogReturnsRows: true, aliasOnly: true },
  ])("refreshes legacy catalogs (rows=$catalogReturnsRows, alias=$aliasOnly)", async (options) => {
    await expectLegacyWorkerCatalogRetention({
      makeTempDir,
      retireAfterTest,
      ...options,
    });
  });
});

it("finishes slow native admission once across catalog refreshes and still bounds provider work", async ({
  signal,
}) => {
  const fixture = await createCatalogFixture(makeTempDir, 0);
  const pluginRoot = path.join(fixture.root, "plugin");
  const dependency = path.join(pluginRoot, "node_modules", "native-fixture");
  fs.mkdirSync(dependency, { recursive: true });
  fs.writeFileSync(
    path.join(dependency, "package.json"),
    JSON.stringify({ name: "native-fixture", version: "1.0.0", main: "index.cjs" }),
  );
  fs.writeFileSync(
    path.join(dependency, "index.cjs"),
    'module.exports = require.resolve("./artifact-0.exe");',
  );
  const binary = path.join(dependency, "artifact-0.exe");
  fs.writeFileSync(binary, "synthetic native companion");
  for (let member = 1; member < 33; member++) {
    fs.linkSync(binary, path.join(dependency, `artifact-${member}.exe`));
  }
  for (let member = 0; member < 21; member++) {
    fs.writeFileSync(path.join(dependency, `companion-${member}.txt`), "companion");
  }
  const admissionMarker = path.join(fixture.root, "native-admissions.txt");
  const holdProvider = path.join(fixture.root, "hold-provider");
  const broadcastName = `catalog-admission:${fixture.root}`;
  fs.writeFileSync(
    path.join(pluginRoot, "index.cjs"),
    `const fs = require("node:fs");
const { BroadcastChannel, threadId } = require("node:worker_threads");
const receipts = new BroadcastChannel(${JSON.stringify(broadcastName)});
receipts.unref();
if (threadId !== ${threadId}) {
  const realpath = fs.realpathSync;
  let held = false;
  fs.realpathSync = Object.assign(function(filename, ...options) {
    const resolved = realpath(filename, ...options);
    if (!held && String(filename).includes("admission-") && String(filename).endsWith("artifact-0.exe")) {
      held = true;
      fs.appendFileSync(${JSON.stringify(admissionMarker)}, JSON.stringify({ event: "started", filename: __filename, native: resolved }) + "\\n");
      const gate = new Int32Array(new SharedArrayBuffer(4));
      receipts.postMessage(gate.buffer);
      Atomics.wait(gate, 0, 0);
    }
    return resolved;
  }, realpath);
  try {
    const native = require("native-fixture");
    fs.appendFileSync(${JSON.stringify(admissionMarker)}, JSON.stringify({ event: "admitted", filename: __filename, native }) + "\\n");
  } finally {
    fs.realpathSync = realpath;
  }
}
module.exports = { id: ${JSON.stringify(PROVIDER_ID)}, register(api) {
  api.registerProvider({ id: ${JSON.stringify(PROVIDER_ID)}, label: "Admission fixture", auth: [],
    catalog: { async run() {
      if (fs.existsSync(${JSON.stringify(holdProvider)})) {
        receipts.postMessage("provider");
        await new Promise(() => {});
      }
      return { provider: { api: "openai-completions", baseUrl: "https://fixture.invalid/v1",
        models: [{ id: "native-admitted", name: "Admitted model" }] } };
    } },
  });
} };`,
  );
  const entered = createDeferred<Int32Array<SharedArrayBuffer>>();
  const providerEntered = createDeferred();
  const broadcast = new BroadcastChannel(broadcastName);
  broadcast.addEventListener("message", ({ data }) => {
    if (data instanceof SharedArrayBuffer) {
      entered.resolve(new Int32Array(data));
    } else if (data === "provider") {
      providerEntered.resolve();
    }
  });
  const retirement = new AbortController();
  retireAfterTest(() => retirement.abort());
  const worker = createPreparedModelCatalogWorker({
    agentFacts: {
      input: {
        config: fixture.config,
        agentDir: fixture.agentDir,
        workspaceDir: fixture.workspaceDir,
        env: fixture.env,
      },
      env: fixture.env,
      authStore: { version: 1, profiles: {} },
      credentials: {},
      providerIds: [PROVIDER_ID],
      configuredModelRefs: [],
      configuredRuntimeModels: [],
      runtimeCapabilityModels: [],
      configuredGeneratedCatalogPluginIds: [],
      templateAuthStorage: AuthStorage.inMemory({}),
    },
    pluginMetadataSnapshot: loadPluginMetadataSnapshot({
      config: fixture.config,
      env: fixture.env,
      workspaceDir: fixture.workspaceDir,
    }),
    isCurrent: () => !retirement.signal.aborted,
    retirementSignal: retirement.signal,
  });
  let gate: Int32Array<SharedArrayBuffer> | undefined;
  let pending: ReturnType<typeof worker.loadCatalog> | undefined;
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    let settled = false;
    pending = worker.loadCatalog().finally(() => {
      settled = true;
    });
    void pending.catch(() => {});
    gate = await withinTest(
      awaitGateBeforeSettlement(entered.promise, pending, "Native capture did not enter"),
      signal,
    );
    await vi.advanceTimersByTimeAsync(PREPARED_MODEL_CATALOG_WORKER_TIMEOUT_MS * 2);
    expect(settled).toBe(false);
    expect(readCatalogWorkers()).toHaveLength(1);
    expect(readCatalogWorkers()[0]!.threadId).not.toBe(-1);
    Atomics.store(gate, 0, 1);
    Atomics.notify(gate, 0);
    const first = await withinTest(pending, signal);
    expect(first.modelCatalog.entries).toContainEqual(
      expect.objectContaining({ provider: PROVIDER_ID, id: "native-admitted" }),
    );
    const refreshed = await withinTest(worker.loadCatalog(), signal);
    expect(refreshed.modelCatalog.entries).toEqual(first.modelCatalog.entries);
    const admissions = fs.readFileSync(admissionMarker, "utf8");
    const records = admissions
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line));
    expect(records).toEqual([
      { event: "started", filename: expect.any(String), native: expect.any(String) },
      { event: "admitted", filename: records[0].filename, native: expect.any(String) },
    ]);
    expect(fs.existsSync(records[1].native)).toBe(true);
    expect(records[0].native).toContain(`${path.sep}native${path.sep}admission-`);
    await withinTest(worker.loadCatalog(), signal);
    expect(fs.readFileSync(admissionMarker, "utf8")).toBe(admissions);
    expect(readCatalogWorkers()).toHaveLength(1);

    fs.writeFileSync(holdProvider, "");
    pending = worker.loadCatalog();
    void pending.catch(() => {});
    await withinTest(
      awaitGateBeforeSettlement(providerEntered.promise, pending, "Provider hook did not enter"),
      signal,
    );
    await vi.advanceTimersByTimeAsync(PREPARED_MODEL_CATALOG_WORKER_TIMEOUT_MS);
    await expect(pending).rejects.toMatchObject({ name: "WorkerTaskError", code: "timeout" });
  } finally {
    if (gate) {
      Atomics.store(gate, 0, 1);
      Atomics.notify(gate, 0);
    }
    retirement.abort();
    vi.useRealTimers();
    broadcast.close();
    await Promise.allSettled([pending]);
  }
});
