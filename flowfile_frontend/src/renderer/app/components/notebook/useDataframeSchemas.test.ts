import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/api/lsp.api", () => ({
  LspApi: { capabilities: vi.fn(), dataframeSchemas: vi.fn() },
}));
vi.mock("@/api/catalog.api", () => ({
  CatalogApi: { resolveTableStrict: vi.fn() },
}));
vi.mock("@/stores/catalog-store", () => ({
  useCatalogStore: () => ({ tree: [] }),
}));

import { LspApi } from "@/api/lsp.api";
import { CatalogApi } from "@/api/catalog.api";
import type { CatalogRefLiteral } from "../nodes/node-types/elements/pythonScript/dataframeSchemaTypes";
import { resetCatalogRefCache } from "./catalogRefResolver";
import {
  beginExecution,
  bumpSessionEpoch,
  disposeOwner,
  endBatch,
  settleExecution,
  startBatch,
} from "./notebookRuntimeState";
import {
  attachDataframeSchemas,
  getSchemas,
  noteExecution,
  refresh,
  resetDataframeSchemas,
} from "./useDataframeSchemas";

const mockCaps = LspApi.capabilities as unknown as ReturnType<typeof vi.fn>;
const mockSchemas = LspApi.dataframeSchemas as unknown as ReturnType<typeof vi.fn>;
const mockResolveTable = CatalogApi.resolveTableStrict as unknown as ReturnType<typeof vi.fn>;

const OWNER = "node:1:2";

interface Ctx {
  kernelId: string | null;
  flowId: number;
  nodeId?: number | null;
  catalogRefs: () => CatalogRefLiteral[];
}

let ctx: Ctx;
const getCtx = () => ctx;

const frame = (name: string, columns: { name: string; dtype: string }[]) => ({
  name,
  kind: "DataFrame",
  state: "ready",
  columns,
  truncated: false,
});

const ready = (dataframes: unknown[], generation = "g1", revision = 1) => ({
  namespace_generation: generation,
  revision,
  state: "ready",
  dataframes,
});

const busy = (generation = "g1", revision = 1) => ({
  namespace_generation: generation,
  revision,
  state: "busy",
  dataframes: [],
});

const unavailable = () => ({
  namespace_generation: "",
  revision: 0,
  state: "unavailable",
  dataframes: [],
});

/** Lets the refresh chain (capabilities → request → store) run to completion. */
const tick = async () => {
  for (let i = 0; i < 6; i += 1) await Promise.resolve();
};

beforeEach(() => {
  vi.clearAllMocks();
  resetDataframeSchemas();
  resetCatalogRefCache();
  disposeOwner(OWNER);
  ctx = { kernelId: "k1", flowId: 7, nodeId: 2, catalogRefs: () => [] };
  mockCaps.mockResolvedValue({ enabled: true, version: "", features: ["dataframe_schemas"] });
  mockSchemas.mockResolvedValue(ready([frame("orders", [{ name: "id", dtype: "Int64" }])]));
  mockResolveTable.mockResolvedValue(null);
});

afterEach(() => {
  resetDataframeSchemas();
  disposeOwner(OWNER);
  vi.useRealTimers();
});

describe("attachDataframeSchemas", () => {
  it("refreshes once on attach with a kernel and stores the frames", async () => {
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);

    expect(mockSchemas).toHaveBeenCalledTimes(1);
    expect(mockSchemas).toHaveBeenCalledWith("k1", { flow_id: 7, node_id: 2 });
    const entry = getSchemas(OWNER);
    expect(entry?.kernelId).toBe("k1");
    expect(entry?.generation).toBe("g1");
    expect(entry?.frames.get("orders")).toEqual({
      kind: "DataFrame",
      state: "ready",
      columns: [{ name: "id", dtype: "Int64" }],
      truncated: false,
    });
  });

  it("makes no request without a kernel but still resolves catalog refs", async () => {
    ctx = { kernelId: null, flowId: 7, catalogRefs: () => [{ table: "orders" }] };
    attachDataframeSchemas(OWNER, getCtx);
    await tick();

    expect(mockSchemas).not.toHaveBeenCalled();
    expect(getSchemas(OWNER)).toBeNull();
    expect(mockResolveTable).toHaveBeenCalledWith("orders");
  });

  it("makes no request when the LSP is disabled", async () => {
    mockCaps.mockResolvedValue({ enabled: false, version: "", features: [] });
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);

    expect(mockSchemas).not.toHaveBeenCalled();
    expect(getSchemas(OWNER)).toBeNull();
  });

  it("dedupes an in-flight refresh", async () => {
    attachDataframeSchemas(OWNER, getCtx);
    await Promise.all([refresh(OWNER), refresh(OWNER), refresh(OWNER)]);

    expect(mockSchemas).toHaveBeenCalledTimes(1);
  });

  it("discards a response whose generation differs from the last one settled", async () => {
    attachDataframeSchemas(OWNER, getCtx);
    noteExecution(OWNER, { namespace_generation: "g2", revision: 4 });
    mockSchemas.mockResolvedValue(ready([frame("orders", [])], "g1", 4));
    await refresh(OWNER);

    expect(mockSchemas).toHaveBeenCalledTimes(1);
    expect(getSchemas(OWNER)).toBeNull();
  });

  it("discards a response older than the cached revision", async () => {
    mockSchemas.mockResolvedValue(
      ready([frame("orders", [{ name: "id", dtype: "Int64" }])], "g1", 5),
    );
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);

    mockSchemas.mockResolvedValue(ready([frame("stale", [])], "g1", 4));
    await refresh(OWNER);

    const entry = getSchemas(OWNER);
    expect(entry?.revision).toBe(5);
    expect(entry?.frames.has("stale")).toBe(false);
  });

  it("discards a response whose kernel changed while it was in flight", async () => {
    attachDataframeSchemas(OWNER, getCtx);
    let release: (v: unknown) => void = () => undefined;
    mockSchemas.mockReturnValue(
      new Promise((resolve) => {
        release = resolve;
      }),
    );
    const pending = refresh(OWNER);
    await tick();
    ctx = { ...ctx, kernelId: "k2" };
    release(ready([frame("orders", [])]));
    await pending;

    expect(getSchemas(OWNER)).toBeNull();
  });

  it("keeps the entry on busy and retries once", async () => {
    vi.useFakeTimers();
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);
    expect(getSchemas(OWNER)?.frames.has("orders")).toBe(true);

    mockSchemas.mockResolvedValue(busy());
    await refresh(OWNER);
    expect(getSchemas(OWNER)?.frames.has("orders")).toBe(true);
    expect(mockSchemas).toHaveBeenCalledTimes(2);

    vi.advanceTimersByTime(1500);
    await tick();
    expect(mockSchemas).toHaveBeenCalledTimes(3);

    // The retry is a single attempt: a second busy answer must not schedule another.
    vi.advanceTimersByTime(5000);
    await tick();
    expect(mockSchemas).toHaveBeenCalledTimes(3);
    expect(getSchemas(OWNER)?.frames.has("orders")).toBe(true);
  });

  it("drops the entry when the namespace is unavailable", async () => {
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);
    expect(getSchemas(OWNER)).not.toBeNull();

    mockSchemas.mockResolvedValue(unavailable());
    await refresh(OWNER);
    expect(getSchemas(OWNER)).toBeNull();
  });

  it("invalidates then refreshes on a session epoch bump", async () => {
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);
    expect(getSchemas(OWNER)).not.toBeNull();

    mockSchemas.mockResolvedValue(ready([frame("fresh", [])], "g1", 2));
    bumpSessionEpoch(OWNER);
    expect(getSchemas(OWNER)).toBeNull();

    await tick();
    expect(mockSchemas).toHaveBeenCalledTimes(2);
    expect(getSchemas(OWNER)?.frames.has("fresh")).toBe(true);
  });

  it("accepts the new kernel's generation after an epoch bump", async () => {
    vi.useFakeTimers();
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);

    settleExecution(beginExecution(OWNER, "cell-a"), { namespace_generation: "g1", revision: 2 });
    vi.advanceTimersByTime(0);
    await tick();

    mockSchemas.mockResolvedValue(ready([frame("fresh", [])], "g2", 1));
    bumpSessionEpoch(OWNER);
    await tick();

    expect(getSchemas(OWNER)?.generation).toBe("g2");
    expect(getSchemas(OWNER)?.frames.has("fresh")).toBe(true);
  });

  it("drops the cached entry when a settle reports a new generation", async () => {
    vi.useFakeTimers();
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);
    expect(getSchemas(OWNER)?.generation).toBe("g1");

    const ticket = beginExecution(OWNER, "cell-a");
    settleExecution(ticket, { namespace_generation: "g2", revision: 9 });
    expect(getSchemas(OWNER)).toBeNull();
  });

  it("defers a settle inside a batch to one refresh after the batch ends", async () => {
    vi.useFakeTimers();
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);
    mockSchemas.mockClear();
    mockSchemas.mockResolvedValue(ready([frame("orders", [])], "g1", 3));

    const batchId = startBatch(OWNER, ["cell-a", "cell-b"]);
    expect(batchId).not.toBeNull();

    settleExecution(beginExecution(OWNER, "cell-a"), {
      namespace_generation: "g1",
      revision: 2,
    });
    vi.advanceTimersByTime(0);
    await tick();
    expect(mockSchemas).not.toHaveBeenCalled();

    settleExecution(beginExecution(OWNER, "cell-b"), {
      namespace_generation: "g1",
      revision: 3,
    });
    endBatch(OWNER, batchId as number);
    vi.advanceTimersByTime(0);
    await tick();
    expect(mockSchemas).toHaveBeenCalledTimes(1);
  });

  it("ignores a discarded settle", async () => {
    vi.useFakeTimers();
    attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);
    mockSchemas.mockClear();

    const ticket = beginExecution(OWNER, "cell-a");
    beginExecution(OWNER, "cell-a"); // supersedes the first request
    expect(settleExecution(ticket, { namespace_generation: "g2" })).toBe("discard");
    vi.advanceTimersByTime(0);
    await tick();

    expect(mockSchemas).not.toHaveBeenCalled();
    expect(getSchemas(OWNER)?.generation).toBe("g1");
  });

  it("stops listening and drops its cache once unsubscribed", async () => {
    vi.useFakeTimers();
    const off = attachDataframeSchemas(OWNER, getCtx);
    await refresh(OWNER);
    expect(getSchemas(OWNER)).not.toBeNull();

    off();
    expect(getSchemas(OWNER)).toBeNull();
    mockSchemas.mockClear();

    settleExecution(beginExecution(OWNER, "cell-a"), { namespace_generation: "g1", revision: 2 });
    bumpSessionEpoch(OWNER);
    await refresh(OWNER);
    vi.advanceTimersByTime(5000);
    await tick();

    expect(mockSchemas).not.toHaveBeenCalled();
    expect(getSchemas(OWNER)).toBeNull();
  });
});
