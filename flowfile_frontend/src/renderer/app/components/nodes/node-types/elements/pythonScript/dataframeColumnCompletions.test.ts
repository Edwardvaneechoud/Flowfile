// `|` in a source marks the caret. The schema cache and the runtime state are driven through
// their own seams, so these tests exercise the same path the editor takes on a keystroke.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EditorState } from "@codemirror/state";
import { CompletionContext, type CompletionResult } from "@codemirror/autocomplete";
import { python } from "@codemirror/lang-python";

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
import { resetCatalogRefCache, resolveCatalogRefs } from "../../../../notebook/catalogRefResolver";
import {
  beginExecution,
  bumpSourceRevision,
  disposeOwner,
  settleExecution,
} from "../../../../notebook/notebookRuntimeState";
import {
  attachDataframeSchemas,
  refresh,
  resetDataframeSchemas,
} from "../../../../notebook/useDataframeSchemas";
import {
  createDataframeColumnCompletions,
  type DataframeCompletionContext,
} from "./dataframeColumnCompletions";
import { scanCatalogRefs } from "./dataframeSchemaInference";
import type { UpstreamColumn } from "./useUpstreamColumns";

const mockCaps = LspApi.capabilities as unknown as ReturnType<typeof vi.fn>;
const mockSchemas = LspApi.dataframeSchemas as unknown as ReturnType<typeof vi.fn>;
const mockResolveTable = CatalogApi.resolveTableStrict as unknown as ReturnType<typeof vi.fn>;

const OWNER = "node:7:1";

const INPUTS: UpstreamColumn[] = [
  { name: "id", data_type: "Int64", source_input: "main" },
  { name: "id", data_type: "String", source_input: "lookup" },
];

const ORDERS_TABLE = {
  schema_columns: [
    { name: "id", dtype: "Int64" },
    { name: "amount", dtype: "Float64" },
  ],
};

let detach: (() => void) | null = null;

function frame(name: string, columns: { name: string; dtype: string }[], kind = "DataFrame") {
  return { name, kind, state: "ready", columns, truncated: false };
}

async function seedFrames(frames: unknown[]): Promise<void> {
  mockCaps.mockResolvedValue({ enabled: true, version: "", features: ["dataframe_schemas"] });
  mockSchemas.mockResolvedValue({
    namespace_generation: "g1",
    revision: 1,
    state: "ready",
    dataframes: frames,
  });
  detach = attachDataframeSchemas(OWNER, () => ({
    kernelId: "k1",
    flowId: 7,
    nodeId: 1,
    catalogRefs: () => [],
  }));
  await refresh(OWNER);
}

function makeSource(overrides: Partial<DataframeCompletionContext> = {}) {
  return createDataframeColumnCompletions(() => ({
    ownerId: OWNER,
    cellId: "cur",
    surface: "node",
    getPriorCells: () => [],
    getUpstreamColumns: () => [],
    ...overrides,
  }));
}

function contextFor(source: string): CompletionContext {
  const pos = source.indexOf("|");
  if (pos < 0) throw new Error(`no caret marker in ${JSON.stringify(source)}`);
  const doc = source.slice(0, pos) + source.slice(pos + 1);
  const state = EditorState.create({ doc, extensions: [python()] });
  return new CompletionContext(state, pos, false);
}

type Source = ReturnType<typeof makeSource>;

function runSync(source: Source, code: string): CompletionResult | null {
  const result = source(contextFor(code));
  if (result instanceof Promise) throw new Error(`expected a synchronous result for ${code}`);
  return result;
}

async function runAsync(source: Source, code: string): Promise<CompletionResult | null> {
  return await source(contextFor(code));
}

function details(result: CompletionResult | null): string[] {
  return (result?.options ?? []).map((o) => `${o.label} → ${o.detail}`);
}

beforeEach(() => {
  vi.clearAllMocks();
  mockCaps.mockResolvedValue({ enabled: true, version: "", features: [] });
  mockResolveTable.mockResolvedValue(ORDERS_TABLE);
});

afterEach(() => {
  detach?.();
  detach = null;
  resetDataframeSchemas();
  resetCatalogRefCache();
  disposeOwner(OWNER);
});

describe("createDataframeColumnCompletions — runtime frames", () => {
  it("keeps each frame's own dtype for a same-named column", async () => {
    await seedFrames([
      frame("orders", [{ name: "id", dtype: "Int64" }]),
      frame("customers", [{ name: "id", dtype: "String" }]),
    ]);
    const source = makeSource();
    expect(details(runSync(source, 'orders.select("|'))).toEqual([
      "id → Int64 · orders (last run)",
    ]);
    expect(details(runSync(source, 'customers.select("|'))).toEqual([
      "id → String · customers (last run)",
    ]);
  });

  it("labels a runtime frame `last run` while its cell is current and `outdated` after an edit", async () => {
    await seedFrames([frame("df", [{ name: "id", dtype: "Int64" }])]);
    const source = makeSource({ getPriorCells: () => [{ id: "c1", code: "df = load_frame()" }] });

    settleExecution(beginExecution(OWNER, "c1"));
    expect(details(runSync(source, 'df.select("|'))).toEqual(["id → Int64 · df (last run)"]);

    bumpSourceRevision(OWNER, "c1");
    expect(details(runSync(source, 'df.select("|'))).toEqual([
      "id → Int64 · df (last run, outdated)",
    ]);
  });

  it("marks a never-run assignment cell outdated", async () => {
    await seedFrames([frame("df", [{ name: "id", dtype: "Int64" }])]);
    const source = makeSource({ getPriorCells: () => [{ id: "c1", code: "df = load_frame()" }] });
    expect(details(runSync(source, 'df.select("|'))).toEqual([
      "id → Int64 · df (last run, outdated)",
    ]);
  });

  it("offers a subscript only for a DataFrame", async () => {
    await seedFrames([
      frame("eager", [{ name: "id", dtype: "Int64" }]),
      frame("lazy_df", [], "LazyFrame"),
    ]);
    const source = makeSource({ getUpstreamColumns: () => INPUTS });
    expect(details(runSync(source, 'eager["|'))).toEqual(["id → Int64 · eager (last run)"]);
    // Never falls back to input rows, even on the node surface.
    expect(runSync(source, 'lazy_df["|')).toBeNull();
    expect(runSync(source, 'unknown_name["|')).toBeNull();
  });
});

describe("createDataframeColumnCompletions — catalog-backed source chains", () => {
  it("offers a renamed column without ever resolving during the typed sequence", async () => {
    const prior = 'lf = flowfile_ctx.read_catalog_table("orders")';
    await resolveCatalogRefs(scanCatalogRefs(prior));
    expect(mockResolveTable).toHaveBeenCalledTimes(1);

    const source = makeSource({ getPriorCells: () => [{ id: "c1", code: prior }] });
    for (const typed of ["", "t", "to"]) {
      const result = runSync(source, `lf.rename({"amount": "total"}).select("${typed}|`);
      expect(details(result)).toEqual(["id → Int64 · lf", "total → Float64 · lf"]);
    }
    expect(mockResolveTable).toHaveBeenCalledTimes(1);
  });

  it("resolves a reference first seen in the current cell, then serves it", async () => {
    const source = makeSource();
    const code = 'orders = flowfile_ctx.read_catalog_table("orders")\norders.select("|';
    const result = await runAsync(source, code);
    expect(details(result)).toEqual(["id → Int64 · orders", "amount → Float64 · orders"]);
    expect(mockResolveTable).toHaveBeenCalledTimes(1);
    // The resolved reference is cached, so the next keystroke is synchronous.
    expect(details(runSync(source, code))).toHaveLength(2);
    expect(mockResolveTable).toHaveBeenCalledTimes(1);
  });

  it("stops at an unsupported transform: node falls back per input, catalog says nothing", async () => {
    const prior = 'lf = flowfile_ctx.read_catalog_table("orders")';
    await resolveCatalogRefs(scanCatalogRefs(prior));
    const code = 'lf.join(other, on="id").select("|';

    const node = makeSource({
      getPriorCells: () => [{ id: "c1", code: prior }],
      getUpstreamColumns: () => INPUTS,
    });
    expect(details(runSync(node, code))).toEqual([
      "id → Int64 · input main",
      "id → String · input lookup",
    ]);

    const catalog = makeSource({
      surface: "catalog",
      getPriorCells: () => [{ id: "c1", code: prior }],
    });
    expect(runSync(catalog, code)).toBeNull();
  });
});

describe("createDataframeColumnCompletions — fallback rows", () => {
  it("lists every input once per input for a bare pl.col on the node surface", () => {
    const source = makeSource({ getUpstreamColumns: () => INPUTS });
    expect(details(runSync(source, 'pl.col("|'))).toEqual([
      "id → Int64 · input main",
      "id → String · input lookup",
    ]);
    expect(details(runSync(source, "col('|"))).toHaveLength(2);
  });

  it("never guesses a receiver on the catalog surface", () => {
    const source = makeSource({ surface: "catalog", getUpstreamColumns: () => INPUTS });
    expect(runSync(source, 'pl.col("|')).toBeNull();
    expect(runSync(source, 'mystery.select("|')).toBeNull();
  });

  it("still serves static rows with no kernel and no schema cache", () => {
    const source = makeSource({ getUpstreamColumns: () => INPUTS });
    expect(details(runSync(source, 'mystery.select("|'))).toHaveLength(2);
    expect(mockSchemas).not.toHaveBeenCalled();
  });
});

describe("createDataframeColumnCompletions — insertion", () => {
  it("replaces the whole string content", async () => {
    await seedFrames([frame("orders", [{ name: "amount", dtype: "Float64" }])]);
    const source = makeSource();
    const doc = 'orders.select("amount")';
    const result = runSync(source, 'orders.select("am|ount")');
    expect(result?.from).toBe(doc.indexOf('"') + 1);
    expect(result?.to).toBe(doc.lastIndexOf('"'));
  });

  it("escapes only a label that carries the active quote or a backslash", async () => {
    await seedFrames([
      frame("df", [
        { name: 'we"ird', dtype: "String" },
        { name: "plain", dtype: "String" },
      ]),
    ]);
    const source = makeSource();
    const double = runSync(source, 'df.select("|')?.options ?? [];
    expect(double.find((o) => o.label === 'we"ird')?.apply).toBe('we\\"ird');
    expect(double.find((o) => o.label === "plain")?.apply).toBeUndefined();

    const single = runSync(source, "df.select('|")?.options ?? [];
    expect(single.find((o) => o.label === 'we"ird')?.apply).toBeUndefined();
  });
});

describe("createDataframeColumnCompletions — performance", () => {
  it("serves a cached 1,000-column frame well under the budget", async () => {
    const columns = Array.from({ length: 1000 }, (_, i) => ({
      name: `col_${i}`,
      dtype: "Int64",
    }));
    await seedFrames([frame("big", columns)]);
    const source = makeSource();
    const code = 'big.select("|';
    runSync(source, code);

    const timings: number[] = [];
    for (let i = 0; i < 200; i += 1) {
      const started = performance.now();
      const result = runSync(source, code);
      timings.push(performance.now() - started);
      expect(result?.options).toHaveLength(1000);
    }
    timings.sort((a, b) => a - b);
    const p50 = timings[Math.floor(timings.length * 0.5)];
    const p95 = timings[Math.floor(timings.length * 0.95)];
    console.log(
      `dataframe column completions (1,000 columns, n=200): p50 ${p50.toFixed(2)}ms, p95 ${p95.toFixed(2)}ms`,
    );
    expect(p95).toBeLessThan(250);
  });
});
