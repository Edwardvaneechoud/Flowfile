// Headless regression tests for the notebook's autocompletion({override}) source list:
// exactly one row per identifier across ALL sources, statics suppressed while Jedi is
// active, and the no-kernel fallback still serving builtins/local names.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { EditorState } from "@codemirror/state";
import { CompletionContext, type Completion } from "@codemirror/autocomplete";
import { python } from "@codemirror/lang-python";

vi.mock("@/api/lsp.api", () => ({
  LspApi: {
    capabilities: vi.fn(),
    complete: vi.fn(),
    hover: vi.fn(),
    signature: vi.fn(),
    diagnostics: vi.fn(),
    resetCapabilitiesCache: vi.fn(),
  },
}));

import { LspApi } from "@/api/lsp.api";
import { buildNotebookCompletionSources, type NotebookEditorOptions } from "./notebookEditor";

const mockCaps = LspApi.capabilities as unknown as ReturnType<typeof vi.fn>;
const mockComplete = LspApi.complete as unknown as ReturnType<typeof vi.fn>;

function optsFor(overrides: Partial<NotebookEditorOptions> = {}): NotebookEditorOptions {
  return { onRun: () => undefined, ...overrides };
}

/** Run every override source against one context and concatenate the surviving options. */
async function allOptions(
  opts: NotebookEditorOptions,
  code: string,
  pos: number,
  explicit = false,
): Promise<Completion[]> {
  const state = EditorState.create({ doc: code, extensions: [python()] });
  const context = new CompletionContext(state, pos, explicit);
  const results = await Promise.all(buildNotebookCompletionSources(opts).map((s) => s(context)));
  return results.flatMap((r) => (r ? [...r.options] : []));
}

beforeEach(() => {
  vi.clearAllMocks();
  mockCaps.mockResolvedValue({ enabled: true, version: "", features: ["complete"] });
  mockComplete.mockResolvedValue({ items: [] });
});

describe("buildNotebookCompletionSources", () => {
  it("yields exactly one row when Jedi and a prior cell both know a symbol", async () => {
    mockComplete.mockResolvedValue({
      items: [
        { label: "catalog", type: "instance", detail: "instance catalog", documentation: "" },
      ],
    });
    const opts = optsFor({
      getKernelId: () => "k1",
      getFlowId: () => 1,
      getPriorCellCodes: () => ["catalog = 1"],
    });
    const options = await allOptions(opts, "catalog", 7);
    const catalogs = options.filter((o) => o.label === "catalog");
    expect(catalogs).toHaveLength(1);
    expect(catalogs[0].detail).toBe("instance catalog"); // the Jedi row won
  });

  it("yields exactly one `read` row when Jedi resolves a catalog-ref chain the curated source also knows", async () => {
    mockComplete.mockResolvedValue({
      items: [
        { label: "read", type: "function", detail: "def read", documentation: "" },
        { label: "write", type: "function", detail: "def write", documentation: "" },
      ],
    });
    const opts = optsFor({ getKernelId: () => "k1", getFlowId: () => 1 });
    const code = 'flowfile_ctx.default_schema().get_table_ref("fx_rates").read';
    const options = await allOptions(opts, code, code.length);
    const reads = options.filter((o) => o.label === "read");
    expect(reads).toHaveLength(1);
    expect(reads[0].detail).toBe("TableRef.read(delta_version?) -> LazyFrame"); // curated row won
    expect(reads[0].apply).toBe("read()");
    expect(options.filter((o) => o.label === "write")).toHaveLength(1);
  });

  it("yields exactly one row per method on a variable bound to a table ref across cells", async () => {
    mockComplete.mockResolvedValue({
      items: [{ label: "exists", type: "function", detail: "def exists", documentation: "" }],
    });
    const opts = optsFor({
      getKernelId: () => "k1",
      getFlowId: () => 1,
      getPriorCellCodes: () => ['tref = flowfile_ctx.default_schema().get_table_ref("t")'],
    });
    const options = await allOptions(opts, "tref.ex", 7);
    expect(options.filter((o) => o.label === "exists")).toHaveLength(1);
  });

  it("still serves the curated catalog-ref entries with no kernel selected", async () => {
    const opts = optsFor({ getKernelId: () => null });
    const code = 'flowfile_ctx.get_catalog("c").get_schema("s").';
    const labels = (await allOptions(opts, code, code.length)).map((o) => o.label);
    expect(labels).toContain("get_table_ref");
    expect(labels.filter((l) => l === "get_table_ref")).toHaveLength(1);
    expect(mockComplete).not.toHaveBeenCalled();
  });

  it("suppresses lang-python statics while Jedi is active (no builtin 'print' row)", async () => {
    mockComplete.mockResolvedValue({
      items: [{ label: "proc_data", type: "function", detail: "", documentation: "" }],
    });
    const opts = optsFor({ getKernelId: () => "k1", getFlowId: () => 1 });
    const labels = (await allOptions(opts, "pr", 2)).map((o) => o.label);
    expect(labels).toContain("proc_data");
    expect(labels).not.toContain("print");
  });

  it("serves builtins and local names from the fallbacks when no kernel is selected", async () => {
    const opts = optsFor({ getKernelId: () => null });
    const labels = (await allOptions(opts, "xval = 1\npr", 11)).map((o) => o.label);
    expect(labels).toContain("print"); // lang-python globalCompletion
    expect(labels).toContain("xval"); // lang-python localCompletionSource
    expect(mockComplete).not.toHaveBeenCalled();
  });

  it("string-literal sources still serve while Jedi is active", async () => {
    const opts = optsFor({
      getKernelId: () => "k1",
      getFlowId: () => 1,
      getInputNames: () => ["main"],
    });
    const code = 'flowfile_ctx.read_input("';
    const labels = (await allOptions(opts, code, code.length)).map((o) => o.label);
    expect(labels).toContain("main");
  });

  it("drops `info` so the docs panel never opens while typing, but keeps `detail`", async () => {
    mockComplete.mockResolvedValue({
      items: [
        {
          label: "read_catalog_table",
          type: "function",
          detail: "def read_catalog_table",
          documentation: "Read a catalog table as a Polars LazyFrame.",
        },
      ],
    });
    const opts = optsFor({
      getKernelId: () => "k1",
      getFlowId: () => 1,
      getInputNames: () => ["main"],
      getUpstreamColumns: () => [{ name: "city", data_type: "String", source_input: "main" }],
    });
    for (const [code, pos] of [
      ["read_catalog_ta", 15],
      ['flowfile_ctx.read_input("', 25],
      ['pl.col("', 8],
    ] as const) {
      const options = await allOptions(opts, code, pos);
      expect(options.length).toBeGreaterThan(0);
      expect(options.every((o) => o.info === undefined)).toBe(true);
    }
    const jedi = (await allOptions(opts, "read_catalog_ta", 15)).find(
      (o) => o.label === "read_catalog_table",
    );
    expect(jedi?.detail).toBe("def read_catalog_table");
  });
});
