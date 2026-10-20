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
    dataframeSchemas: vi.fn(),
    resetCapabilitiesCache: vi.fn(),
  },
}));
vi.mock("@/api/catalog.api", () => ({
  CatalogApi: { resolveTableStrict: vi.fn() },
}));
vi.mock("@/stores/catalog-store", () => ({
  useCatalogStore: () => ({ tree: [] }),
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

  it("registers the dataframe column source: one row per input, none from any other source", async () => {
    const opts = optsFor({
      getKernelId: () => "k1",
      getFlowId: () => 1,
      getUpstreamColumns: () => [
        { name: "id", data_type: "Int64", source_input: "main" },
        { name: "id", data_type: "String", source_input: "lookup" },
      ],
    });
    const code = 'pl.col("';
    const options = await allOptions(opts, code, code.length);
    expect(options.map((o) => `${o.label} → ${o.detail}`)).toEqual([
      "id → Int64 · input main",
      "id → String · input lookup",
    ]);
  });

  it("serves column rows with no kernel selected", async () => {
    const opts = optsFor({
      getKernelId: () => null,
      getUpstreamColumns: () => [{ name: "city", data_type: "String", source_input: "main" }],
    });
    const code = "orders.select('";
    const options = await allOptions(opts, code, code.length);
    expect(options.map((o) => o.label)).toEqual(["city"]);
    expect(mockComplete).not.toHaveBeenCalled();
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

  it("offers the generated ff. names once, merged with Jedi's", async () => {
    const code = "ff.read_c";
    mockComplete.mockResolvedValue({
      items: [{ label: "read_csv", type: "function", detail: "def read_csv", documentation: "" }],
    });
    const labels = (await allOptions(optsFor({ getKernelId: () => "k" }), code, code.length)).map(
      (o) => o.label,
    );
    expect(labels.filter((l) => l === "read_csv")).toHaveLength(1);
    expect(labels).toContain("read_database");
  });

  it("offers FlowFrame methods after a dot only in FlowFrame cells", async () => {
    const code = "orders.add_to";
    const noKernel = { getKernelId: () => null };
    const frameLabels = (
      await allOptions(optsFor({ ...noKernel, frameMethods: true }), code, code.length)
    ).map((o) => o.label);
    expect(frameLabels.filter((l) => l === "add_to_group")).toHaveLength(1);
    const plain = "df.sel";
    const selects = (
      await allOptions(optsFor({ ...noKernel, frameMethods: true }), plain, plain.length)
    ).filter((o) => o.label === "select");
    expect(selects).toHaveLength(1);
    const polarsLabels = (await allOptions(optsFor(noKernel), code, code.length)).map(
      (o) => o.label,
    );
    expect(polarsLabels).not.toContain("add_to_group");
    expect(mockComplete).not.toHaveBeenCalled();
  });

  it("offers the group colours inside an ff.FlowGroup color= argument, once each", async () => {
    const opts = optsFor({ getKernelId: () => null });
    const colours = ["amber", "blue", "cyan", "green", "rose", "slate", "violet"];
    for (const code of [
      'clean = ff.FlowGroup("Cleaning (v2)", color="',
      'clean = ff.FlowGroup(\n    "Cleaning",\n    color=\'vi',
    ]) {
      const options = await allOptions(opts, code, code.length);
      expect(options.filter((o) => o.detail === "group colour").map((o) => o.label)).toEqual(
        colours,
      );
      expect(options.filter((o) => o.label === "violet")).toHaveLength(1);
    }
  });

  it("quotes the colour when none is typed yet, merged once with Jedi's rows", async () => {
    mockComplete.mockResolvedValue({
      items: [{ label: "violet", type: "statement", detail: "violet", documentation: "" }],
    });
    for (const kernel of [null, "k1"]) {
      const opts = optsFor({ getKernelId: () => kernel, getFlowId: () => 1 });
      const code = 'clean = ff.FlowGroup("Cleaning", color=vi';
      const violets = (await allOptions(opts, code, code.length)).filter(
        (o) => o.label === "violet",
      );
      expect(violets).toHaveLength(1);
      expect(violets[0].apply).toBe('"violet"');
    }
  });

  it("stays out of comments, other calls and code after an unclosed FlowGroup call", async () => {
    const opts = optsFor({ getKernelId: () => null });
    for (const code of [
      'chart(color="',
      '# ff.FlowGroup("x", color="',
      'g = ff.FlowGroup("a",\nstyle.color = "',
    ]) {
      const options = await allOptions(opts, code, code.length);
      expect(options.some((o) => o.detail === "group colour")).toBe(false);
    }
  });
});
