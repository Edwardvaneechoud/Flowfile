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
  return { onRun: () => {}, ...overrides };
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
      items: [{ label: "catalog", type: "instance", detail: "instance catalog", documentation: "" }],
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
});
