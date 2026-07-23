import { describe, it, expect, vi, beforeEach } from "vitest";
import { EditorState } from "@codemirror/state";
import { CompletionContext } from "@codemirror/autocomplete";

vi.mock("@/api/lsp.api", () => ({
  LspApi: {
    capabilities: vi.fn(),
    complete: vi.fn(),
    hover: vi.fn(),
    signature: vi.fn(),
    resetCapabilitiesCache: vi.fn(),
  },
}));

import { LspApi } from "@/api/lsp.api";
import {
  createIdentifierCompletionSource,
  fallbackWhenNoLsp,
  lspActiveFor,
  type LspContext,
} from "./lspCompletionSource";

const mockCaps = LspApi.capabilities as unknown as ReturnType<typeof vi.fn>;
const mockComplete = LspApi.complete as unknown as ReturnType<typeof vi.fn>;

function ctxFor(code: string, pos: number, explicit = false): CompletionContext {
  const state = EditorState.create({ doc: code });
  return new CompletionContext(state, pos, explicit);
}

function source(ctx: Partial<LspContext> = {}, priorCells: string[] = []) {
  return createIdentifierCompletionSource(
    () => ({
      kernelId: ctx.kernelId === undefined ? "k1" : ctx.kernelId,
      flowId: ctx.flowId ?? -42,
      nodeId: ctx.nodeId ?? 7,
    }),
    () => priorCells,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  mockCaps.mockResolvedValue({ enabled: true, version: "", features: ["complete"] });
  mockComplete.mockResolvedValue({ items: [] });
});

describe("createIdentifierCompletionSource (LSP side)", () => {
  it("returns null and does not call the API when no kernel is selected", async () => {
    const result = await source({ kernelId: null })(ctxFor("df.", 3));
    expect(result).toBeNull();
    expect(mockComplete).not.toHaveBeenCalled();
  });

  it("returns null when capabilities are disabled", async () => {
    mockCaps.mockResolvedValue({ enabled: false, version: "", features: [] });
    const result = await source()(ctxFor("df.", 3));
    expect(result).toBeNull();
    expect(mockComplete).not.toHaveBeenCalled();
  });

  it("returns null when the backend yields no items (static sources serve)", async () => {
    mockComplete.mockResolvedValue({ items: [] });
    const result = await source()(ctxFor("df.", 3));
    expect(result).toBeNull();
  });

  it("maps items to CodeMirror completions after a dot, replacing from the cursor", async () => {
    mockComplete.mockResolvedValue({
      items: [
        { label: "select", type: "function", detail: "select(...)", documentation: "doc" },
        { label: "filter", type: "method", detail: "", documentation: "" },
      ],
    });
    const result = await source()(ctxFor("df.", 3));
    expect(result).not.toBeNull();
    expect(result!.from).toBe(3); // right after the dot
    expect(result!.options.map((o) => o.label)).toEqual(["select", "filter"]);
    expect(result!.options[0].type).toBe("function");
    expect(result!.options[1].type).toBe("method");
  });

  it("sends 1-based line / 0-based column and the full cell code", async () => {
    mockComplete.mockResolvedValue({ items: [{ label: "x", type: "instance", detail: "", documentation: "" }] });
    await source({ flowId: -99 })(ctxFor("import polars as pl\npl.", 23));
    expect(mockComplete).toHaveBeenCalledTimes(1);
    const [kernelId, payload] = mockComplete.mock.calls[0];
    expect(kernelId).toBe("k1");
    expect(payload.line).toBe(2); // second line
    expect(payload.column).toBe(3); // after "pl."
    expect(payload.flow_id).toBe(-99);
    expect(payload.code).toContain("import polars as pl");
  });

  it("sets from to the start of the in-progress word", async () => {
    mockComplete.mockResolvedValue({ items: [{ label: "select", type: "function", detail: "", documentation: "" }] });
    const result = await source()(ctxFor("df.sel", 6));
    expect(result!.from).toBe(3); // start of "sel"
  });

  it("boosts public names above _private above __dunder__", async () => {
    mockComplete.mockResolvedValue({
      items: [
        { label: "__repr__", type: "function", detail: "", documentation: "" },
        { label: "_internal", type: "instance", detail: "", documentation: "" },
        { label: "catalog", type: "property", detail: "", documentation: "" },
      ],
    });
    const result = await source()(ctxFor("schema.", 7));
    const byLabel = Object.fromEntries(result!.options.map((o) => [o.label, o.boost]));
    expect(byLabel["catalog"]).toBe(0);
    expect(byLabel["_internal"]).toBe(-50);
    expect(byLabel["__repr__"]).toBe(-99);
    expect(byLabel["catalog"]!).toBeGreaterThan(byLabel["_internal"]!);
    expect(byLabel["_internal"]!).toBeGreaterThan(byLabel["__repr__"]!);
  });
});

describe("createIdentifierCompletionSource (merged prior-cell scope)", () => {
  it("dedupes a symbol known to both Jedi and a prior cell — the Jedi entry wins", async () => {
    mockComplete.mockResolvedValue({
      items: [{ label: "catalog", type: "instance", detail: "instance catalog", documentation: "" }],
    });
    const result = await source({}, ["catalog = 1"])(ctxFor("cat", 3));
    const catalogs = result!.options.filter((o) => o.label === "catalog");
    expect(catalogs).toHaveLength(1);
    expect(catalogs[0].detail).toBe("instance catalog");
  });

  it("keeps unexecuted prior-cell symbols alongside LSP items", async () => {
    mockComplete.mockResolvedValue({
      items: [{ label: "executed_var", type: "instance", detail: "", documentation: "" }],
    });
    const result = await source({}, ["pending_var = 5"])(ctxFor("pe", 2));
    const labels = result!.options.map((o) => o.label);
    expect(labels).toContain("executed_var");
    expect(labels).toContain("pending_var");
  });

  it("serves scope symbols without a kernel and never calls the LSP api", async () => {
    const result = await source({ kernelId: null }, ["foo = 1"])(ctxFor("fo", 2));
    expect(result!.options.map((o) => o.label)).toEqual(["foo"]);
    expect(mockCaps).not.toHaveBeenCalled();
    expect(mockComplete).not.toHaveBeenCalled();
  });

  it("serves scope symbols when capabilities are disabled", async () => {
    mockCaps.mockResolvedValue({ enabled: false, version: "", features: [] });
    const result = await source({}, ["foo = 1"])(ctxFor("fo", 2));
    expect(result!.options.map((o) => o.label)).toEqual(["foo"]);
    expect(mockComplete).not.toHaveBeenCalled();
  });

  it("serves scope symbols when the LSP backend degrades to empty", async () => {
    mockComplete.mockResolvedValue({ items: [] });
    const result = await source({}, ["foo = 1"])(ctxFor("fo", 2));
    expect(result!.options.map((o) => o.label)).toEqual(["foo"]);
  });

  it("excludes scope symbols in attribute position", async () => {
    mockComplete.mockResolvedValue({
      items: [{ label: "select", type: "function", detail: "", documentation: "" }],
    });
    const result = await source({}, ["foo = 1"])(ctxFor("df.", 3));
    expect(result!.options.map((o) => o.label)).toEqual(["select"]);
  });

  it("dedupes repeated labels within the LSP response (first wins)", async () => {
    mockComplete.mockResolvedValue({
      items: [
        { label: "dup", type: "function", detail: "first", documentation: "" },
        { label: "dup", type: "instance", detail: "second", documentation: "" },
      ],
    });
    const result = await source()(ctxFor("du", 2));
    const dups = result!.options.filter((o) => o.label === "dup");
    expect(dups).toHaveLength(1);
    expect(dups[0].detail).toBe("first");
  });
});

describe("fallbackWhenNoLsp / lspActiveFor", () => {
  it("lspActiveFor is false without a kernel and never probes capabilities", async () => {
    const active = lspActiveFor(() => ({ kernelId: null, flowId: 0 }));
    expect(await active()).toBe(false);
    expect(mockCaps).not.toHaveBeenCalled();
  });

  it("lspActiveFor reflects capabilities when a kernel is present", async () => {
    mockCaps.mockResolvedValue({ enabled: true, version: "", features: [] });
    expect(await lspActiveFor(() => ({ kernelId: "k1", flowId: 0 }))()).toBe(true);
    mockCaps.mockResolvedValue({ enabled: false, version: "", features: [] });
    expect(await lspActiveFor(() => ({ kernelId: "k1", flowId: 0 }))()).toBe(false);
  });

  it("suppresses the wrapped source when Jedi is active", async () => {
    const inner = vi.fn().mockReturnValue({ from: 0, options: [{ label: "x" }] });
    const wrapped = fallbackWhenNoLsp(inner, async () => true);
    const result = await wrapped(ctxFor("x", 1));
    expect(result).toBeNull();
    expect(inner).not.toHaveBeenCalled();
  });

  it("delegates to the wrapped source when Jedi is inactive", async () => {
    const inner = vi.fn().mockReturnValue({ from: 0, options: [{ label: "x" }] });
    const wrapped = fallbackWhenNoLsp(inner, async () => false);
    const result = await wrapped(ctxFor("x", 1));
    expect(result).toEqual({ from: 0, options: [{ label: "x" }] });
    expect(inner).toHaveBeenCalledOnce();
  });
});
