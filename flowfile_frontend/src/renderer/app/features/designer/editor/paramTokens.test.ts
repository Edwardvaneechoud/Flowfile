// Unit tests for the parameter-token helpers used by the formula/filter editor.
import { describe, it, expect } from "vitest";
import {
  findParamSpans,
  paramInsertText,
  isKnownParam,
  shortType,
  PARAM_NAME_RE,
} from "./paramTokens";

describe("findParamSpans", () => {
  it("finds a bare reference", () => {
    const spans = findParamSpans("[a] + ${bump}");
    expect(spans).toHaveLength(1);
    expect(spans[0]).toMatchObject({ name: "bump", quoted: false });
    expect("[a] + ${bump}".slice(spans[0].start, spans[0].end)).toBe("${bump}");
  });

  it("absorbs matching wrapping quotes (double)", () => {
    const spans = findParamSpans('[a] == "${city}"');
    expect(spans).toHaveLength(1);
    expect(spans[0]).toMatchObject({ name: "city", quoted: true });
    expect('[a] == "${city}"'.slice(spans[0].start, spans[0].end)).toBe('"${city}"');
  });

  it("absorbs matching wrapping quotes (single)", () => {
    const spans = findParamSpans("'${city}'");
    expect(spans[0]).toMatchObject({ name: "city", quoted: true });
    expect(spans[0].end - spans[0].start).toBe("'${city}'".length);
  });

  it("finds multiple references with correct offsets", () => {
    const text = "${a} + ${b}";
    const spans = findParamSpans(text);
    expect(spans.map((s) => s.name)).toEqual(["a", "b"]);
    expect(text.slice(spans[1].start, spans[1].end)).toBe("${b}");
  });

  it("ignores invalid names", () => {
    expect(findParamSpans("${1x}")).toHaveLength(0);
    expect(findParamSpans("${}")).toHaveLength(0);
    expect(findParamSpans("$notbraced")).toHaveLength(0);
  });

  it("treats a mismatched quote as a bare reference", () => {
    const spans = findParamSpans("\"${p}'");
    expect(spans).toHaveLength(1);
    expect(spans[0]).toMatchObject({ name: "p", quoted: false });
  });

  it("mirrors the backend name rule", () => {
    expect(PARAM_NAME_RE).toBe("[a-zA-Z_][a-zA-Z0-9_]*");
    expect(findParamSpans("${_ok9}")[0].name).toBe("_ok9");
  });
});

describe("paramInsertText", () => {
  it("bare variant is always ${name}", () => {
    for (const type of ["string", "integer", "float", "boolean", "enum"] as const) {
      expect(paramInsertText({ name: "p", type }, "bare")).toBe("${p}");
    }
  });

  it("quoted variant quotes string/enum, leaves numeric/boolean bare", () => {
    expect(paramInsertText({ name: "p", type: "string" }, "quoted")).toBe('"${p}"');
    expect(paramInsertText({ name: "p", type: "enum" }, "quoted")).toBe('"${p}"');
    expect(paramInsertText({ name: "p", type: "integer" }, "quoted")).toBe("${p}");
    expect(paramInsertText({ name: "p", type: "float" }, "quoted")).toBe("${p}");
    expect(paramInsertText({ name: "p", type: "boolean" }, "quoted")).toBe("${p}");
  });

  it("treats missing type as string in the quoted variant", () => {
    expect(paramInsertText({ name: "p", type: undefined }, "quoted")).toBe('"${p}"');
  });

  it("defaults to the bare variant", () => {
    expect(paramInsertText({ name: "p", type: "string" })).toBe("${p}");
  });
});

describe("isKnownParam / shortType", () => {
  it("isKnownParam checks the set", () => {
    const known = new Set(["a", "b"]);
    expect(isKnownParam("a", known)).toBe(true);
    expect(isKnownParam("z", known)).toBe(false);
  });

  it("shortType maps each type", () => {
    expect(shortType("integer")).toBe("int");
    expect(shortType("float")).toBe("float");
    expect(shortType("boolean")).toBe("bool");
    expect(shortType("enum")).toBe("enum");
    expect(shortType("string")).toBe("str");
    expect(shortType(undefined)).toBe("str");
  });
});
