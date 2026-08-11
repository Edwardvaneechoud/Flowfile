import { describe, expect, it } from "vitest";
import { inferColumnDataType, parseCsvText, parseTabularText } from "./clipboardUtils";

describe("parseTabularText", () => {
  it("returns null for empty text", () => {
    expect(parseTabularText("")).toBeNull();
  });

  it("returns null for a single line without tabs", () => {
    expect(parseTabularText("[price] * 1.21")).toBeNull();
  });

  it("parses a single line with tabs as one row", () => {
    expect(parseTabularText("a\tb")).toEqual([["a", "b"]]);
  });

  it("parses multi-line TSV into rows", () => {
    expect(parseTabularText("a\tb\n1\t2\n3\t4")).toEqual([
      ["a", "b"],
      ["1", "2"],
      ["3", "4"],
    ]);
  });

  it("parses multi-line text without tabs as a single column", () => {
    expect(parseTabularText("alpha\nbeta")).toEqual([["alpha"], ["beta"]]);
  });

  it("normalizes CRLF and strips a trailing newline", () => {
    expect(parseTabularText("a\tb\r\n1\t2\r\n")).toEqual([
      ["a", "b"],
      ["1", "2"],
    ]);
  });
});

describe("inferColumnDataType", () => {
  it.each([
    [["true", "false"], "Boolean"],
    [["1", "2", "3"], "Int64"],
    [["1.5", "2"], "Float64"],
    [["1", "x"], "String"],
    [[], "String"],
    [["", null, undefined], "String"],
  ])("%j → %s", (values, expected) => {
    expect(inferColumnDataType(values as unknown[])).toBe(expected);
  });
});

describe("parseCsvText", () => {
  it("auto-detects comma and tab", () => {
    expect(parseCsvText("a,b\n1,2")).toEqual([
      ["a", "b"],
      ["1", "2"],
    ]);
    expect(parseCsvText("a\tb\n1\t2")).toEqual([
      ["a", "b"],
      ["1", "2"],
    ]);
  });

  it("handles RFC 4180 quoting", () => {
    expect(parseCsvText('name,quote\nx,"hello ""world"""')).toEqual([
      ["name", "quote"],
      ["x", 'hello "world"'],
    ]);
  });

  it("returns [] for empty text", () => {
    expect(parseCsvText("")).toEqual([]);
  });
});
