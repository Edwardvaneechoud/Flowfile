// Unit tests for the typed flow-parameter helpers: legacy-parameter
// normalization, per-type default coercion, and enum text round-trips.

import { describe, it, expect } from "vitest";
import {
  normalizeParameter,
  coerceDefaultForType,
  enumValuesFromText,
  enumValuesToText,
  validateParameters,
} from "./parameterSettings";

describe("normalizeParameter", () => {
  it("backfills type and enum_values for legacy parameters", () => {
    const normalized = normalizeParameter({ name: "p", default_value: "x", description: "" });
    expect(normalized.type).toBe("string");
    expect(normalized.enum_values).toEqual([]);
  });

  it("keeps existing type and enum values", () => {
    const normalized = normalizeParameter({
      name: "p",
      default_value: "a",
      description: "",
      type: "enum",
      enum_values: ["a", "b"],
    });
    expect(normalized.type).toBe("enum");
    expect(normalized.enum_values).toEqual(["a", "b"]);
  });
});

describe("coerceDefaultForType", () => {
  it("passes strings through unchanged", () => {
    expect(coerceDefaultForType("hello", "string")).toBe("hello");
    expect(coerceDefaultForType("", "string")).toBe("");
  });

  it("keeps valid integers and evicts invalid ones", () => {
    expect(coerceDefaultForType("42", "integer")).toBe("42");
    expect(coerceDefaultForType("4.2", "integer")).toBe("");
    expect(coerceDefaultForType("abc", "integer")).toBe("");
    expect(coerceDefaultForType("", "integer")).toBe("");
  });

  it("keeps valid floats and evicts invalid ones", () => {
    expect(coerceDefaultForType("4.2", "float")).toBe("4.2");
    expect(coerceDefaultForType("42", "float")).toBe("42");
    expect(coerceDefaultForType("abc", "float")).toBe("");
    expect(coerceDefaultForType("", "float")).toBe("");
  });

  it("coerces booleans to true/false strings", () => {
    expect(coerceDefaultForType("true", "boolean")).toBe("true");
    expect(coerceDefaultForType("false", "boolean")).toBe("false");
    expect(coerceDefaultForType("whatever", "boolean")).toBe("false");
  });

  it("evicts enum defaults not in the allowed values", () => {
    expect(coerceDefaultForType("b", "enum", ["a", "b"])).toBe("b");
    expect(coerceDefaultForType("z", "enum", ["a", "b"])).toBe("a");
    expect(coerceDefaultForType("z", "enum", [])).toBe("");
  });
});

describe("enum text round-trip", () => {
  it("parses comma-separated text with trimming and dedupe", () => {
    expect(enumValuesFromText(" a, b ,a,, c ")).toEqual(["a", "b", "c"]);
    expect(enumValuesFromText("")).toEqual([]);
  });

  it("round-trips values through text", () => {
    const values = ["low", "medium", "high"];
    expect(enumValuesFromText(enumValuesToText(values))).toEqual(values);
    expect(enumValuesToText(null)).toBe("");
  });
});

describe("validateParameters", () => {
  it("flags enum parameters without values", () => {
    const issues = validateParameters([
      { name: "mode", default_value: "", description: "", type: "enum", enum_values: [] },
    ]);
    expect(issues).toHaveLength(1);
    expect(issues[0].index).toBe(0);
    expect(issues[0].message).toMatch(/at least one value/);
  });

  it("flags duplicate non-empty names on the later row", () => {
    const issues = validateParameters([
      { name: "limit", default_value: "1", description: "", type: "integer", enum_values: [] },
      { name: "limit", default_value: "2", description: "", type: "integer", enum_values: [] },
    ]);
    expect(issues).toHaveLength(1);
    expect(issues[0].index).toBe(1);
    expect(issues[0].message).toMatch(/Duplicate/);
  });

  it("ignores empty names and accepts valid rows", () => {
    expect(
      validateParameters([
        { name: "", default_value: "", description: "", type: "string", enum_values: [] },
        { name: "", default_value: "", description: "", type: "string", enum_values: [] },
        { name: "mode", default_value: "a", description: "", type: "enum", enum_values: ["a"] },
      ]),
    ).toEqual([]);
  });
});
