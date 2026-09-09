import { describe, it, expect } from "vitest";
import { cellValueFormatter, formatCellValue } from "./cellFormat";

describe("formatCellValue", () => {
  it("renders nested lists of structs as JSON instead of [object Object]", () => {
    const cell = [[{ vehicle_type_id: "1", count: 3 }], [{ vehicle_type_id: "2", count: 0 }]];
    const expected = '[[{"vehicle_type_id":"1","count":3}],[{"vehicle_type_id":"2","count":0}]]';
    expect(formatCellValue(cell)).toBe(expected);
    expect(formatCellValue({ a: 1 })).toBe('{"a":1}');
  });

  it("keeps primitive lists unambiguous", () => {
    expect(formatCellValue(["a", "b"])).toBe('["a","b"]');
    expect(formatCellValue([10, 4])).toBe("[10,4]");
  });

  it("leaves scalars and nulls as AG Grid would show them", () => {
    expect(formatCellValue(10)).toBe("10");
    expect(formatCellValue(false)).toBe("false");
    expect(formatCellValue("x")).toBe("x");
    expect(formatCellValue(null)).toBe("");
    expect(formatCellValue(undefined)).toBe("");
  });
});

describe("cellValueFormatter", () => {
  it("adapts formatCellValue to AG Grid's valueFormatter signature", () => {
    expect(cellValueFormatter({ value: [{ k: 1 }] })).toBe('[{"k":1}]');
    expect(cellValueFormatter({ value: null })).toBe("");
  });
});
