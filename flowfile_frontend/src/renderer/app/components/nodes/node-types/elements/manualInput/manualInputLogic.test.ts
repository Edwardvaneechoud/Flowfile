// Unit tests for the column data-type inference shared by the raw-data editor
// (manual_input and flow_input sample data).

import { describe, it, expect } from "vitest";
import { inferDataType } from "./manualInputLogic";

describe("inferDataType", () => {
  it("returns String when there are no usable values", () => {
    expect(inferDataType([])).toBe("String");
    expect(inferDataType([null, undefined, ""])).toBe("String");
  });

  it("detects booleans from literals and strings", () => {
    expect(inferDataType([true, false])).toBe("Boolean");
    expect(inferDataType(["true", "false", "true"])).toBe("Boolean");
    expect(inferDataType(["true", "nope"])).toBe("String");
  });

  it("detects integers vs floats, ignoring empty cells", () => {
    expect(inferDataType(["1", "2", ""])).toBe("Int64");
    expect(inferDataType([1, 2, 3])).toBe("Int64");
    expect(inferDataType(["1.5", "2"])).toBe("Float64");
    expect(inferDataType([1.5, 2])).toBe("Float64");
  });

  it("falls back to String for mixed or non-numeric values", () => {
    expect(inferDataType(["1", "abc"])).toBe("String");
    expect(inferDataType(["  ", "2"])).toBe("String");
  });
});
