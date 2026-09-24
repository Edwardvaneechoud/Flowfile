import { describe, expect, it } from "vitest";
import { advancedFilterError } from "./filterValidation";

describe("advancedFilterError", () => {
  it("refuses an empty or blank advanced expression", () => {
    expect(advancedFilterError("")).not.toBeNull();
    expect(advancedFilterError("   \n")).not.toBeNull();
    expect(advancedFilterError(undefined)).not.toBeNull();
  });

  it("accepts a real expression", () => {
    expect(advancedFilterError("[id] > 1")).toBeNull();
  });
});
