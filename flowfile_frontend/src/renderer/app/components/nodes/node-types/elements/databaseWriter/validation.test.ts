import { describe, it, expect } from "vitest";
import { validateSqlIdentifier } from "./validation";

describe("validateSqlIdentifier", () => {
  it("accepts plain identifiers and dotted schema.table notation", () => {
    expect(validateSqlIdentifier("yelp_database")).toBeNull();
    expect(validateSqlIdentifier("main.yelp_database")).toBeNull();
    expect(validateSqlIdentifier("_private9")).toBeNull();
  });

  it("treats empty/unset as valid (required-ness handled elsewhere)", () => {
    expect(validateSqlIdentifier("")).toBeNull();
    expect(validateSqlIdentifier(null)).toBeNull();
    expect(validateSqlIdentifier(undefined)).toBeNull();
  });

  it("rejects dashes, spaces, leading digits and empty parts", () => {
    const msg =
      "Invalid SQL identifier: 'tesst-2'. Only letters, numbers, and underscores are allowed.";
    expect(validateSqlIdentifier("tesst-2")).toBe(msg);
    expect(validateSqlIdentifier("has space")).toContain("Invalid SQL identifier");
    expect(validateSqlIdentifier("9starts")).toContain("Invalid SQL identifier");
    expect(validateSqlIdentifier("main.")).toContain("Invalid SQL identifier");
    expect(validateSqlIdentifier("bad-schema.table")).toContain("Invalid SQL identifier");
  });
});
