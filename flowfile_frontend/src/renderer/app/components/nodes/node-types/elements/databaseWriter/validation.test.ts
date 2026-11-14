import { describe, it, expect } from "vitest";
import { QUOTED_NAME_HINT, validateSqlIdentifier } from "./validation";

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

describe("validateSqlIdentifier for a dialect that takes quoted names", () => {
  const quoted = { quoted: true };

  it("stays strict by default, so a quoted or $ name is refused", () => {
    expect(validateSqlIdentifier('"USER$evaneechoud"')).toContain("Only letters, numbers");
    expect(validateSqlIdentifier("A$B")).toContain("Only letters, numbers");
    expect(validateSqlIdentifier('"USER$evaneechoud"', { quoted: false })).toContain(
      "Only letters, numbers",
    );
  });

  it("accepts plain, $ and double-quoted parts", () => {
    for (const name of [
      '"USER$evaneechoud"',
      '"my.db"."My Schema".T',
      "A$B",
      "ORDERS",
      "PUBLIC.ORDERS",
      '"quo""te"',
    ]) {
      expect(validateSqlIdentifier(name, quoted)).toBeNull();
    }
  });

  it("refuses anything that could end a quote or a statement, with the quoting hint", () => {
    for (const name of ['a"b', '"unterminated', "a;b", "a b", "a--b", '""', "a..b", "1ST"]) {
      expect(validateSqlIdentifier(name, quoted)).toBe(
        `Invalid SQL identifier: '${name}'. ${QUOTED_NAME_HINT}`,
      );
    }
    expect(QUOTED_NAME_HINT).toBe(
      "Use letters, numbers, _ and $, or put the name in double quotes.",
    );
  });
});
