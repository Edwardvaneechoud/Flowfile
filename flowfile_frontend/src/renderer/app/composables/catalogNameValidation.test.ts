import { describe, it, expect } from "vitest";
import { validateCatalogName, sanitizeCatalogName } from "./catalogNameValidation";

describe("validateCatalogName", () => {
  it("accepts letters, numbers, hyphens and underscores", () => {
    expect(validateCatalogName("my_table")).toBeNull();
    expect(validateCatalogName("my-table")).toBeNull();
    expect(validateCatalogName("Table123")).toBeNull();
    expect(validateCatalogName("a")).toBeNull();
  });

  it("treats empty / whitespace-only as valid (required is enforced elsewhere)", () => {
    expect(validateCatalogName("")).toBeNull();
    expect(validateCatalogName("   ")).toBeNull();
  });

  it("rejects spaces with a space-specific message", () => {
    expect(validateCatalogName("my table", "Table")).toBe("Table cannot contain spaces");
    expect(validateCatalogName("a b")).toBe("Name cannot contain spaces");
  });

  it("rejects other punctuation with the char-set message", () => {
    const msg = "Catalog can only contain letters, numbers, hyphens and underscores";
    expect(validateCatalogName("my.table", "Catalog")).toBe(msg);
    expect(validateCatalogName("my/table", "Catalog")).toBe(msg);
    expect(validateCatalogName("naïve", "Catalog")).toBe(msg);
    expect(validateCatalogName("tbl!", "Catalog")).toBe(msg);
  });

  it("uses the supplied kind in the message", () => {
    expect(validateCatalogName("a b", "Schema")).toBe("Schema cannot contain spaces");
  });
});

describe("sanitizeCatalogName", () => {
  it("collapses invalid runs to a single underscore", () => {
    expect(sanitizeCatalogName("my sales")).toBe("my_sales");
    expect(sanitizeCatalogName("my   sales")).toBe("my_sales");
    expect(sanitizeCatalogName("a.b/c d")).toBe("a_b_c_d");
  });

  it("trims leading and trailing underscores produced by sanitizing", () => {
    expect(sanitizeCatalogName(" sales ")).toBe("sales");
    expect(sanitizeCatalogName("...edge...")).toBe("edge");
  });

  it("leaves already-valid names untouched", () => {
    expect(sanitizeCatalogName("my_table-1")).toBe("my_table-1");
  });
});
