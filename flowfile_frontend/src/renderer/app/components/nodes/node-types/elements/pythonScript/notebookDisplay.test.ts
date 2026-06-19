import { describe, it, expect } from "vitest";
import { TABLE_MIME, EXPLORE_MIME, isTableMime, parseTablePayload } from "./notebookDisplay";

describe("isTableMime", () => {
  it("matches the table and explore mimes", () => {
    expect(isTableMime(TABLE_MIME)).toBe(true);
    expect(isTableMime(EXPLORE_MIME)).toBe(true);
  });

  it("rejects other mimes", () => {
    expect(isTableMime("text/plain")).toBe(false);
    expect(isTableMime("image/png")).toBe(false);
  });
});

describe("parseTablePayload", () => {
  it("parses a valid payload", () => {
    const payload = {
      columns: ["a"],
      fields: [],
      data: [{ a: 1 }],
      total_rows: 1,
      loaded_rows: 1,
      truncated: false,
      max_rows: 10000,
    };
    const parsed = parseTablePayload(JSON.stringify(payload));
    expect(parsed?.columns).toEqual(["a"]);
    expect(parsed?.data).toEqual([{ a: 1 }]);
    expect(parsed?.truncated).toBe(false);
  });

  it("returns null for malformed JSON", () => {
    expect(parseTablePayload("{not json")).toBeNull();
    expect(parseTablePayload("")).toBeNull();
  });
});
