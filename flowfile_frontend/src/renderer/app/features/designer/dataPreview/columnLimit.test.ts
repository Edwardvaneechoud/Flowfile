import { describe, expect, it } from "vitest";

import { MAX_PREVIEW_COLUMNS, limitPreviewColumns } from "./columnLimit";

const cols = (n: number) => Array.from({ length: n }, (_, i) => ({ name: `c${i}` }));
const row = (n: number) => Object.fromEntries(cols(n).map((c, i) => [c.name, i]));

describe("limitPreviewColumns", () => {
  it("passes through untouched at or under the cap", () => {
    const columns = cols(3);
    const rows = [row(3)];
    const result = limitPreviewColumns(columns, rows, 3);
    expect(result.columns).toBe(columns);
    expect(result.rows).toBe(rows);
    expect(result.hiddenColumnCount).toBe(0);
  });

  it("keeps the first columns and drops the rest from every row", () => {
    const result = limitPreviewColumns(cols(5), [row(5), row(5)], 2);
    expect(result.columns.map((c) => c.name)).toEqual(["c0", "c1"]);
    expect(result.rows).toEqual([
      { c0: 0, c1: 1 },
      { c0: 0, c1: 1 },
    ]);
    expect(result.hiddenColumnCount).toBe(3);
  });

  it("defaults to MAX_PREVIEW_COLUMNS", () => {
    const result = limitPreviewColumns(cols(MAX_PREVIEW_COLUMNS + 7), []);
    expect(result.columns).toHaveLength(MAX_PREVIEW_COLUMNS);
    expect(result.hiddenColumnCount).toBe(7);
  });
});
