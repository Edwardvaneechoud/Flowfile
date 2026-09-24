import { describe, it, expect } from "vitest";
import { buildDelimited, rowsForCellCap } from "./tableExport";

const columns = ["a", "b"];

describe("buildDelimited", () => {
  it("writes a header row and one line per row", () => {
    const rows = [
      { a: 1, b: "x" },
      { a: 2, b: "y" },
    ];
    expect(buildDelimited(columns, rows, "\t")).toBe("a\tb\n1\tx\n2\ty");
  });

  it("quotes TSV cells holding tabs, newlines or quotes", () => {
    const rows = [{ a: "t\tab", b: 'say "hi"\nthere' }];
    expect(buildDelimited(columns, rows, "\t")).toBe('a\tb\n"t\tab"\t"say ""hi""\nthere"');
  });

  it("quotes CSV cells holding the comma separator only when needed", () => {
    const rows = [{ a: "1,5", b: "plain" }];
    expect(buildDelimited(columns, rows, ",")).toBe('a,b\n"1,5",plain');
  });

  it("renders null and missing values as empty cells", () => {
    expect(buildDelimited(columns, [{ a: null }], ",")).toBe("a,b\n,");
  });
});

describe("rowsForCellCap", () => {
  it("caps at 10,000 rows and the cell budget", () => {
    expect(rowsForCellCap(0)).toBe(10_000);
    expect(rowsForCellCap(1)).toBe(10_000);
    expect(rowsForCellCap(10)).toBe(10_000);
    expect(rowsForCellCap(20)).toBe(5_000);
  });
});
