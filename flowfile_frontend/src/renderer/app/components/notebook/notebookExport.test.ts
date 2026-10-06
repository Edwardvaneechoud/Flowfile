import { describe, it, expect } from "vitest";
import {
  exportFileName,
  hasExportableCells,
  serializeNotebook,
  toIpynb,
  toPythonScript,
} from "./notebookExport";
import type { NotebookCellModel } from "./types";
import type { CellOutput } from "../../types/node.types";

function cell(
  id: string,
  code: string,
  cellType: NotebookCellModel["cellType"] = "python",
  output?: CellOutput | null,
): NotebookCellModel {
  return { id, cellType, code, metadata: {}, output };
}

function output(partial: Partial<CellOutput>): CellOutput {
  return {
    stdout: "",
    stderr: "",
    display_outputs: [],
    error: null,
    execution_time_ms: 1,
    execution_count: 0,
    ...partial,
  };
}

describe("toPythonScript", () => {
  it("joins the cells in order under percent-format markers with a trailing newline", () => {
    const script = toPythonScript([
      cell("a", "import flowfile as ff\n"),
      cell("b", "df = ff.read_csv('x.csv')"),
    ]);
    expect(script).toBe("# %%\nimport flowfile as ff\n\n# %%\ndf = ff.read_csv('x.csv')\n");
  });

  it("turns markdown cells into comment blocks, keeping blank lines as a bare #", () => {
    const script = toPythonScript([
      cell("m", "# Title\n\nSome notes", "markdown"),
      cell("p", "x = 1"),
    ]);
    expect(script).toBe("# %% [markdown]\n# # Title\n#\n# Some notes\n\n# %%\nx = 1\n");
  });

  it("skips blank cells and is empty for a blank notebook", () => {
    expect(toPythonScript([cell("a", "  \n"), cell("b", "x = 1"), cell("c", "")])).toBe(
      "# %%\nx = 1\n",
    );
    expect(toPythonScript([cell("a", "")])).toBe("");
  });
});

describe("toIpynb", () => {
  const parse = (cells: NotebookCellModel[]) => JSON.parse(toIpynb(cells));

  it("writes an nbformat 4.5 document with a python kernelspec", () => {
    const nb = parse([cell("a", "x = 1")]);
    expect(nb.nbformat).toBe(4);
    expect(nb.nbformat_minor).toBe(5);
    expect(nb.metadata.kernelspec.language).toBe("python");
    expect(nb.metadata.language_info.name).toBe("python");
    expect(toIpynb([cell("a", "x = 1")]).endsWith("}\n")).toBe(true);
  });

  it("maps python cells to code cells and markdown cells to markdown cells", () => {
    const nb = parse([cell("a", "x = 1"), cell("b", "## Notes", "markdown")]);
    expect(nb.cells.map((c: { cell_type: string }) => c.cell_type)).toEqual(["code", "markdown"]);
    expect(nb.cells[0]).toMatchObject({
      id: "a",
      source: "x = 1",
      execution_count: null,
      outputs: [],
    });
    expect(nb.cells[1]).toMatchObject({ id: "b", source: "## Notes" });
    expect(nb.cells[1]).not.toHaveProperty("outputs");
  });

  it("carries a run cell's count, streams, display outputs and error", () => {
    const nb = parse([
      cell(
        "a",
        "print(1)",
        "python",
        output({
          execution_count: 3,
          stdout: "1\n",
          stderr: "warn\n",
          display_outputs: [{ mime_type: "text/html", data: "<b>hi</b>", title: "frame" }],
          error: "boom",
        }),
      ),
    ]);
    expect(nb.cells[0].execution_count).toBe(3);
    expect(nb.cells[0].outputs).toEqual([
      { output_type: "stream", name: "stdout", text: "1\n" },
      { output_type: "stream", name: "stderr", text: "warn\n" },
      {
        output_type: "display_data",
        data: { "text/html": "<b>hi</b>" },
        metadata: { title: "frame" },
      },
      { output_type: "error", ename: "Error", evalue: "boom", traceback: ["boom"] },
    ]);
  });

  it("treats a zero execution count as never run", () => {
    const nb = parse([cell("a", "x", "python", output({ execution_count: 0 }))]);
    expect(nb.cells[0].execution_count).toBeNull();
  });

  it("replaces ids nbformat would reject, keeps ids unique and skips blank cells", () => {
    const nb = parse([
      cell("", "x = 1"),
      cell("b", ""),
      cell("has space", "y = 2"),
      cell("dup", "z = 1"),
      cell("dup", "z = 2"),
    ]);
    expect(nb.cells.map((c: { id: string }) => c.id)).toEqual([
      "nb-cell-1",
      "nb-cell-2",
      "dup",
      "nb-cell-4",
    ]);
  });
});

describe("helpers", () => {
  it("builds a slug file name per format", () => {
    expect(exportFileName("My Flow (v2)", "py")).toBe("my_flow_v2.py");
    expect(exportFileName("  ", "ipynb")).toBe("notebook.ipynb");
  });

  it("knows whether anything can be exported and dispatches on format", () => {
    expect(hasExportableCells([cell("a", " ")])).toBe(false);
    expect(hasExportableCells([cell("a", "x")])).toBe(true);
    expect(serializeNotebook([cell("a", "x")], "py")).toBe("# %%\nx\n");
    expect(JSON.parse(serializeNotebook([cell("a", "x")], "ipynb")).nbformat).toBe(4);
  });
});
