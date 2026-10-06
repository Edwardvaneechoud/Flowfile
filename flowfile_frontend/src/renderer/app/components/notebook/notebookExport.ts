// Pure serialisers for exporting notebook cells; the I/O (save dialog, clipboard) stays in the panel.
import type { CellOutput } from "../../types/node.types";
import type { NotebookCellModel } from "./types";

export type ExportFormat = "py" | "ipynb";

export const EXPORT_MIME: Record<ExportFormat, string> = {
  py: "text/x-python",
  ipynb: "application/x-ipynb+json",
};

export const EXPORT_FILTER: Record<ExportFormat, { name: string; extensions: string[] }> = {
  py: { name: "Python script", extensions: ["py"] },
  ipynb: { name: "Jupyter notebook", extensions: ["ipynb"] },
};

const nonBlank = (cells: NotebookCellModel[]) => cells.filter((c) => c.code.trim() !== "");

export const hasExportableCells = (cells: NotebookCellModel[]): boolean =>
  nonBlank(cells).length > 0;

/**
 * The cells as one script. Every cell opens with a `# %%` marker (Jupytext's percent format, which
 * VS Code and Spyder read as cell boundaries); Markdown cells become comment blocks so the file
 * still runs with a plain `python`.
 */
export function toPythonScript(cells: NotebookCellModel[]): string {
  const blocks = nonBlank(cells).map((cell) => {
    const code = cell.code.replace(/\s+$/, "");
    if (cell.cellType === "markdown") {
      const lines = code.split("\n").map((line) => (line.trim() ? `# ${line}` : "#"));
      return ["# %% [markdown]", ...lines].join("\n");
    }
    return `# %%\n${code}`;
  });
  return blocks.length ? `${blocks.join("\n\n")}\n` : "";
}

const CELL_ID_RE = /^[a-zA-Z0-9-_]{1,64}$/;

/** nbformat wants unique `[a-zA-Z0-9-_]{1,64}` ids; a model id that fits is kept as is. */
function ipynbCellId(cell: NotebookCellModel, index: number, used: Set<string>): string {
  let id = CELL_ID_RE.test(cell.id) && !used.has(cell.id) ? cell.id : `nb-cell-${index + 1}`;
  while (used.has(id)) id = `${id}-x`;
  used.add(id);
  return id;
}

function ipynbOutputs(output: CellOutput | null | undefined): Record<string, unknown>[] {
  if (!output) return [];
  const outputs: Record<string, unknown>[] = [];
  if (output.stdout) outputs.push({ output_type: "stream", name: "stdout", text: output.stdout });
  if (output.stderr) outputs.push({ output_type: "stream", name: "stderr", text: output.stderr });
  for (const display of output.display_outputs ?? []) {
    outputs.push({
      output_type: "display_data",
      data: { [display.mime_type]: display.data },
      metadata: display.title ? { title: display.title } : {},
    });
  }
  if (output.error) {
    outputs.push({
      output_type: "error",
      ename: "Error",
      evalue: output.error,
      traceback: [output.error],
    });
  }
  return outputs;
}

/** The cells as an nbformat 4.5 document; a run cell keeps its count and outputs. */
export function toIpynb(cells: NotebookCellModel[]): string {
  const used = new Set<string>();
  const nbCells = nonBlank(cells).map((cell, index) => {
    const base = { id: ipynbCellId(cell, index, used), metadata: {}, source: cell.code };
    if (cell.cellType === "markdown") return { ...base, cell_type: "markdown" };
    const count = cell.output?.execution_count;
    return {
      ...base,
      cell_type: "code",
      execution_count: count && count > 0 ? count : null,
      outputs: ipynbOutputs(cell.output),
    };
  });
  const notebook = {
    cells: nbCells,
    metadata: {
      kernelspec: { name: "python3", display_name: "Python 3", language: "python" },
      language_info: { name: "python" },
    },
    nbformat: 4,
    nbformat_minor: 5,
  };
  return `${JSON.stringify(notebook, null, 1)}\n`;
}

/** `base` slugified for a file name, with the format's extension; blank bases become `notebook`. */
export function exportFileName(base: string, format: ExportFormat): string {
  const slug = base
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `${slug || "notebook"}.${format}`;
}

export function serializeNotebook(cells: NotebookCellModel[], format: ExportFormat): string {
  return format === "py" ? toPythonScript(cells) : toIpynb(cells);
}
