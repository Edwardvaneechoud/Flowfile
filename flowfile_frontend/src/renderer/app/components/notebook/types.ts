// In-memory notebook cell model for the Catalog notebook UI. The persisted /
// wire shape (NotebookCellWire in api/notebook.api.ts) keys cells as
// {type, source}; here we use {cellType, code} to match the rest of the
// notebook UI, plus transient execution state that is never persisted.
//
// Cell types are Python + Markdown. SQL cells were removed (the catalog has a
// dedicated SQL editor tab); a legacy persisted "sql" cell is coerced to
// "python" on load.
import type { CellOutput } from "../../types/node.types";

export type CellType = "python" | "markdown";

export type CellExecState = "idle" | "running" | "error";

export interface NotebookCellModel {
  id: string;
  cellType: CellType;
  code: string;
  metadata: Record<string, any>;
  // Transient (never persisted):
  output?: CellOutput | null; // python cell output
  renderedHtml?: string | null; // markdown render
  execState?: CellExecState;
  editing?: boolean; // markdown edit/preview toggle
}
