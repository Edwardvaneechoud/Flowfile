// In-memory cell shape uses {cellType, code}; the wire shape (NotebookCellWire) uses {type, source}.
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
