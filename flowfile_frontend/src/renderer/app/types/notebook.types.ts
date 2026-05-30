// Standalone (flow-independent) notebook types — mirror flowfile_core.notebook.models

export interface NotebookCellData {
  id: string;
  source: string;
  cell_type?: string;
}

export interface NotebookSummary {
  id: string;
  name: string;
  // Synthetic, notebook-scoped flow id used as the kernel namespace key.
  flow_id: number;
  kernel_id: string | null;
  created_at: string;
  modified_at: string;
}

export interface Notebook extends NotebookSummary {
  cells: NotebookCellData[];
}

export interface NotebookCreate {
  name?: string;
  kernel_id?: string | null;
}

export interface NotebookUpdate {
  name?: string;
  kernel_id?: string | null;
  cells?: NotebookCellData[];
}
