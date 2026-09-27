// Canvas notebook wrappers for core's /notebook/* router (no trailing slashes).
// The router is feature-flagged: a 503 on /notebook/status means the flag is off.
import axios from "../services/axios.config";

export interface CanvasNotebookStatus {
  canvas_notebook: boolean;
  sessions: boolean;
}

export type RenderedCellKind = "imports" | "parameters" | "node";
export type RenderedCellStatus = "code" | "placeholder" | "unsupported";

export interface RenderedCell {
  cell_id: string;
  node_ids: number[];
  kind: RenderedCellKind;
  code: string;
  defines: string[];
  uses: string[];
  status: RenderedCellStatus;
  reason: string | null;
}

export interface NotebookRendering {
  cells: RenderedCell[];
  warnings: string[];
  var_by_node: Record<string, string>;
  code_fingerprint: string;
}

const statusOf = (error: unknown): number | undefined =>
  (error as { response?: { status?: number } })?.response?.status;

export class CanvasNotebookApi {
  /** The flag status, or null when the router answers 503 (flag off). */
  static async getStatus(): Promise<CanvasNotebookStatus | null> {
    try {
      const response = await axios.get<CanvasNotebookStatus>("/notebook/status");
      return response.data;
    } catch (error) {
      if (statusOf(error) === 503) return null;
      throw error;
    }
  }

  static async render(flowId: number): Promise<NotebookRendering> {
    const response = await axios.get<NotebookRendering>("/notebook/render", {
      params: { flow_id: flowId },
    });
    return response.data;
  }
}
