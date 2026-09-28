// Catalog notebook CRUD plus a flow's canvas-notebook routes (render, plan, push, run lineage). Python cells execute via KernelApi, not here.
import axios from "../services/axios.config";
import type { AccessInfo } from "../types/sharing.types";

const API_BASE_URL = "/catalog/notebooks";

export type NotebookCellType = "python" | "sql" | "markdown";

/** On-the-wire / DB cell shape (matches the backend NotebookCellModel). */
export interface NotebookCellWire {
  id: string;
  type: NotebookCellType;
  source: string;
  metadata: Record<string, any>;
}

export interface NotebookSummary {
  id: number;
  name: string;
  description: string | null;
  namespace_id: number | null;
  default_kernel_id: string | null;
  owner_id: number;
  created_at: string;
  updated_at: string;
  namespace_name: string | null;
  access: AccessInfo | null;
}

export interface Notebook extends NotebookSummary {
  cells: NotebookCellWire[];
}

export interface NotebookCreate {
  name: string;
  namespace_id?: number | null;
  description?: string | null;
  cells: NotebookCellWire[];
  default_kernel_id?: string | null;
}

export interface NotebookUpdate {
  name?: string;
  namespace_id?: number | null;
  description?: string | null;
  cells?: NotebookCellWire[];
  default_kernel_id?: string | null;
}

/** One cell of `GET /notebook/render`; a `cell-<first node id>` cell is one statement spanning `node_ids`. */
export interface RenderedCell {
  cell_id: string;
  node_ids: number[];
  kind: "imports" | "parameters" | "node";
  code: string;
  status: "code" | "placeholder" | "unsupported";
  reason: string | null;
}

export interface NotebookRendering {
  cells: RenderedCell[];
  code_fingerprint: string;
}

export interface NotebookPushBody {
  flow_id: number;
  cells: [string, string][];
  changed_cell_ids: string[];
  provenance: Record<string, [string, number][]>;
  code_fingerprint: string;
  client_max_node_id: number;
}

export interface NotebookPlan {
  warnings: string[];
  deletions: number[];
  parameter_changes: boolean;
}

export interface NotebookPushResult {
  code_fingerprint: string;
  max_node_id: number;
  node_ids_by_cell: Record<string, number[]>;
}

export class NotebookApi {
  /** `{sessions}` for the current user, `null` when the status request fails. */
  static async flowStatus(): Promise<{ sessions: boolean } | null> {
    return (await axios.get("/notebook/status").catch(() => null))?.data ?? null;
  }

  static async renderFlowNotebook(flowId: number): Promise<NotebookRendering> {
    return (await axios.get("/notebook/render", { params: { flow_id: flowId } })).data;
  }

  static async planPush(body: NotebookPushBody): Promise<NotebookPlan> {
    return (await axios.post("/notebook/plan", body)).data;
  }

  static async pushFlowNotebook(body: NotebookPushBody): Promise<NotebookPushResult> {
    return (await axios.post("/editor/notebook/push/", body)).data;
  }

  /** Runs `nodeId` and its ancestors on the canvas; poll `/flow/run_status/` afterwards. */
  static async runLineage(flowId: number, nodeId: number): Promise<void> {
    await axios.post("/editor/notebook/run_lineage/", { flow_id: flowId, node_id: nodeId });
  }

  static async list(): Promise<NotebookSummary[]> {
    const response = await axios.get<NotebookSummary[]>(API_BASE_URL);
    return response.data;
  }

  static async get(id: number): Promise<Notebook> {
    const response = await axios.get<Notebook>(`${API_BASE_URL}/${id}`);
    return response.data;
  }

  static async create(body: NotebookCreate): Promise<Notebook> {
    const response = await axios.post<Notebook>(API_BASE_URL, body);
    return response.data;
  }

  static async update(id: number, body: NotebookUpdate): Promise<Notebook> {
    const response = await axios.put<Notebook>(`${API_BASE_URL}/${id}`, body);
    return response.data;
  }

  static async remove(id: number): Promise<void> {
    await axios.delete(`${API_BASE_URL}/${id}`);
  }
}
