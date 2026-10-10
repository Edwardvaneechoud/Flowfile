// Catalog notebook CRUD plus a flow's canvas-notebook routes; catalog cells execute via KernelApi.
import axios from "../services/axios.config";
import type { HistoryState } from "../types/flow.types";
import type { ExecuteResult } from "../types/kernel.types";
import type { AccessInfo } from "../types/sharing.types";
import { EMPTY_DATAFRAME_SCHEMAS, type LspDataframeSchemasResponse } from "./lsp.api";

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

/** One cell of `GET /notebook/render`; a `cell-<first node id>` cell is one statement spanning `node_ids`;
 * the `groups` cell declares the canvas's visual groups (`ff.FlowGroup`). */
export interface RenderedCell {
  cell_id: string;
  node_ids: number[];
  kind: "imports" | "parameters" | "groups" | "node";
  code: string;
  status: "code" | "placeholder" | "unsupported";
  reason: string | null;
}

export interface NotebookRendering {
  cells: RenderedCell[];
  warnings: string[];
  var_by_node: Record<number, string>;
  code_fingerprint: string;
  /** The flow revision the rendering was taken at. */
  revision?: number;
}

export interface NotebookPushBody {
  flow_id: number;
  cells: [string, string][];
  changed_cell_ids: string[];
  provenance: Record<string, [string, number][]>;
  code_fingerprint: string;
  /** Core holds back (`applied: false`) a push this action must review first; absent, it applies. */
  trigger?: "push" | "run";
  /** Set when a kernel is picked: the push runs the cells on that kernel. */
  kernel_id?: string;
}

/** Identifies one canvas-notebook session: a flow's namespace on a kernel. */
export interface NotebookSessionKey {
  flow_id: number;
  kernel_id: string;
}

export interface NotebookSessionExecute extends NotebookSessionKey {
  cell_id: string;
  code: string;
  node_id: number;
}

export interface NotebookPushResult {
  history: HistoryState;
  code_fingerprint: string;
  max_node_id: number;
  node_ids_by_cell: Record<string, number[]>;
  warnings: string[];
  applied: boolean;
  deletions: number[];
  parameter_changes: boolean;
}

/** The 422 detail of a push: the failing cell and 1-based line (`null` when unknown). */
export interface NotebookSyncErrorDetail {
  message: string;
  cell_id: string | null;
  line: number | null;
  kind: "needs_kernel" | "refused" | "error";
}

export interface RunLineageResult {
  message: string;
  flow_id: number;
  node_ids: number[];
}

export class NotebookApi {
  /** `null` when the status request fails. */
  static async flowStatus(): Promise<{ kernel_sessions: boolean } | null> {
    return (await axios.get("/notebook/status").catch(() => null))?.data ?? null;
  }

  static async openSession(key: NotebookSessionKey): Promise<{ status: string }> {
    return (await axios.post("/notebook/session/open", key)).data;
  }

  static async executeInSession(body: NotebookSessionExecute): Promise<ExecuteResult> {
    return (await axios.post("/notebook/session/execute", body)).data;
  }

  static async resetSession(key: NotebookSessionKey): Promise<void> {
    await axios.post("/notebook/session/reset", key);
  }

  /** Failure-safe like the kernel LSP call: errors resolve to the empty shape. */
  static async sessionSchemas(key: NotebookSessionKey): Promise<LspDataframeSchemasResponse> {
    try {
      return (await axios.post("/notebook/session/schemas", key)).data ?? EMPTY_DATAFRAME_SCHEMAS;
    } catch {
      return EMPTY_DATAFRAME_SCHEMAS;
    }
  }

  static async interruptSession(key: NotebookSessionKey): Promise<void> {
    await axios.post("/notebook/session/interrupt", key);
  }

  static async renderFlowNotebook(flowId: number): Promise<NotebookRendering> {
    return (await axios.get("/notebook/render", { params: { flow_id: flowId } })).data;
  }

  static async pushFlowNotebook(body: NotebookPushBody): Promise<NotebookPushResult> {
    return (await axios.post("/editor/notebook/push/", body)).data;
  }

  /** Runs `nodeId` and its ancestors on the canvas; poll `/flow/run_status/` afterwards. */
  static async runLineage(flowId: number, nodeId: number): Promise<RunLineageResult> {
    return (await axios.post("/editor/notebook/run_lineage/", { flow_id: flowId, node_id: nodeId }))
      .data;
  }

  /** Tells core the user exported the cells; the download itself is client-side (telemetry only). */
  static async confirmExport(format: "py" | "ipynb"): Promise<void> {
    await axios.post(`/notebook/exported/${format}`);
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
