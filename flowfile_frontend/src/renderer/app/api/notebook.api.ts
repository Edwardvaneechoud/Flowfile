// Catalog notebook CRUD; maps wire cells {id, type, source, metadata} <-> in-memory {cellType, code}. Python cells execute via KernelApi, not here.
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

export class NotebookApi {
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
