import axios from "../../services/axios.config";
import type { Notebook, NotebookCreate, NotebookSummary, NotebookUpdate } from "../../types";

const BASE_URL = "/notebooks";

export class NotebookApi {
  static async list(): Promise<NotebookSummary[]> {
    const response = await axios.get<NotebookSummary[]>(`${BASE_URL}/`);
    return response.data;
  }

  static async create(payload: NotebookCreate): Promise<Notebook> {
    const response = await axios.post<Notebook>(`${BASE_URL}/`, payload);
    return response.data;
  }

  static async get(id: string): Promise<Notebook> {
    const response = await axios.get<Notebook>(`${BASE_URL}/${encodeURIComponent(id)}`);
    return response.data;
  }

  static async update(id: string, payload: NotebookUpdate): Promise<Notebook> {
    const response = await axios.put<Notebook>(`${BASE_URL}/${encodeURIComponent(id)}`, payload);
    return response.data;
  }

  static async remove(id: string): Promise<void> {
    await axios.delete(`${BASE_URL}/${encodeURIComponent(id)}`);
  }
}
