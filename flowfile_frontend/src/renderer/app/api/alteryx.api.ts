import axios from "../services/axios.config";

const API_BASE = "/converters";

export type AlteryxToolStatus = "converted" | "partial" | "commented" | "placeholder" | "skipped";

export interface AlteryxToolRow {
  alteryx_tool_id: string | number;
  alteryx_tool: string;
  flowfile_node_ids: number[];
  flowfile_node_type: string | null;
  status: AlteryxToolStatus;
  messages: string[];
}

export interface AlteryxConversionReport {
  workflow_name: string;
  total_tools: number;
  converted: number;
  partial: number;
  commented: number;
  placeholder: number;
  skipped: number;
  rows: AlteryxToolRow[];
}

export interface AlteryxImportResponse {
  flow_id: number;
  flow_path: string;
  report: AlteryxConversionReport;
}

export class AlteryxApi {
  static async importWorkflow(
    file: File,
    onProgress?: (percent: number) => void,
    opts?: { signal?: AbortSignal },
  ): Promise<AlteryxImportResponse> {
    const formData = new FormData();
    formData.append("file", file);
    // No trailing slash — it must match the FastAPI decorator exactly (307 trap).
    const response = await axios.post<AlteryxImportResponse>(`${API_BASE}/alteryx`, formData, {
      headers: { "Content-Type": "multipart/form-data" },
      signal: opts?.signal,
      onUploadProgress: (e) => {
        if (onProgress && e.total) {
          onProgress(Math.round((e.loaded * 100) / e.total));
        }
      },
    });
    return response.data;
  }
}
