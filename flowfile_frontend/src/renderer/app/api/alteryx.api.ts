import axios from "../services/axios.config";

const API_BASE = "/converters";

export type AlteryxToolStatus = "converted" | "partial" | "commented" | "placeholder" | "skipped";

// A canvas comment is an annotation, never a tool — it must not reach the coverage numbers.
export type AlteryxToolEntity = "tool" | "annotation";

export interface AlteryxToolRow {
  alteryx_tool_id: string | number;
  alteryx_tool: string;
  entity: AlteryxToolEntity;
  alteryx_tool_key?: string;
  flowfile_node_ids: number[];
  flowfile_node_type: string | null;
  status: AlteryxToolStatus;
  messages: string[];
}

export interface AlteryxCoverageSummary {
  tools: number;
  mapped: number;
  converted: number;
  mapped_percent: number;
  converted_percent: number;
  definition: string;
}

export interface AlteryxConversionReport {
  workflow_name: string;
  total_tools: number;
  total_annotations: number;
  converted: number;
  partial: number;
  commented: number;
  placeholder: number;
  skipped: number;
  coverage: AlteryxCoverageSummary;
  rows: AlteryxToolRow[];
}

export interface AlteryxImportResponse {
  flow_id: number;
  flow_path: string;
  report: AlteryxConversionReport;
}

export interface AlteryxNodeRequests {
  issues: Record<string, string>;
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

  static async fetchNodeRequests(): Promise<AlteryxNodeRequests> {
    const response = await axios.get<AlteryxNodeRequests>(`${API_BASE}/alteryx/node_requests`);
    return response.data;
  }
}
