import axios from "../services/axios.config";

export type NodeRequestKind = "alteryx" | "general";

export interface NodeRequest {
  number: number;
  title: string;
  url: string;
  kind: NodeRequestKind;
  tool_key: string | null;
  upvotes: number;
}

export interface OpenNodeRequests {
  requests: NodeRequest[];
}

export class NodeRequestsApi {
  static async fetchOpen(): Promise<OpenNodeRequests> {
    // No trailing slash — it must match the FastAPI decorator exactly (307 trap).
    const response = await axios.get<OpenNodeRequests>("/node_requests");
    return response.data;
  }
}
