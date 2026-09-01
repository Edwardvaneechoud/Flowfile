// Share-link endpoint wrapper: mints a browser link that carries the whole flow
// inside its URL hash. The path has NO trailing slash — it must match the
// FastAPI route exactly or the absolute 307 redirect drops the request in Docker.
import axios from "../services/axios.config";

export type ShareLinkNodeStatus = "supported" | "placeholder";

/** Per-node verdict: a placeholder travels as an inert stub the browser can't run. */
export interface ShareLinkNodeReport {
  node_id: number;
  node_type: string;
  status: ShareLinkNodeStatus;
  reason?: string | null;
}

export interface ShareLinkResponse {
  /** null only when the flow is too large to encode into a link. */
  url: string | null;
  hash_chars: number;
  /** True iff no node was demoted to a placeholder. */
  compatible: boolean;
  nodes_report: ShareLinkNodeReport[];
  warnings: string[];
  placeholder_count: number;
  /** Node ids whose read file the recipient must supply themselves. */
  local_file_nodes: number[];
}

export class ShareLinkApi {
  static async getShareLink(flowId: number): Promise<ShareLinkResponse> {
    const response = await axios.get<ShareLinkResponse>("/editor/share_link", {
      params: { flow_id: flowId },
    });
    return response.data;
  }
}
