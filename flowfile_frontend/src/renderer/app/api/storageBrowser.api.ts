import axios from "../services/axios.config";
import type { FileInfo } from "../types";
import type { CloudStorageKind } from "../utils/storagePath";

// Must match the FastAPI decorator exactly: prefix "/storage_browser" + "/cloud",
// no trailing slash. A mismatch only breaks behind the Docker proxy.
const BROWSE_URL = "storage_browser/cloud";

export interface CloudBrowseResponse {
  path: string;
  parent_path: string | null;
  is_root: boolean;
  entries: FileInfo[];
  truncated: boolean;
  next_token: string | null;
  root_listing_denied: boolean;
  current_is_delta_table: boolean;
}

export interface CloudBrowseParams {
  path?: string;
  connectionName?: string | null;
  storageType?: CloudStorageKind | null;
  pageToken?: string | null;
  pageSize?: number;
}

/** A typed browse failure. The backend never uses 401 here, so this can't be a JWT problem. */
export class StorageBrowseError extends Error {
  constructor(
    public errorCode: string,
    message: string,
    public status: number,
  ) {
    super(message);
    this.name = "StorageBrowseError";
  }
}

export async function browseCloudStorage(params: CloudBrowseParams): Promise<CloudBrowseResponse> {
  try {
    const response = await axios.get<CloudBrowseResponse>(BROWSE_URL, {
      params: {
        path: params.path ?? "",
        connection_name: params.connectionName || undefined,
        storage_type: params.storageType || undefined,
        page_token: params.pageToken || undefined,
        page_size: params.pageSize,
      },
    });
    return response.data;
  } catch (error: any) {
    const detail = error.response?.data?.detail;
    throw new StorageBrowseError(
      detail?.error_code ?? "UNKNOWN",
      detail?.message ?? (typeof detail === "string" ? detail : "Could not browse cloud storage"),
      error.response?.status ?? 500,
    );
  }
}
