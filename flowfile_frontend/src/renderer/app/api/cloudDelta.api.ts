import axios from "../services/axios.config";
import type { DeltaVersionCommit } from "../types/catalog.types";
import type { AuthMethod } from "../views/CloudConnectionView/CloudConnectionTypes";

// Must match the FastAPI decorators exactly: prefix "/cloud_storage/delta", no trailing slash.
const BASE_URL = "/cloud_storage/delta";

/** A Delta table location as the cloud storage nodes store it. */
export interface CloudDeltaTarget {
  connection_name?: string | null;
  auth_mode?: AuthMethod;
  resource_path: string;
}

/** State of the table at a path; `exists: false` leaves every other field empty. */
export interface CloudDeltaInfo {
  exists: boolean;
  current_version: number | null;
  partition_columns: string[];
  columns: { name: string; dtype: string }[];
  cdc_enabled: boolean;
  // Change-feed floor derived from the Delta log.
  cdc_enabled_version: number | null;
}

/**
 * Metadata reads for a Delta table on a bare object-storage path. Failures carry
 * `detail: {error_code, message}` (never 401), e.g. NOT_A_DELTA_TABLE or GCS_UNSUPPORTED.
 */
export class CloudDeltaApi {
  static async getInfo(target: CloudDeltaTarget): Promise<CloudDeltaInfo> {
    const response = await axios.post<CloudDeltaInfo>(`${BASE_URL}/info`, target);
    return response.data;
  }

  /** Turn the change data feed on; idempotent. */
  static async enableCdc(target: CloudDeltaTarget): Promise<CloudDeltaInfo> {
    const response = await axios.post<CloudDeltaInfo>(`${BASE_URL}/cdc/enable`, target);
    return response.data;
  }

  /** Most recent commits, newest first. */
  static async getHistory(target: CloudDeltaTarget, limit?: number): Promise<DeltaVersionCommit[]> {
    const body = typeof limit === "number" ? { ...target, limit } : target;
    const response = await axios.post<DeltaVersionCommit[]>(`${BASE_URL}/history`, body);
    return response.data;
  }
}
