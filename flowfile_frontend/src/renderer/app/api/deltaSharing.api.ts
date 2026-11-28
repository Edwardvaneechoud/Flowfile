// Delta Sharing management: shares of catalog tables, the API consumers granted them, and profile files.
import axios from "../services/axios.config";

export interface DeltaShareTable {
  id: number;
  table_id: number;
  catalog_name: string;
  schema_name: string;
  table_name: string;
  share_history: boolean;
  served: boolean;
  refusal: string | null;
}

export interface DeltaShareRecipient {
  consumer_id: number;
  name: string;
  enabled: boolean;
}

export interface DeltaShare {
  id: number;
  name: string;
  description: string | null;
  owner_id: number;
  uuid: string;
  created_at: string;
  can_manage: boolean;
  tables: DeltaShareTable[];
  recipients: DeltaShareRecipient[];
}

export interface DeltaShareEligibleTable {
  table_id: number;
  catalog_name: string;
  default_schema_name: string;
  default_table_name: string;
  refusal: string | null;
}

export interface DeltaShareProfileFile {
  shareCredentialsVersion: number;
  endpoint: string;
  bearerToken: string;
  expirationTime?: string;
}

export interface DeltaShareProfile {
  profile: DeltaShareProfileFile;
  file_name: string;
  key_prefix: string;
  table_urls: string[];
}

export interface DeltaSharingStatus {
  enabled: boolean;
  endpoint: string | null;
}

export interface DeltaShareTableAdd {
  table_id: number;
  schema_name?: string;
  table_name?: string;
  share_history?: boolean;
}

const BASE = "/delta-sharing/manage";

export class DeltaSharingApi {
  static async status(): Promise<DeltaSharingStatus> {
    return (await axios.get<DeltaSharingStatus>(`${BASE}/status`)).data;
  }

  static async listShares(): Promise<DeltaShare[]> {
    return (await axios.get<DeltaShare[]>(`${BASE}/shares`)).data;
  }

  static async createShare(name: string, description?: string | null): Promise<DeltaShare> {
    return (await axios.post<DeltaShare>(`${BASE}/shares`, { name, description })).data;
  }

  static async updateShare(
    id: number,
    body: { name?: string; description?: string | null },
  ): Promise<DeltaShare> {
    return (await axios.patch<DeltaShare>(`${BASE}/shares/${id}`, body)).data;
  }

  static async deleteShare(id: number): Promise<void> {
    await axios.delete(`${BASE}/shares/${id}`);
  }

  static async addTable(shareId: number, body: DeltaShareTableAdd): Promise<DeltaShare> {
    return (await axios.post<DeltaShare>(`${BASE}/shares/${shareId}/tables`, body)).data;
  }

  static async updateTable(
    shareId: number,
    entryId: number,
    body: { schema_name?: string; table_name?: string; share_history?: boolean },
  ): Promise<DeltaShare> {
    return (await axios.patch<DeltaShare>(`${BASE}/shares/${shareId}/tables/${entryId}`, body))
      .data;
  }

  static async removeTable(shareId: number, entryId: number): Promise<DeltaShare> {
    return (await axios.delete<DeltaShare>(`${BASE}/shares/${shareId}/tables/${entryId}`)).data;
  }

  static async grant(shareId: number, consumerId: number): Promise<DeltaShare> {
    return (
      await axios.post<DeltaShare>(`${BASE}/shares/${shareId}/recipients`, {
        consumer_id: consumerId,
      })
    ).data;
  }

  static async revoke(shareId: number, consumerId: number): Promise<DeltaShare> {
    return (await axios.delete<DeltaShare>(`${BASE}/shares/${shareId}/recipients/${consumerId}`))
      .data;
  }

  static async createProfile(
    shareId: number,
    body: { consumer_id: number; endpoint?: string; expires_in_days?: number | null },
  ): Promise<DeltaShareProfile> {
    return (await axios.post<DeltaShareProfile>(`${BASE}/shares/${shareId}/profile`, body)).data;
  }

  static async eligibleTables(): Promise<DeltaShareEligibleTable[]> {
    return (await axios.get<DeltaShareEligibleTable[]>(`${BASE}/eligible-tables`)).data;
  }

  static async sharesOfConsumer(consumerId: number): Promise<DeltaShare[]> {
    return (await axios.get<DeltaShare[]>(`${BASE}/consumers/${consumerId}/shares`)).data;
  }
}
