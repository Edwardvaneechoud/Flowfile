import axios from "../services/axios.config";
import type { PermissionLevel, ResourceType, Share, ShareCreate } from "../types/sharing.types";

const BASE = "/shares";

export class SharesApi {
  static async list(resourceType: ResourceType, resourceId: number): Promise<Share[]> {
    const response = await axios.get<Share[]>(BASE, {
      params: { resource_type: resourceType, resource_id: resourceId },
    });
    return response.data;
  }

  static async create(data: ShareCreate): Promise<Share> {
    const response = await axios.post<Share>(BASE, data);
    return response.data;
  }

  static async update(grantId: number, permission: PermissionLevel): Promise<Share> {
    const response = await axios.patch<Share>(`${BASE}/${grantId}`, { permission });
    return response.data;
  }

  static async remove(grantId: number): Promise<void> {
    await axios.delete(`${BASE}/${grantId}`);
  }
}
