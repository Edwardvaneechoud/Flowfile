import { defineStore } from "pinia";
import { SharesApi } from "../api/shares.api";
import type {
  PermissionLevel,
  ResourceType,
  Share,
  ShareCreate,
} from "../types/sharing.types";

// Pinia id "sharing": distinct from node-canvas groups (which are not a store).
export const useSharingStore = defineStore("sharing", {
  actions: {
    async listShares(resourceType: ResourceType, resourceId: number): Promise<Share[]> {
      return SharesApi.list(resourceType, resourceId);
    },

    async createShare(data: ShareCreate): Promise<Share> {
      return SharesApi.create(data);
    },

    async updateShare(grantId: number, permission: PermissionLevel): Promise<Share> {
      return SharesApi.update(grantId, permission);
    },

    async removeShare(grantId: number): Promise<void> {
      await SharesApi.remove(grantId);
    },
  },
});
