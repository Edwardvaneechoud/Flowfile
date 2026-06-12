import { defineStore } from "pinia";
import { SharesApi } from "../api/shares.api";
import { UserGroupsApi } from "../api/userGroups.api";
import type {
  PermissionLevel,
  ResourceType,
  Share,
  ShareCreate,
  UserGroup,
} from "../types/sharing.types";

interface SharingState {
  // The groups the current user belongs to — the share targets.
  myGroups: UserGroup[];
  groupsLoaded: boolean;
}

// Pinia id "sharing": distinct from node-canvas groups (which are not a store).
export const useSharingStore = defineStore("sharing", {
  state: (): SharingState => ({
    myGroups: [],
    groupsLoaded: false,
  }),

  actions: {
    async loadMyGroups(force = false): Promise<UserGroup[]> {
      if (this.groupsLoaded && !force) {
        return this.myGroups;
      }
      this.myGroups = await UserGroupsApi.list(false);
      this.groupsLoaded = true;
      return this.myGroups;
    },

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
