import { useAuthStore } from "../stores/auth-store";
import type { AccessInfo } from "../types/sharing.types";
import { useMultiUser } from "./useMultiUser";

interface HasAccess {
  access?: AccessInfo | null;
}

// Shared helpers for the catalog/secrets/connection Share affordances. `access` is
// null for owners in unrestricted mode (admin/electron) — treated as owned.
export function useResourceSharing() {
  const { isMultiUser } = useMultiUser();
  const authStore = useAuthStore();

  const isOwned = (r: HasAccess) => r.access?.is_owner !== false;
  const isShared = (r: HasAccess) => !!r.access && r.access.is_owner === false;
  // Owner, manage-grantee, or admin can edit/delete; use-grantees cannot.
  const canManage = (r: HasAccess) =>
    !r.access || r.access.is_owner || r.access.access_level === "manage" || authStore.isAdmin;
  // Who may open the Share dialog at all (multi-user + can manage the resource).
  const canShare = (r: HasAccess) => isMultiUser.value && canManage(r);
  // Who may mint manage-level grants (owner or admin only — backend enforces too).
  const canManageGrants = (r: HasAccess) => r.access?.is_owner !== false || authStore.isAdmin;
  const sharedLabel = (r: HasAccess) => `Shared · ${r.access?.access_level}`;
  const sharedTitle = (r: HasAccess) =>
    r.access?.shared_by ? `Shared by ${r.access.shared_by}` : "Shared with you";

  return {
    isMultiUser,
    isOwned,
    isShared,
    canManage,
    canShare,
    canManageGrants,
    sharedLabel,
    sharedTitle,
  };
}
