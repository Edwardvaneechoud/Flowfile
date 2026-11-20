import type { AccessInfo } from "../../types/sharing.types";

/**
 * Whether the person viewing may edit a dashboard or chart, as far as the page can tell. Core
 * stamps `access` only where sharing is on: the owner reads `owner`, a group grant `use` or
 * `manage`. Without it (the desktop app, an admin, a resource reached another way) the rights are
 * unknown and editing stays offered, since core still refuses a save it does not allow.
 */
export function mayEdit(access: AccessInfo | null | undefined): boolean {
  return access?.access_level !== "use";
}
