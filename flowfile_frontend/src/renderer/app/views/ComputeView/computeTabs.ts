// Single source of truth for the Compute sections shown in the ComputeView
// header tab bar and the sidebar sub-menu (NavigationRoutes). Keep this file
// dependency-free (pure data) so it can be imported from the sidebar without
// risking an import cycle.

export const COMPUTE_TAB_KEYS = ["kernels", "backends", "privacy", "backups"] as const;

export type ComputeTabKey = (typeof COMPUTE_TAB_KEYS)[number];

/** Old ?tab= values that still resolve (the warm pool used to be its own Performance tab). */
export const COMPUTE_TAB_ALIASES: Record<string, ComputeTabKey> = {
  performance: "backends",
};

/** The tab a ?tab= value names, retired aliases included; null when it names none. */
export function resolveComputeTab(value: unknown): ComputeTabKey | null {
  if (typeof value !== "string") return null;
  if ((COMPUTE_TAB_KEYS as ReadonlyArray<string>).includes(value)) return value as ComputeTabKey;
  return COMPUTE_TAB_ALIASES[value] ?? null;
}

// "preferences" is app-wide settings parked under Compute until they earn their own route.
export type ComputeTabGroup = "execution" | "preferences";

/** Sidebar group-header labels (i18n keys), keyed by tab group. */
export const COMPUTE_TAB_GROUP_KEYS: Record<ComputeTabGroup, string> = {
  execution: "menu.computeGroupExecution",
  preferences: "menu.computeGroupPreferences",
};

/** Tab-bar group captions (plain text, like each tab's `label`). */
export const COMPUTE_TAB_GROUP_LABELS: Record<ComputeTabGroup, string> = {
  execution: "Execution",
  preferences: "Preferences",
};

export interface ComputeTabDef {
  key: ComputeTabKey; // also the ?tab= query value
  label: string; // header tab label
  icon: string; // FontAwesome class
  sidebarKey: string; // i18n key for the sidebar child label
  group: ComputeTabGroup; // tab-bar cluster; a divider renders between groups
  requiresAdmin?: boolean; // hide from nav + tab bar for non-admins
}

export const computeTabs: ComputeTabDef[] = [
  {
    key: "kernels",
    label: "Python Kernels",
    icon: "fa-brands fa-python",
    sidebarKey: "menu.computeKernels",
    group: "execution",
  },
  {
    key: "backends",
    label: "Backends",
    icon: "fa-solid fa-network-wired",
    sidebarKey: "menu.computeBackends",
    group: "execution",
    requiresAdmin: true,
  },
  {
    key: "privacy",
    label: "Privacy",
    icon: "fa-solid fa-shield-halved",
    sidebarKey: "menu.computePrivacy",
    group: "preferences",
  },
  {
    key: "backups",
    label: "Backups",
    icon: "fa-solid fa-database",
    sidebarKey: "menu.computeBackups",
    group: "preferences",
    requiresAdmin: true,
  },
];
