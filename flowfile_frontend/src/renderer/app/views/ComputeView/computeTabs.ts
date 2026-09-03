// Single source of truth for the Compute sections shown in the ComputeView
// header tab bar and the sidebar sub-menu (NavigationRoutes). Keep this file
// dependency-free (pure data) so it can be imported from the sidebar without
// risking an import cycle.

export const COMPUTE_TAB_KEYS = ["kernels", "performance", "privacy", "backups"] as const;

export type ComputeTabKey = (typeof COMPUTE_TAB_KEYS)[number];

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
    key: "performance",
    label: "Performance",
    icon: "fa-solid fa-gauge-high",
    sidebarKey: "menu.computePerformance",
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
