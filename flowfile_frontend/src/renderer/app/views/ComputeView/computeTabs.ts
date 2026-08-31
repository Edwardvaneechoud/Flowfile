// Single source of truth for the Compute sections shown in the ComputeView
// header tab bar and the sidebar sub-menu (NavigationRoutes). Keep this file
// dependency-free (pure data) so it can be imported from the sidebar without
// risking an import cycle.

export const COMPUTE_TAB_KEYS = ["kernels", "performance", "privacy"] as const;

export type ComputeTabKey = (typeof COMPUTE_TAB_KEYS)[number];

export interface ComputeTabDef {
  key: ComputeTabKey; // also the ?tab= query value
  label: string; // header tab label
  icon: string; // FontAwesome class
  sidebarKey: string; // i18n key for the sidebar child label
  requiresAdmin?: boolean; // hide from nav + tab bar for non-admins
}

export const computeTabs: ComputeTabDef[] = [
  {
    key: "kernels",
    label: "Python Kernels",
    icon: "fa-brands fa-python",
    sidebarKey: "menu.computeKernels",
  },
  {
    key: "performance",
    label: "Performance",
    icon: "fa-solid fa-gauge-high",
    sidebarKey: "menu.computePerformance",
    requiresAdmin: true,
  },
  {
    key: "privacy",
    label: "Privacy",
    icon: "fa-solid fa-shield-halved",
    sidebarKey: "menu.computePrivacy",
  },
];
