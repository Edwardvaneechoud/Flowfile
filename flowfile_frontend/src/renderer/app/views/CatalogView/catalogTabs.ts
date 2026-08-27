// Single source of truth for the Catalog sections shown in the CatalogView
// header tab bar and the sidebar sub-menu (NavigationRoutes). Keep this file
// dependency-free (pure data) so it can be imported from the sidebar without
// risking an import cycle.

export const CATALOG_TAB_KEYS = [
  "catalog",
  "favorites",
  "sql",
  "notebook",
  "visuals",
  "runs",
  "schedules",
  "alerts",
  "apis",
  "customNodes",
  "community",
] as const;

export type CatalogTabKey = (typeof CATALOG_TAB_KEYS)[number];

export type CatalogTabGroup = "browse" | "analyze" | "operate" | "extend";

/** Sidebar group-header labels (i18n keys), keyed by tab group. */
export const CATALOG_TAB_GROUP_KEYS: Record<CatalogTabGroup, string> = {
  browse: "menu.catalogGroupBrowse",
  analyze: "menu.catalogGroupAnalyze",
  operate: "menu.catalogGroupOperate",
  extend: "menu.catalogGroupExtend",
};

/** Tab-bar group captions (plain text, like each tab's `label`). */
export const CATALOG_TAB_GROUP_LABELS: Record<CatalogTabGroup, string> = {
  browse: "Browse",
  analyze: "Analyze",
  operate: "Operate",
  extend: "Extend",
};

export interface CatalogTabDef {
  key: CatalogTabKey; // also the ?tab= query value
  label: string; // header tab label
  icon: string; // FontAwesome class
  sidebarKey: string; // i18n key for the sidebar child label
  group: CatalogTabGroup; // tab-bar cluster; a divider renders between groups
}

// Ordered as a workflow: find data, work with it, operate/monitor it, extend
// the platform. Reordering is safe — every consumer addresses tabs by key.
export const catalogTabs: CatalogTabDef[] = [
  {
    key: "catalog",
    label: "Catalog",
    icon: "fa-solid fa-folder-tree",
    sidebarKey: "menu.catalogBrowse",
    group: "browse",
  },
  {
    key: "favorites",
    label: "Favorites",
    icon: "fa-solid fa-star",
    sidebarKey: "menu.catalogFavorites",
    group: "browse",
  },
  {
    key: "sql",
    label: "SQL",
    icon: "fa-solid fa-code",
    sidebarKey: "menu.catalogSql",
    group: "analyze",
  },
  {
    key: "notebook",
    label: "Notebook",
    icon: "fa-solid fa-book",
    sidebarKey: "menu.catalogNotebook",
    group: "analyze",
  },
  {
    key: "visuals",
    label: "Visuals",
    icon: "fa-solid fa-chart-pie",
    sidebarKey: "menu.catalogVisuals",
    group: "analyze",
  },
  {
    key: "runs",
    label: "Run History",
    icon: "fa-solid fa-clock-rotate-left",
    sidebarKey: "menu.catalogRuns",
    group: "operate",
  },
  {
    key: "schedules",
    label: "Schedules",
    icon: "fa-solid fa-calendar-days",
    sidebarKey: "menu.catalogSchedules",
    group: "operate",
  },
  {
    key: "alerts",
    label: "Alerts",
    icon: "fa-solid fa-bell",
    sidebarKey: "menu.catalogAlerts",
    group: "operate",
  },
  {
    key: "apis",
    label: "APIs",
    icon: "fa-solid fa-plug",
    sidebarKey: "menu.catalogApis",
    group: "extend",
  },
  {
    key: "customNodes",
    label: "Custom Nodes",
    icon: "fa-solid fa-cube",
    sidebarKey: "menu.catalogCustomNodes",
    group: "extend",
  },
  {
    key: "community",
    label: "Community Nodes",
    icon: "fa-solid fa-store",
    sidebarKey: "menu.catalogCommunity",
    group: "extend",
  },
];
