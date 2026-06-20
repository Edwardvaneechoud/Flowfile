// Single source of truth for the Catalog sections shown in the CatalogView
// header tab bar and the sidebar sub-menu (NavigationRoutes). Keep this file
// dependency-free (pure data) so it can be imported from the sidebar without
// risking an import cycle.

export const CATALOG_TAB_KEYS = [
  "catalog",
  "favorites",
  "runs",
  "schedules",
  "sql",
  "notebook",
  "visuals",
  "apis",
] as const;

export type CatalogTabKey = (typeof CATALOG_TAB_KEYS)[number];

export interface CatalogTabDef {
  key: CatalogTabKey; // also the ?tab= query value
  label: string; // header tab label
  icon: string; // FontAwesome class
  sidebarKey: string; // i18n key for the sidebar child label
}

export const catalogTabs: CatalogTabDef[] = [
  {
    key: "catalog",
    label: "Catalog",
    icon: "fa-solid fa-folder-tree",
    sidebarKey: "menu.catalogBrowse",
  },
  {
    key: "favorites",
    label: "Favorites",
    icon: "fa-solid fa-star",
    sidebarKey: "menu.catalogFavorites",
  },
  {
    key: "runs",
    label: "Run History",
    icon: "fa-solid fa-clock-rotate-left",
    sidebarKey: "menu.catalogRuns",
  },
  {
    key: "schedules",
    label: "Schedules",
    icon: "fa-solid fa-calendar-days",
    sidebarKey: "menu.catalogSchedules",
  },
  {
    key: "sql",
    label: "SQL",
    icon: "fa-solid fa-code",
    sidebarKey: "menu.catalogSql",
  },
  {
    key: "notebook",
    label: "Notebook",
    icon: "fa-solid fa-book",
    sidebarKey: "menu.catalogNotebook",
  },
  {
    key: "visuals",
    label: "Visuals",
    icon: "fa-solid fa-chart-pie",
    sidebarKey: "menu.catalogVisuals",
  },
  {
    key: "apis",
    label: "APIs",
    icon: "fa-solid fa-plug",
    sidebarKey: "menu.catalogApis",
  },
];
