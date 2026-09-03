import {
  CATALOG_TAB_GROUP_KEYS,
  catalogTabsInSection,
  type CatalogTabDef,
} from "../../../views/CatalogView/catalogTabs";
import { COMPUTE_TAB_GROUP_KEYS, computeTabs } from "../../../views/ComputeView/computeTabs";
import { connectionTypes } from "../../../views/ConnectionsView/connectionTypes";

export interface INavigationRoute {
  name: string;
  displayName: string;
  meta: { icon: string };
  children?: INavigationRoute[];
  // Query carried into the resolved route (el-menu router mode) — lets children
  // share a route name but target different ?tab= values.
  query?: Record<string, string>;
  // Unique el-menu index; falls back to `name` when absent. Required when several
  // entries resolve to the same route name.
  index?: string;
  disabled?: boolean;
  requiresAdmin?: boolean;
  hideInElectron?: boolean;
  dockerOnly?: boolean;
  // Status-colored dot overlaid on the item's icon (e.g. project sync state).
  // Injected at render time, not part of the static route table.
  statusDot?: string;
  // Collapsible section this child belongs to in the sidebar sub-menu; adjacent
  // children sharing a key render under one toggleable group header.
  group?: { key: string; labelKey: string };
}

type NavGroup = INavigationRoute["group"];

const catalogChild = (t: CatalogTabDef, group?: NavGroup): INavigationRoute => ({
  name: "catalog",
  index: `catalog:${t.key}`,
  query: { tab: t.key },
  displayName: t.sidebarKey,
  meta: { icon: t.icon },
  group,
});

const extensionsGroup: NavGroup = { key: "extensions", labelKey: "menu.settingsGroupExtensions" };
const workspaceGroup: NavGroup = { key: "workspace", labelKey: "menu.settingsGroupWorkspace" };

// The rail is organised by intent (build, catalog, settings) rather than by backend
// subsystem; admin-only and docker-only entries all live under Settings.
export default {
  root: {
    name: "/",
    displayName: "navigationRoutes.home",
  },
  routes: [
    {
      name: "home",
      displayName: "menu.home",
      meta: {
        icon: "fa-solid fa-house",
      },
    },
    {
      name: "designer",
      displayName: "menu.designer",
      meta: {
        icon: "fa-solid fa-diagram-project",
      },
    },
    {
      name: "catalog",
      displayName: "menu.catalog",
      meta: {
        icon: "fa-solid fa-folder-tree",
      },
      // Clicking the parent itself opens the catalog browse / tree view.
      query: { tab: "catalog" },
      children: catalogTabsInSection("catalog").map((t) =>
        catalogChild(t, { key: t.group, labelKey: CATALOG_TAB_GROUP_KEYS[t.group] }),
      ),
    },
    {
      name: "connections",
      index: "settings",
      displayName: "menu.settings",
      meta: {
        icon: "fa-solid fa-gear",
      },
      query: { tab: "overview" },
      children: [
        {
          name: "connections",
          displayName: "menu.connectionsAll",
          meta: { icon: "fa-solid fa-link" },
          group: { key: "connections", labelKey: "menu.settingsGroupConnections" },
          // Nested level: the title opens the overview, the entries one connection type each.
          query: { tab: "overview" },
          children: [
            {
              name: "connections",
              index: "connections:overview",
              query: { tab: "overview" },
              displayName: "menu.connectionsOverview",
              meta: { icon: "fa-solid fa-grip" },
            },
            ...connectionTypes.map(
              (t): INavigationRoute => ({
                name: "connections",
                index: `connections:${t.key}`,
                query: { tab: t.key },
                displayName: t.sidebarKey,
                meta: { icon: t.icon },
              }),
            ),
          ],
        },
        ...computeTabs.map(
          (t): INavigationRoute => ({
            name: "compute",
            index: `compute:${t.key}`,
            query: { tab: t.key },
            displayName: t.sidebarKey,
            meta: { icon: t.icon },
            requiresAdmin: t.requiresAdmin,
            group: { key: t.group, labelKey: COMPUTE_TAB_GROUP_KEYS[t.group] },
          }),
        ),
        {
          name: "nodeDesigner",
          displayName: "menu.nodeDesigner",
          meta: { icon: "fa-solid fa-puzzle-piece" },
          group: extensionsGroup,
        },
        ...catalogTabsInSection("extend").map((t) => catalogChild(t, extensionsGroup)),
        {
          name: "project",
          displayName: "menu.project",
          meta: { icon: "fa-solid fa-code-branch" },
          group: workspaceGroup,
        },
        {
          name: "fileManager",
          displayName: "menu.fileManager",
          meta: { icon: "fa-solid fa-folder-open" },
          dockerOnly: true,
          group: workspaceGroup,
        },
        {
          name: "groups",
          displayName: "menu.groups",
          meta: { icon: "fa-solid fa-user-group" },
          dockerOnly: true,
          group: workspaceGroup,
        },
        {
          name: "admin",
          displayName: "menu.admin",
          meta: { icon: "fa-solid fa-users-cog" },
          requiresAdmin: true,
          hideInElectron: true,
          group: workspaceGroup,
        },
      ],
    },
  ] as INavigationRoute[],
};
