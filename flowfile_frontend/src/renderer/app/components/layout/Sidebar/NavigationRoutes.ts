import { connectionTypes } from "../../../views/ConnectionsView/connectionTypes";
import { catalogTabs } from "../../../views/CatalogView/catalogTabs";

export interface INavigationRoute {
  name: string;
  displayName: string;
  meta: { icon: string };
  children?: INavigationRoute[];
  // Query carried into the resolved route (el-menu router mode) — lets children
  // share a route name but target different ?tab= values.
  query?: Record<string, string>;
  // Unique el-menu index; falls back to `name` when absent. Required when several
  // children resolve to the same route name.
  index?: string;
  disabled?: boolean;
  requiresAdmin?: boolean;
  hideInElectron?: boolean;
  dockerOnly?: boolean;
}

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
      children: catalogTabs.map((t) => ({
        name: "catalog",
        index: `catalog:${t.key}`,
        query: { tab: t.key },
        displayName: t.sidebarKey,
        meta: { icon: t.icon },
      })),
    },
    {
      name: "connections",
      displayName: "menu.connections",
      meta: {
        icon: "fa-solid fa-link",
      },
      // Clicking the parent itself opens the Connections overview landing.
      query: { tab: "overview" },
      children: [
        {
          name: "connections",
          index: "connections:overview",
          query: { tab: "overview" },
          displayName: "menu.connectionsOverview",
          meta: { icon: "fa-solid fa-grip" },
        },
        ...connectionTypes.map((t) => ({
          name: "connections",
          index: `connections:${t.key}`,
          query: { tab: t.key },
          displayName: t.sidebarKey,
          meta: { icon: t.icon },
        })),
      ],
    },
    {
      name: "kernelManager",
      displayName: "menu.kernelManager",
      meta: {
        icon: "fa-solid fa-server",
      },
    },
    {
      name: "nodeDesigner",
      displayName: "menu.nodeDesigner",
      meta: {
        icon: "fa-solid fa-puzzle-piece",
      },
    },
    {
      name: "fileManager",
      displayName: "menu.fileManager",
      meta: {
        icon: "fa-solid fa-folder-open",
      },
      dockerOnly: true,
    },
    {
      name: "groups",
      displayName: "menu.groups",
      meta: {
        icon: "fa-solid fa-user-group",
      },
      dockerOnly: true,
    },
    {
      name: "admin",
      displayName: "menu.admin",
      meta: {
        icon: "fa-solid fa-users-cog",
      },
      requiresAdmin: true,
      hideInElectron: true,
    },
  ] as INavigationRoute[],
};
