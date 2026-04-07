export interface INavigationRoute {
  name: string;
  displayName: string;
  meta: { icon: string };
  children?: INavigationRoute[];
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
    },
    {
      name: "documentation",
      displayName: "menu.documentation",
      meta: {
        icon: "fa-solid fa-book",
      },
    },
    {
      name: "connections",
      displayName: "menu.connections",
      meta: {
        icon: "fa-solid fa-link",
      },
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
