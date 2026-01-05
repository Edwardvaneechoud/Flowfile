export interface INavigationRoute {
  name: string;
  displayName: string;
  meta: { icon: string };
  children?: INavigationRoute[];
  disabled?: boolean;
  requiresAdmin?: boolean;
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
      name: "documentation",
      displayName: "menu.documentation",
      meta: {
        icon: "fa-solid fa-book",
      },
    },
    {
      name: "databaseManager",
      displayName: "menu.databaseManager",
      meta: {
        icon: "fa-solid fa-database"
      }
    },
    {
      name: "cloudConnectionManager",
      displayName: "menu.cloudConnectionManager",
      meta: {
        icon: "fa-solid fa-cloud"
      }
    },
    {
      name: "secretManager",
      displayName: "menu.secretManager",
      meta: {
        icon: "fa-solid fa-key",
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
      name: "admin",
      displayName: "menu.admin",
      meta: {
        icon: "fa-solid fa-users-cog",
      },
      requiresAdmin: true,
    },
  ] as INavigationRoute[],
};