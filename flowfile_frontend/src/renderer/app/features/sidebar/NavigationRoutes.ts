export interface INavigationRoute {
  name: string;
  displayName: string;
  meta: { icon: string };
  children?: INavigationRoute[];
  disabled?: boolean;
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
        icon: "fa-solid fa-diagram-project", // Flow designer icon
      },
    },
    {
      name: "documentation",  // This should match the route name in your router
      displayName: "menu.documentation",
      meta: {
        icon: "fa-solid fa-book", // Documentation icon
      },
    },
  ] as INavigationRoute[],
};