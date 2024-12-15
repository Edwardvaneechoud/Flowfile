import { createRouter, createWebHashHistory, RouteRecordRaw } from "vue-router";
import AppLayout from "../layouts/AppLayout.vue";
import RouteViewComponent from "../layouts/RouterBypass.vue";

const routes: Array<RouteRecordRaw> = [
  {
    path: "/",
    redirect: "/main"  // Redirect directly to /main
  },
  {
    path: "/main",
    component: AppLayout,
    children: [
      {
        path: "",  // Empty path for default child
        name: "main",
        redirect: { name: "designer" }  // Explicitly redirect to designer
      },
      {
        path: "designer",
        name: "designer",
        component: () => import("../pages/designer.vue"),
      },
      {
        name: "nodeData",
        path: "nodeData",
        component: () => import("../features/designer/editor/fullEditor.vue"),
      },
      {
        name: "documentation",
        path: "documentation",
        component: RouteViewComponent,
      },
    ],
  },
  {
    path: "/:pathMatch(.*)*",
    redirect: { name: "designer" }
  }
];

const router = createRouter({
  history: createWebHashHistory(import.meta.env.BASE_URL as string),
  routes,
});

export default router;