import { createRouter, createWebHistory, RouteRecordRaw } from "vue-router";
import AppLayout from "../layouts/AppLayout.vue";
import RouteViewComponent from "../layouts/RouterBypass.vue";

const routes: Array<RouteRecordRaw> = [
  {
    path: "/",
    redirect: { name: "designer" }  // First redirect to admin
  },
  {
    name: "admin",
    path: "/admin",
    component: AppLayout,
    children: [
      {
        name: "designer",
        path: "",  // Make this the default child route
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
  history: createWebHistory(import.meta.env.BASE_URL),
  routes,
});

export default router;