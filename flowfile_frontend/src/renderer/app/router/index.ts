// src/renderer/app/router/index.ts
import { createRouter, createWebHashHistory, RouteRecordRaw } from "vue-router";
import AppLayout from "../layouts/AppLayout.vue";

const routes: Array<RouteRecordRaw> = [
  {
    path: "/",
    redirect: "/main"
  },
  {
    path: "/main",
    component: AppLayout,
    children: [
      {
        path: "",
        name: "main",
        redirect: { name: "designer" }
      },
      {
        path: "designer",
        name: "designer",
        component: () => import("../views/DesignerView/DesignerView.vue"),
      },
      {
        name: "nodeData",
        path: "nodeData",
        component: () => import("../features/designer/editor/fullEditor.vue"),
      },
      {
        name: "documentation",
        path: "documentation",
        component: () => import("../views/DocumentationView/DocumentationView.vue"),
      },
      {
        name: "databaseManager",
        path: "databaseManager",
        component: () => import("../views/DatabaseView/DatabaseView.vue"),
      },
      {
        name: "cloudConnectionManager",
        path: "cloudConnectionManager",
        component: () => import("../views/CloudConnectionView/CloudConnectionView.vue"),
      },
      {
        name: "secretManager",
        path: "secretManager",
        component: () => import("../views/SecretsView/SecretsView.vue"),
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
