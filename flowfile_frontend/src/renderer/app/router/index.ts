// src/renderer/app/router/index.ts
import { createRouter, createWebHashHistory, RouteRecordRaw } from "vue-router";
import AppLayout from "../layouts/AppLayout.vue";
import authService from "../services/auth.service";
import { useAuthStore } from "../stores/auth-store";

const routes: Array<RouteRecordRaw> = [
  {
    path: "/",
    redirect: "/main",
  },
  {
    path: "/login",
    name: "login",
    component: () => import("../views/LoginView/LoginView.vue"),
    meta: { requiresAuth: false },
  },
  {
    path: "/main",
    component: AppLayout,
    meta: { requiresAuth: true },
    children: [
      {
        path: "",
        name: "main",
        redirect: { name: "designer" },
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
      {
        name: "nodeDesigner",
        path: "nodeDesigner",
        component: () => import("../pages/NodeDesigner.vue"),
      },
      {
        name: "admin",
        path: "admin",
        component: () => import("../views/AdminView/AdminView.vue"),
        meta: { requiresAdmin: true, hideInElectron: true },
      },
    ],
  },
  {
    path: "/:pathMatch(.*)*",
    redirect: { name: "designer" },
  },
];

const router = createRouter({
  history: createWebHashHistory(import.meta.env.BASE_URL as string),
  routes,
});

// Navigation guard for authentication
router.beforeEach(async (to, _from, next) => {
  const authStore = useAuthStore();
  const requiresAuth = to.matched.some((record) => record.meta.requiresAuth !== false);
  const hideInElectron = to.matched.some((record) => record.meta.hideInElectron);

  // Initialize auth store if authenticated but user info not loaded (e.g., page refresh)
  if (authService.isAuthenticated() && !authStore.user) {
    await authStore.initialize();
  }

  // Block routes that are hidden in Electron mode (e.g., admin/user management)
  if (hideInElectron && authService.isInElectronMode()) {
    next({ name: "designer" });
    return;
  }

  // Check if route requires auth
  if (requiresAuth) {
    // In Electron mode, always allow (auto-auth)
    if (authService.isInElectronMode()) {
      next();
      return;
    }

    // In Docker/web mode, check for valid token
    if (authService.isAuthenticated()) {
      next();
    } else {
      next({ name: "login" });
    }
  } else {
    // Route doesn't require auth (like login page)
    // If already authenticated and going to login, redirect to main
    if (to.name === "login" && authService.isAuthenticated()) {
      next({ name: "designer" });
    } else {
      next();
    }
  }
});

export default router;
