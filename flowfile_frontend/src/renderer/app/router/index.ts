import { createRouter, createWebHashHistory, RouteRecordRaw } from "vue-router";
import AppLayout from "../layouts/AppLayout.vue";
import authService from "../services/auth.service";
import setupService from "../services/setup.service";
import { useAuthStore } from "../stores/auth-store";

const routes: Array<RouteRecordRaw> = [
  {
    path: "/",
    redirect: "/main",
  },
  {
    path: "/setup",
    name: "setup",
    component: () => import("../views/SetupView/SetupView.vue"),
    meta: { requiresAuth: false, isSetupPage: true },
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
        name: "kernelManager",
        path: "kernelManager",
        component: () => import("../views/KernelManagerView/KernelManagerView.vue"),
      },
      {
        name: "catalog",
        path: "catalog",
        component: () => import("../views/CatalogView/CatalogView.vue"),
      },
      {
        name: "nodeDesigner",
        path: "nodeDesigner",
        component: () => import("../pages/NodeDesigner.vue"),
      },
      {
        name: "fileManager",
        path: "fileManager",
        component: () => import("../views/FileManagerView/FileManagerView.vue"),
        meta: { dockerOnly: true },
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

let setupChecked = false;
let setupRequired = false;

router.beforeEach(async (to, _from, next) => {
  const authStore = useAuthStore();
  const requiresAuth = to.matched.some((record) => record.meta.requiresAuth !== false);
  const hideInElectron = to.matched.some((record) => record.meta.hideInElectron);
  const isSetupPage = to.matched.some((record) => record.meta.isSetupPage);

  // First, check if we need to get mode from backend (for "flowfile run ui" case)
  if (!setupChecked || isSetupPage) {
    try {
      const status = await setupService.getSetupStatus(isSetupPage);
      // Update auth service with backend mode - this handles "flowfile run ui"
      // where electronAPI doesn't exist but backend is in electron mode
      authService.setModeFromBackend(status.mode);
      setupRequired = status.setup_required;
      setupChecked = status.mode !== "unknown";
    } catch {
      setupRequired = true;
      setupChecked = false;
    }
  }

  if (!authService.isInElectronMode()) {
    if (setupRequired && !isSetupPage) {
      next({ name: "setup" });
      return;
    }

    if (!setupRequired && isSetupPage) {
      next({ name: "login" });
      return;
    }
  } else {
    if (isSetupPage) {
      next({ name: "designer" });
      return;
    }
  }

  if (authService.isAuthenticated() && !authStore.user) {
    await authStore.initialize();
  }

  if (hideInElectron && authService.isInElectronMode()) {
    next({ name: "designer" });
    return;
  }

  const dockerOnly = to.matched.some((record) => record.meta.dockerOnly);
  if (dockerOnly && authService.isInElectronMode()) {
    next({ name: "designer" });
    return;
  }

  if (requiresAuth) {
    if (authService.isInElectronMode()) {
      next();
      return;
    }
    if (authService.isAuthenticated()) {
      next();
    } else {
      next({ name: "login" });
    }
  } else {
   if (to.name === "login" && (authService.isInElectronMode() || authService.isAuthenticated())) {
      next({ name: "designer" });
    } else {
      next();
    }
  }
});

export default router;
