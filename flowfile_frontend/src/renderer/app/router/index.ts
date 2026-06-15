import { createRouter, createWebHashHistory, RouteRecordRaw } from "vue-router";
import AppLayout from "../layouts/AppLayout.vue";
import authService from "../services/auth.service";
import setupService from "../services/setup.service";
import { useAuthStore } from "../stores/auth-store";
import { FlowApi } from "../api";

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
        // App entry: resume into the designer when flows are already open
        // (matches the pre-Home startup behavior); land on Home only when
        // nothing is open. The component is an unreachable fallback — the
        // guard always redirects.
        path: "",
        name: "main",
        component: () => import("../views/HomeView/HomeView.vue"),
        beforeEnter: async (_to, _from, next) => {
          try {
            const flows = await FlowApi.getAllFlows();
            next({ name: flows.length > 0 ? "designer" : "home" });
          } catch {
            next({ name: "home" });
          }
        },
      },
      {
        path: "home",
        name: "home",
        component: () => import("../views/HomeView/HomeView.vue"),
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
        name: "connections",
        path: "connections",
        component: () => import("../views/ConnectionsView/ConnectionsView.vue"),
      },
      {
        name: "workspace",
        path: "workspace",
        component: () => import("../views/WorkspaceView/WorkspaceView.vue"),
      },
      {
        path: "databaseManager",
        redirect: { name: "connections", query: { tab: "database" } },
      },
      {
        path: "cloudConnectionManager",
        redirect: { name: "connections", query: { tab: "cloud" } },
      },
      {
        path: "kafkaConnectionManager",
        redirect: { name: "connections", query: { tab: "kafka" } },
      },
      {
        path: "secretManager",
        redirect: { name: "connections", query: { tab: "secrets" } },
      },
      {
        path: "aiProviders",
        redirect: { name: "connections", query: { tab: "ai" } },
      },
      {
        name: "kernelManager",
        path: "kernelManager",
        component: () => import("../views/KernelManagerView/KernelManagerView.vue"),
      },
      {
        name: "templates",
        path: "templates",
        component: () => import("../views/TemplatesView/TemplatesView.vue"),
      },
      {
        name: "catalog",
        path: "catalog",
        component: () => import("../views/CatalogView/CatalogView.vue"),
      },
      {
        name: "dashboard-new",
        path: "dashboards/new",
        component: () => import("../views/DashboardsView/DashboardEditorView.vue"),
      },
      {
        name: "dashboard-edit",
        path: "dashboards/:id/edit",
        component: () => import("../views/DashboardsView/DashboardEditorView.vue"),
        props: true,
      },
      {
        name: "dashboard-view",
        path: "dashboards/:id",
        component: () => import("../views/DashboardsView/DashboardViewerView.vue"),
        props: true,
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
      {
        name: "groups",
        path: "groups",
        component: () => import("../views/GroupsView/GroupsView.vue"),
        meta: { dockerOnly: true },
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
      // where the desktop runtime isn't present but the backend is in desktop mode
      authService.setModeFromBackend(status.mode);
      setupRequired = status.setup_required;
      setupChecked = status.mode !== "unknown";
    } catch {
      setupRequired = true;
      setupChecked = false;
    }
  }

  if (!authService.isInDesktopMode()) {
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
      next({ name: "home" });
      return;
    }
  }

  if (authService.isAuthenticated() && !authStore.user) {
    await authStore.initialize();
  }

  if (hideInElectron && authService.isInDesktopMode()) {
    next({ name: "home" });
    return;
  }

  const dockerOnly = to.matched.some((record) => record.meta.dockerOnly);
  if (dockerOnly && authService.isInDesktopMode()) {
    next({ name: "home" });
    return;
  }

  if (requiresAuth) {
    if (authService.isInDesktopMode()) {
      next();
      return;
    }
    if (authService.isAuthenticated()) {
      next();
    } else {
      next({ name: "login" });
    }
  } else {
    if (to.name === "login" && (authService.isInDesktopMode() || authService.isAuthenticated())) {
      next({ name: "home" });
    } else {
      next();
    }
  }
});

export default router;
