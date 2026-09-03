<template>
  <div class="sidebar">
    <div class="center-container">
      <Logo width="50px" :app-name="''" position-app-name="left" :click-action="goHome" />
    </div>
    <div class="sidebar-container">
      <menu-accordion :items="items" :is-collapse="true" />
    </div>
    <div class="sidebar-footer">
      <div v-if="currentPageHelp" class="footer-btn-wrapper" data-tooltip="Page info">
        <button class="info-button" @click="showHelp = true">
          <i class="fa-solid fa-circle-info"></i>
        </button>
      </div>
      <div class="footer-btn-wrapper" data-tooltip="Help &amp; more">
        <el-popover
          placement="right-end"
          :width="220"
          trigger="click"
          popper-class="sidebar-more-popover"
          :show-arrow="true"
        >
          <template #reference>
            <button class="tutorial-button">
              <i class="fa-solid fa-circle-question"></i>
            </button>
          </template>
          <div class="sidebar-more-menu">
            <button class="sidebar-more-item" @click="handleStartTutorial">
              <span class="material-icons">school</span>
              <span>Interactive tutorial</span>
            </button>
            <button class="sidebar-more-item" @click="handleOpenTemplates">
              <i class="fa-solid fa-layer-group"></i>
              <span>Templates</span>
            </button>
            <button class="sidebar-more-item" @click="handleOpenDocumentation">
              <i class="fa-solid fa-book"></i>
              <span>Documentation</span>
            </button>
            <button class="sidebar-more-item" @click="handleOpenPrivacy">
              <i class="fa-solid fa-shield-halved"></i>
              <span>Privacy &amp; data collection</span>
            </button>
            <div class="sidebar-more-divider" aria-hidden="true"></div>
            <button class="sidebar-more-item" @click="toggleTheme">
              <i :class="isDark ? 'fa-solid fa-sun' : 'fa-solid fa-moon'"></i>
              <span>{{ isDark ? "Light mode" : "Dark mode" }}</span>
            </button>
            <button v-if="showLogout" class="sidebar-more-item is-danger" @click="handleLogout">
              <i class="fa-solid fa-right-from-bracket"></i>
              <span>Sign out</span>
            </button>
          </div>
        </el-popover>
      </div>
    </div>
    <PageHelpModal
      v-if="currentPageHelp"
      :show="showHelp"
      v-bind="currentPageHelp"
      @close="showHelp = false"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useRouter, useRoute } from "vue-router";
import NavigationRoutes, { type INavigationRoute } from "./NavigationRoutes";
import MenuAccordion from "./menu/MenuAccordion.vue";
import Logo from "../Logo/Logo.vue";
import { PageHelpModal } from "../../common";
import type { PageHelpContent } from "../../common/PageHelpModal/types";
import authService from "../../../services/auth.service";
import { desktop } from "../../../../lib/desktop";
import { DOCS_BASE_URL } from "../../../lib/docsLinks";
import { useAuthStore } from "../../../stores/auth-store";
import { useMultiUser } from "../../../composables/useMultiUser";
import { useTheme } from "../../../composables/useTheme";
import { useProjectStore } from "../../../stores/project-store";
import { useTutorialStore } from "../../../stores/tutorial-store";
import { gettingStartedTutorial } from "../../tutorial/tutorials";
import { designerHelp } from "../../../views/DesignerView/designerHelp";
import { catalogHelp } from "../../../views/CatalogView/catalogHelp";
import { lastCatalogQuery } from "../../../views/CatalogView/catalogLastLocation";
import { catalogSectionOfTab } from "../../../views/CatalogView/catalogTabs";
import { connectionsHelp } from "../../../views/ConnectionsView/connectionsHelp";
import { templatesHelp } from "../../../views/TemplatesView/templatesHelp";
import { computeHelp } from "../../../views/ComputeView/computeHelp";
import { dashboardHelp } from "../../../views/DashboardsView/dashboardHelp";
import { projectHelp } from "../../../views/ProjectView/projectHelp";

defineProps({
  isCollapse: {
    type: Boolean,
    default: false,
  },
});

defineEmits(["toggle-collapse"]);

const router = useRouter();
const route = useRoute();
const authStore = useAuthStore();
const projectStore = useProjectStore();
const tutorialStore = useTutorialStore();
const { isMultiUser } = useMultiUser();
const { isDark, toggleTheme } = useTheme();

// Page help
const showHelp = ref(false);

const helpByRoute: Record<string, PageHelpContent> = {
  designer: designerHelp,
  catalog: catalogHelp,
  connections: connectionsHelp,
  project: projectHelp,
  templates: templatesHelp,
  compute: computeHelp,
  "dashboard-new": dashboardHelp,
  "dashboard-edit": dashboardHelp,
  "dashboard-view": dashboardHelp,
};

const currentPageHelp = computed(() => {
  const name = route.name as string;
  if (name === "catalog" && route.query.tab === "dashboards") return dashboardHelp;
  return helpByRoute[name] ?? null;
});

watch(
  () => route.name,
  () => {
    showHelp.value = false;
  },
);

const items = computed(() => {
  const isAdmin = authStore.isAdmin;
  const isDesktopShell = authService.isInDesktopMode();
  const projectDot = projectStore.isActive ? projectStore.status : undefined;

  const visible = (entry: INavigationRoute) => {
    if ((entry.hideInElectron || entry.dockerOnly) && isDesktopShell) return false;
    // Projects are admin-only in docker; admins keep it even when disabled (to reach the how-to page).
    if (entry.name === "project" && isMultiUser.value && !isAdmin) return false;
    return !entry.requiresAdmin || isAdmin;
  };
  const decorate = (entry: INavigationRoute): INavigationRoute =>
    entry.name === "project" ? { ...entry, statusDot: projectDot } : entry;

  return NavigationRoutes.routes.filter(visible).map((route) => {
    if (!route.children) return decorate(route);
    const children = route.children.filter(visible).map(decorate);
    const parent: INavigationRoute = { ...route };
    // A child's status dot (project sync state) also shows on the collapsed rail icon.
    const statusDot = children.find((c) => c.statusDot)?.statusDot;
    if (statusDot) parent.statusDot = statusDot;
    // The catalog icon resumes the last sub-page, unless that page lives under Settings.
    const last = lastCatalogQuery.value;
    if (route.name === "catalog" && last && catalogSectionOfTab(last.tab) === "catalog") {
      parent.query = last;
    }
    // A one-child sub-menu is noise — flatten to a plain item (the page
    // defaults to the surviving tab anyway).
    return children.length > 1 ? { ...parent, children } : { ...parent, children: undefined };
  });
});

const showLogout = computed(() => !authService.isInDesktopMode());

const goHome = () => router.push({ name: "home" });

const handleStartTutorial = async () => {
  if (router.currentRoute.value.name !== "designer") {
    await router.push({ name: "designer" });
  }
  tutorialStore.startTutorial(gettingStartedTutorial);
};

const handleOpenTemplates = () => {
  router.push({ name: "templates" });
};

const handleOpenPrivacy = () => {
  router.push({ name: "compute", query: { tab: "privacy" } });
};

const handleOpenDocumentation = () => {
  void desktop.openExternal(DOCS_BASE_URL);
};

const handleLogout = () => {
  // Store logout, not authService: per-user store state must be torn down too.
  authStore.logout();
  router.push({ name: "login" });
};
</script>

<style lang="scss">
.sidebar {
  height: 100%;
  display: flex;
  flex-direction: column;
  background-color: var(--color-background-primary);
}

.sidebar-container {
  display: flex;
  flex-direction: column;
  flex: 1;
}

.center-container {
  display: flex;
  justify-content: center;
  width: 100%;
  padding: var(--spacing-8) 0;
}

.sidebar-footer {
  margin-top: auto;
  padding: var(--spacing-3);
  border-top: 1px solid var(--color-border-primary);
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-2);
}

/* Uses centralized [data-tooltip] styles from _modals.css */
.footer-btn-wrapper {
  position: relative;
}

.tutorial-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  padding: 0;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-base) var(--transition-timing);
}

.tutorial-button:hover {
  background-color: var(--color-accent-light, rgba(59, 130, 246, 0.1));
  color: var(--color-accent);
  border-color: var(--color-accent);
}

.tutorial-button:focus {
  outline: none;
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
  border-color: var(--color-accent);
}

.tutorial-button .material-icons {
  font-size: 20px;
}

.info-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  padding: 0;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-base) var(--transition-timing);
}

.info-button:hover {
  background-color: var(--color-accent-light, rgba(59, 130, 246, 0.1));
  color: var(--color-accent);
  border-color: var(--color-accent);
}

.info-button:focus {
  outline: none;
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
  border-color: var(--color-accent);
}

.info-button i {
  font-size: var(--font-size-lg);
}

.sidebar-more-menu {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.sidebar-more-item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: none;
  border-radius: var(--border-radius-md);
  background-color: transparent;
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  text-align: left;
  cursor: pointer;
  transition: all var(--transition-base) var(--transition-timing);
}

.sidebar-more-item:hover {
  background-color: var(--color-accent-light, rgba(59, 130, 246, 0.1));
  color: var(--color-accent);
}

.sidebar-more-item i,
.sidebar-more-item .material-icons {
  width: 18px;
  font-size: var(--font-size-base);
  color: var(--color-text-secondary);
}

.sidebar-more-item:hover i,
.sidebar-more-item:hover .material-icons {
  color: var(--color-accent);
}

.sidebar-more-item.is-danger:hover {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.sidebar-more-item.is-danger:hover i {
  color: var(--color-danger);
}

.sidebar-more-divider {
  height: 1px;
  margin: var(--spacing-1) var(--spacing-2);
  background-color: var(--color-border-primary);
}
</style>
