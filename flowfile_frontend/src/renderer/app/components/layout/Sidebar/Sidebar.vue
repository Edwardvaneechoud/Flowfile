<template>
  <div class="sidebar">
    <div class="center-container">
      <Logo width="50px" :app-name="''" position-app-name="left" />
    </div>
    <div class="sidebar-container">
      <menu-accordion :items="items" :is-collapse="true" />
    </div>
    <div class="sidebar-footer">
      <div class="footer-btn-wrapper" data-tooltip="Interactive tutorial">
        <button class="tutorial-button" @click="handleStartTutorial">
          <span class="material-icons">school</span>
        </button>
      </div>
      <div class="footer-btn-wrapper" data-tooltip="Toggle theme">
        <ThemeToggle />
      </div>
      <div v-if="currentPageHelp" class="footer-btn-wrapper" data-tooltip="Page info">
        <button class="info-button" @click="showHelp = true">
          <i class="fa-solid fa-circle-info"></i>
        </button>
      </div>
      <div v-if="showLogout" class="footer-btn-wrapper" data-tooltip="Sign out">
        <button class="logout-button" @click="handleLogout">
          <i class="fa-solid fa-right-from-bracket"></i>
        </button>
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
import NavigationRoutes from "./NavigationRoutes";
import MenuAccordion from "./menu/MenuAccordion.vue";
import Logo from "../Logo/Logo.vue";
import ThemeToggle from "../ThemeToggle/ThemeToggle.vue";
import { PageHelpModal } from "../../common";
import type { PageHelpContent } from "../../common/PageHelpModal/types";
import authService from "../../../services/auth.service";
import { useAuthStore } from "../../../stores/auth-store";
import { useTutorialStore } from "../../../stores/tutorial-store";
import { gettingStartedTutorial } from "../../tutorial/tutorials";
import { designerHelp } from "../../../views/DesignerView/designerHelp";
import { catalogHelp } from "../../../views/CatalogView/catalogHelp";
import { connectionsHelp } from "../../../views/ConnectionsView/connectionsHelp";
import { templatesHelp } from "../../../views/TemplatesView/templatesHelp";
import { kernelHelp } from "../../../views/KernelManagerView/kernelHelp";

// Define the isCollapse prop
defineProps({
  isCollapse: {
    type: Boolean,
    default: false,
  },
});

// Define the toggle-collapse emit
defineEmits(["toggle-collapse"]);

const router = useRouter();
const route = useRoute();
const authStore = useAuthStore();
const tutorialStore = useTutorialStore();

// Page help
const showHelp = ref(false);

const helpByRoute: Record<string, PageHelpContent> = {
  designer: designerHelp,
  catalog: catalogHelp,
  connections: connectionsHelp,
  templates: templatesHelp,
  kernelManager: kernelHelp,
};

const currentPageHelp = computed(() => {
  const name = route.name as string;
  return helpByRoute[name] ?? null;
});

// Close help modal on route change
watch(
  () => route.name,
  () => {
    showHelp.value = false;
  },
);

// Filter routes based on admin status and Electron mode
const items = computed(() => {
  const isAdmin = authStore.isAdmin;
  const isElectron = authService.isInElectronMode();
  return NavigationRoutes.routes.filter((route) => {
    // Hide routes marked as hideInElectron when in Electron mode
    if (route.hideInElectron && isElectron) return false;
    // Hide routes marked as dockerOnly when in Electron mode
    if (route.dockerOnly && isElectron) return false;
    // Show route if it doesn't require admin, or if user is admin
    return !route.requiresAdmin || isAdmin;
  });
});

// Only show logout button in Docker/web mode
const showLogout = computed(() => !authService.isInElectronMode());

const handleStartTutorial = async () => {
  if (router.currentRoute.value.name !== "designer") {
    await router.push({ name: "designer" });
  }
  tutorialStore.startTutorial(gettingStartedTutorial);
};

const handleLogout = () => {
  authService.logout();
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
  border-top: 1px solid var(--color-border-primary);
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

.logout-button {
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

.logout-button:hover {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  border-color: var(--color-danger);
}

.logout-button:focus {
  outline: none;
  box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.2);
  border-color: var(--color-danger);
}

.logout-button i {
  font-size: var(--font-size-lg);
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
</style>
