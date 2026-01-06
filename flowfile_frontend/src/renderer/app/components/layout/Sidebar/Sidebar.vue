<template>
  <div class="sidebar">
    <div class="center-container">
      <Logo width="50px" :app-name="''" position-app-name="left" />
    </div>
    <div class="sidebar-container">
      <menu-accordion :items="items" :is-collapse="true" />
    </div>
    <div class="sidebar-footer">
      <div class="footer-btn-wrapper" data-tooltip="Toggle theme">
        <ThemeToggle />
      </div>
      <div v-if="showLogout" class="footer-btn-wrapper" data-tooltip="Sign out">
        <button
          class="logout-button"
          @click="handleLogout"
        >
          <i class="fa-solid fa-right-from-bracket"></i>
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useRouter } from "vue-router";
import NavigationRoutes from "./NavigationRoutes";
import MenuAccordion from "./menu/MenuAccordion.vue";
import Logo from "../Logo/Logo.vue";
import ThemeToggle from "../ThemeToggle/ThemeToggle.vue";
import authService from "../../../services/auth.service";
import { useAuthStore } from "../../../stores/auth-store";

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
const authStore = useAuthStore();

// Filter routes based on admin status and Electron mode
const items = computed(() => {
  const isAdmin = authStore.isAdmin;
  const isElectron = authService.isInElectronMode();
  return NavigationRoutes.routes.filter(route => {
    // Hide routes marked as hideInElectron when in Electron mode
    if (route.hideInElectron && isElectron) return false;
    // Show route if it doesn't require admin, or if user is admin
    return !route.requiresAdmin || isAdmin;
  });
});

// Only show logout button in Docker/web mode
const showLogout = computed(() => !authService.isInElectronMode());

const handleLogout = () => {
  authService.logout();
  router.push({ name: 'login' });
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
</style>
