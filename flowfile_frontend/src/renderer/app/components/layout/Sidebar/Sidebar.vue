<template>
  <div class="sidebar">
    <div class="center-container">
      <Logo width="50px" :app-name="''" position-app-name="left" />
    </div>
    <div class="sidebar-container">
      <menu-accordion :items="items" :is-collapse="true" />
    </div>
    <div class="sidebar-footer">
      <ThemeToggle />
      <button
        v-if="showLogout"
        class="logout-button"
        title="Sign Out"
        @click="handleLogout"
      >
        <i class="fa-solid fa-right-from-bracket"></i>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { useRouter } from "vue-router";
import NavigationRoutes from "./NavigationRoutes";
import MenuAccordion from "./menu/MenuAccordion.vue";
import Logo from "../Logo/Logo.vue";
import ThemeToggle from "../ThemeToggle/ThemeToggle.vue";
import authService from "../../../services/auth.service";

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
const items = ref(NavigationRoutes.routes);

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
  padding: var(--spacing-4);
  border-top: 1px solid var(--color-border-primary);
  display: flex;
  justify-content: center;
  gap: var(--spacing-3);
  align-items: center;
}

.logout-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border: none;
  border-radius: var(--border-radius-md);
  background-color: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.logout-button:hover {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.logout-button i {
  font-size: var(--font-size-base);
}
</style>
