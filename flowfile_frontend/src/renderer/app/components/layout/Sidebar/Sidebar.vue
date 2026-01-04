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
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import NavigationRoutes from "./NavigationRoutes";
import MenuAccordion from "./menu/MenuAccordion.vue";
import Logo from "../Logo/Logo.vue";
import ThemeToggle from "../ThemeToggle/ThemeToggle.vue";

// Define the isCollapse prop
defineProps({
  isCollapse: {
    type: Boolean,
    default: false,
  },
});

// Define the toggle-collapse emit
defineEmits(["toggle-collapse"]);

const items = ref(NavigationRoutes.routes);
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
}
</style>
