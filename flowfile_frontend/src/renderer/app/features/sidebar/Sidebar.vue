<template>
  <div class="sidebar">
    <div class="center-container">
      <Logo width="50px" :app-name="''" position-app-name="left" />
    </div>
    <div class="sidebar-container">
      <menu-accordion :items="items" :is-collapse="true" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import NavigationRoutes from "./NavigationRoutes";
import MenuAccordion from "./menu/MenuAccordion.vue";
import Logo from "../logo/Logo.vue";

// Define the isCollapse prop
const props = defineProps({
  isCollapse: {
    type: Boolean,
    default: false
  }
});

// Define the toggle-collapse emit
const emit = defineEmits(['toggle-collapse']);

const items = ref(NavigationRoutes.routes);
</script>

<style lang="scss">
.sidebar {
  height: 100%; // Add this if you need full height
}

.sidebar-container {
  border-top: 0.5px solid #dfdfdf;
  display: flex;
  flex-direction: column;
}

.center-container {
  display: flex;
  justify-content: center;
  width: 100%;
  padding: 2rem 0;
}
</style>