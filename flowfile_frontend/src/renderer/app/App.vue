<script setup lang="ts">
import { useNativeContextMenuGuard } from "./composables/useNativeContextMenuGuard";

// Suppress WebKit's native right-click menu in the desktop shell (no-op on web).
useNativeContextMenuGuard();
</script>

<template>
  <router-view />
</template>

<style lang="scss">
#app {
  font-family: var(--font-family-base);
  color: var(--color-text-primary);
}

html,
body,
#app {
  height: 100%;
  margin: 0;
  overflow: hidden;
}
</style>
