<template>
  <button
    class="theme-toggle"
    :title="isDark ? 'Switch to light mode' : 'Switch to dark mode'"
    @click="toggleTheme"
  >
    <i v-if="isDark" class="fas fa-sun"></i>
    <i v-else class="fas fa-moon"></i>
  </button>
</template>

<script setup lang="ts">
import { useTheme } from "../../../composables/useTheme";

const { isDark, toggleTheme } = useTheme();
</script>

<style scoped>
.theme-toggle {
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

.theme-toggle:hover {
  background-color: var(--color-background-hover);
  color: var(--color-accent);
  border-color: var(--color-accent);
}

.theme-toggle:focus {
  outline: none;
  box-shadow: 0 0 0 2px var(--color-accent-subtle);
  border-color: var(--color-accent);
}

.theme-toggle i {
  font-size: var(--font-size-lg);
}
</style>
