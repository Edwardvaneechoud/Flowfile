<template>
  <div class="performance-container">
    <div class="mb-3">
      <h2 class="page-title">Performance</h2>
      <p class="page-description">
        Data nodes run in the worker service; the warm worker pool keeps worker processes ready
        between nodes. Python Script and custom nodes run in Python kernels, managed in the Python
        Kernels tab.
      </p>
    </div>

    <WorkerPoolCard v-if="isAdmin" />

    <div v-else class="performance-locked">
      <i class="fa-solid fa-lock"></i>
      <h3>Administrator setting</h3>
      <p>
        Worker settings are server-wide and managed by administrators. Your flows use the worker
        service automatically.
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import WorkerPoolCard from "../../components/settings/WorkerPoolCard.vue";
import { useAuthStore } from "../../stores/auth-store";

const isAdmin = computed(() => useAuthStore().isAdmin);
</script>

<style scoped>
.performance-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

.performance-locked {
  text-align: center;
  padding: var(--spacing-8) var(--spacing-4);
  color: var(--color-text-secondary);
}

.performance-locked i {
  font-size: 2em;
  opacity: 0.4;
}
</style>
