<template>
  <div class="performance-container">
    <div class="mb-3">
      <h2 class="page-title">Performance</h2>
      <p class="page-description">
        How Flowfile runs the regular data nodes in your flows — joins, filters, formulas, file
        reads. These settings apply to the whole Flowfile instance. Python Script and custom nodes
        run in kernels instead — manage those on the Python Kernels tab.
      </p>
    </div>

    <WorkerPoolCard v-if="isAdmin" />

    <div v-else class="performance-locked">
      <i class="fa-solid fa-lock"></i>
      <h3>Administrator setting</h3>
      <p>
        Worker settings apply to everyone on this server, so only administrators can view or change
        them. There's nothing you need to set up — your flows use worker processes automatically.
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
