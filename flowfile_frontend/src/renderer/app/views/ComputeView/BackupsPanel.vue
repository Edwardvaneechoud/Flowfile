<template>
  <div class="backups-container">
    <div class="mb-3">
      <h2 class="page-title">Backups</h2>
      <p class="page-description">
        Snapshots of the catalog database — the connections, secrets, schedules and catalog metadata
        Flowfile stores. One is taken before every migration and before a desktop update.
      </p>
    </div>

    <DbBackupsCard v-if="isAdmin" />

    <div v-else class="backups-locked">
      <i class="fa-solid fa-lock"></i>
      <h3>Administrator setting</h3>
      <p>
        Database snapshots are server-wide and managed by administrators. Your flows and their data
        are unaffected.
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import DbBackupsCard from "../../components/settings/DbBackupsCard.vue";
import { useAuthStore } from "../../stores/auth-store";

const isAdmin = computed(() => useAuthStore().isAdmin);
</script>

<style scoped>
.backups-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

.backups-locked {
  text-align: center;
  padding: var(--spacing-8) var(--spacing-4);
  color: var(--color-text-secondary);
}

.backups-locked i {
  font-size: 2em;
  opacity: 0.4;
}
</style>
