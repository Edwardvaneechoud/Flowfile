<template>
  <div class="run-overview">
    <h2>Run History</h2>
    <RunHistoryTable
      @view-run="$emit('viewRun', $event)"
      @view-flow="$emit('viewFlow', $event)"
      @view-schedule-runs="$emit('viewScheduleRuns', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import RunHistoryTable from "./RunHistoryTable.vue";

defineEmits<{
  viewRun: [runId: number];
  viewFlow: [registrationId: number];
  viewScheduleRuns: [scheduleId: number];
}>();
</script>

<style scoped>
.run-overview {
  max-width: 1000px;
  margin: 0 auto;
}

.run-overview h2 {
  margin: 0 0 var(--spacing-5) 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}
</style>
