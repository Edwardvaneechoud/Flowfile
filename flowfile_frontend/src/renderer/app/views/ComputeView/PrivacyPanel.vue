<template>
  <div class="privacy-container">
    <div class="mb-3">
      <h2 class="page-title">Privacy</h2>
      <p class="page-description">
        Control what Flowfile shares. Anonymous usage telemetry is turned off by default — nothing
        is ever sent without consent.
      </p>
    </div>

    <TelemetryCard />
  </div>
</template>

<script setup lang="ts">
import TelemetryCard from "../../components/settings/TelemetryCard.vue";
</script>

<style scoped>
.privacy-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}
</style>
