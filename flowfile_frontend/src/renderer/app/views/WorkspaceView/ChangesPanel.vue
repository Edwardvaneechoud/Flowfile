<template>
  <section class="ws-card">
    <header class="ws-card-head">
      <h3 class="ws-card-title"><i class="fa-solid fa-pen" /> Changes since last checkpoint</h3>
      <span class="ws-card-sub">{{ paths.length }} item(s)</span>
    </header>
    <ul class="ws-path-list">
      <li v-for="path in paths" :key="path">
        <code>{{ path }}</code>
      </li>
    </ul>
    <p class="ws-muted">Create a checkpoint to save these to history.</p>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { DriftReport } from "../../types";

const props = defineProps<{ drift: DriftReport }>();

// One flat, de-duplicated list — the db/files/conflict split is an internal
// detail the user doesn't need to reason about.
const paths = computed(() =>
  [
    ...new Set([...props.drift.db_ahead, ...props.drift.files_ahead, ...props.drift.conflict]),
  ].sort(),
);
</script>
