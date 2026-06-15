<template>
  <section class="ws-card">
    <header class="ws-card-head">
      <h3 class="ws-card-title"><i class="fa-solid fa-arrow-up-from-bracket" /> Last export</h3>
      <span class="ws-card-sub">{{ result.project_root }}</span>
    </header>

    <div class="ws-stats">
      <div class="ws-stat">
        <span class="ws-stat-num">{{ result.written.length }}</span>
        <span class="ws-stat-label">written</span>
      </div>
      <div class="ws-stat">
        <span class="ws-stat-num">{{ result.unchanged.length }}</span>
        <span class="ws-stat-label">unchanged</span>
      </div>
      <div class="ws-stat">
        <span class="ws-stat-num">{{ result.removed.length }}</span>
        <span class="ws-stat-label">removed</span>
      </div>
    </div>

    <div v-if="countEntries.length" class="ws-chips">
      <span v-for="[key, value] in countEntries" :key="key" class="ws-chip"
        >{{ key }}: {{ value }}</span
      >
    </div>

    <div v-if="result.warnings.length" class="ws-warnings">
      <div v-for="(warning, i) in result.warnings" :key="i" class="ws-warning-line">
        ⚠ {{ warning }}
      </div>
    </div>

    <details v-if="changedFiles.length" class="ws-files">
      <summary>{{ changedFiles.length }} changed file(s)</summary>
      <ul class="ws-path-list">
        <li
          v-for="file in changedFiles"
          :key="`${file.kind}:${file.path}`"
          :class="`ws-file-${file.kind}`"
        >
          <span class="ws-file-kind">{{ file.kind }}</span>
          <code>{{ file.path }}</code>
        </li>
      </ul>
    </details>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { WorkspaceExportResult } from "../../types";

const props = defineProps<{ result: WorkspaceExportResult }>();

const countEntries = computed(() => Object.entries(props.result.counts));

const changedFiles = computed(() => [
  ...props.result.written.map((path) => ({ path, kind: "written" })),
  ...props.result.removed.map((path) => ({ path, kind: "removed" })),
]);
</script>
