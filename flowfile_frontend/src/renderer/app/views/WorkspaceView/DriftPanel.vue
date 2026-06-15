<template>
  <section class="ws-card">
    <header class="ws-card-head">
      <h3 class="ws-card-title"><i class="fa-solid fa-code-compare" /> Sync status</h3>
    </header>

    <div v-if="drift.in_sync" class="ws-insync">
      <i class="fa-solid fa-circle-check" />
      <span>Everything is exported — the database and the project tree match.</span>
    </div>

    <div v-else class="ws-drift-groups">
      <div
        v-for="group in groups"
        :key="group.key"
        class="ws-drift-group"
        :class="`tone-${group.tone}`"
      >
        <div class="ws-drift-group-head">
          <i :class="group.icon" />
          <span>{{ group.title }}</span>
          <span class="ws-count">{{ group.paths.length }}</span>
        </div>
        <ul class="ws-path-list">
          <li v-for="path in group.paths" :key="path">
            <code>{{ path }}</code>
          </li>
        </ul>
      </div>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { DriftReport } from "../../types";

const props = defineProps<{ drift: DriftReport }>();

const groups = computed(() =>
  [
    {
      key: "db",
      tone: "warn",
      icon: "fa-solid fa-database",
      title: "In database, not yet exported",
      paths: props.drift.db_ahead,
    },
    {
      key: "files",
      tone: "info",
      icon: "fa-solid fa-file-lines",
      title: "In files, not in database",
      paths: props.drift.files_ahead,
    },
    {
      key: "conflict",
      tone: "danger",
      icon: "fa-solid fa-triangle-exclamation",
      title: "Conflicts — changed on both sides",
      paths: props.drift.conflict,
    },
  ].filter((group) => group.paths.length > 0),
);
</script>
