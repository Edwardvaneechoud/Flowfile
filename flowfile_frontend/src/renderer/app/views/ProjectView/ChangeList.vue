<template>
  <ul class="change-list">
    <li v-for="c in changes" :key="c.path" class="change" :class="`change--${c.change}`">
      <i :class="iconFor(c.change)"></i>
      <span class="change__text">
        <strong>{{ c.kind }}</strong> “{{ c.label }}”
        <span class="change__verb">{{ verbFor(c.change) }}</span>
      </span>
    </li>
  </ul>
</template>

<script setup lang="ts">
import type { ProjectVersionChange } from "../../types";

const props = withDefaults(
  defineProps<{
    changes: ProjectVersionChange[];
    mode?: "restore" | "summary";
  }>(),
  { mode: "summary" },
);

type Change = ProjectVersionChange["change"];

const iconFor = (change: Change): string =>
  ({
    removed: "fa-solid fa-circle-minus",
    added: "fa-solid fa-circle-plus",
    modified: "fa-solid fa-pen",
  })[change];

const verbFor = (change: Change): string =>
  (props.mode === "restore"
    ? { removed: "will be removed", added: "will be added", modified: "will change" }
    : { removed: "removed", added: "added", modified: "changed" })[change];
</script>

<style scoped>
.change-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
  max-height: 240px;
  overflow-y: auto;
}

.change {
  display: flex;
  align-items: baseline;
  gap: 8px;
  font-size: 13px;
  color: var(--color-text-primary, #0f172a);
}

.change i {
  font-size: 12px;
}

.change--removed i {
  color: var(--color-danger, #ef4444);
}

.change--added i {
  color: var(--color-success, #16a34a);
}

.change--modified i {
  color: var(--color-warning, #d97706);
}

.change__verb {
  color: var(--color-text-tertiary, #94a3b8);
}
</style>
