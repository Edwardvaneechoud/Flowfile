<template>
  <div class="history-tab">
    <div class="toolbar">
      <el-select
        v-model="subject"
        filterable
        placeholder="Pick a table, column, metric or join"
        @change="load"
      >
        <el-option-group v-for="group in options" :key="group.label" :label="group.label">
          <el-option v-for="o in group.items" :key="o.value" :label="o.label" :value="o.value" />
        </el-option-group>
      </el-select>
    </div>

    <p v-if="!subject" class="muted">Pick something to see how it changed and who changed it.</p>
    <p v-else-if="store.historyLoading" class="muted">Loading…</p>
    <p v-else-if="store.historyError" class="error" role="alert">{{ store.historyError }}</p>
    <p v-else-if="store.history.length === 0" class="muted">No events yet.</p>

    <ol v-else class="timeline">
      <li v-for="event in store.history" :key="event.id" class="event">
        <span class="dot"></span>
        <div class="event-body">
          <div class="event-line">
            <strong>{{ EVENT_LABELS[event.type] ?? event.type }}</strong>
            <span v-if="event.attribute" class="attr">{{ event.attribute }}</span>
            <span class="actor">by {{ event.actor }}</span>
            <time class="when">{{ shortTime(event.ts) }}</time>
          </div>
          <p v-if="event.value !== null && event.value !== undefined" class="event-value">
            {{ valueText(event.value) }}
          </p>
          <p v-if="event.comment" class="event-comment">{{ event.comment }}</p>
        </div>
      </li>
    </ol>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import type { SemanticModel } from "../../../types/semantic.types";
import { shortTime, valueText } from "./semanticFormat";

const props = defineProps<{ model: SemanticModel }>();
const store = useSemanticModelStore();
const subject = ref("");

const EVENT_LABELS: Record<string, string> = {
  proposed: "Proposed",
  accepted: "Accepted",
  rejected: "Rejected",
  superseded: "Replaced",
  retracted: "Retracted",
  staled: "Marked stale",
  stale: "Marked stale",
  reopened: "Reopened",
};

const options = computed(() => {
  const m = props.model;
  const opt = (label: string, value: string) => ({ label, value });
  const groups = [
    { label: "Tables", items: m.tables.map((t) => opt(t.name, `dataset:${t.key}`)) },
    {
      label: "Columns",
      items: m.tables.flatMap((t) =>
        t.fields.map((f) => opt(`${t.name}.${f.name}`, `field:${t.key}/${f.name}`)),
      ),
    },
    {
      label: "Metrics",
      items: m.tables.flatMap((t) => t.metrics.map((x) => opt(`${t.name}: ${x.name}`, x.key))),
    },
    { label: "Joins", items: m.joins.map((j) => opt(j.name, j.key)) },
  ];
  return groups.filter((g) => g.items.length > 0);
});

const load = () => store.loadHistory(subject.value);
</script>

<style scoped>
.history-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.toolbar .el-select {
  width: 360px;
  max-width: 100%;
}

.muted {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.error {
  margin: 0;
  color: var(--color-danger);
  font-size: var(--font-size-sm);
}

.timeline {
  margin: 0;
  padding: 0 0 0 var(--spacing-3);
  list-style: none;
  border-left: 2px solid var(--color-border-light);
}

.event {
  position: relative;
  padding: 0 0 var(--spacing-4) var(--spacing-4);
}

.dot {
  position: absolute;
  left: -8px;
  top: 5px;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background: var(--color-primary);
}

.event-line {
  display: flex;
  align-items: baseline;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
}

.attr,
.actor,
.when {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.event-value {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-sm);
  word-break: break-word;
}

.event-comment {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-xs);
  font-style: italic;
  color: var(--color-text-secondary);
}
</style>
