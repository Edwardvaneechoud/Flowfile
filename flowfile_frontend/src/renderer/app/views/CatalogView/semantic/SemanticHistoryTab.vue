<template>
  <div class="stack">
    <div class="picker">
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

    <p v-if="!subject" class="hint">Pick something to see how it changed and who changed it.</p>
    <p v-else-if="loading" class="hint">Loading…</p>
    <p v-else-if="error" class="error" role="alert">{{ error }}</p>
    <p v-else-if="events.length === 0" class="hint">No events yet.</p>

    <ol v-else class="timeline">
      <li v-for="event in events" :key="event.id" class="event">
        <span class="dot"></span>
        <div class="event-body">
          <div class="event-line">
            <strong>{{ EVENT_LABELS[event.type] ?? event.type }}</strong>
            <span v-if="event.attribute" class="attr">{{ event.attribute }}</span>
            <span class="actor">by {{ event.actor }}</span>
            <time class="when">{{ new Date(event.ts).toLocaleString() }}</time>
          </div>
          <p v-if="event.value !== null && event.value !== undefined" class="event-value">
            {{ valueText(event.value) }}
          </p>
          <p v-if="event.comment" class="note event-comment">{{ event.comment }}</p>
        </div>
      </li>
    </ol>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { SemanticModelApi } from "../../../api/semanticModel.api";
import { errorText, type SemanticEvent, type SemanticModel } from "../../../types/semantic.types";
import { valueText } from "./semanticFormat";

const props = defineProps<{ model: SemanticModel }>();
const subject = ref("");
const events = ref<SemanticEvent[]>([]);
const loading = ref(false);
const error = ref("");

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

async function load() {
  const picked = subject.value;
  loading.value = true;
  error.value = "";
  events.value = [];
  try {
    const found = await SemanticModelApi.getHistory(props.model.namespace_id, picked);
    if (picked === subject.value) events.value = found;
  } catch (e) {
    if (picked === subject.value) error.value = errorText(e, "Could not load history");
  } finally {
    if (picked === subject.value) loading.value = false;
  }
}
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.picker .el-select {
  width: 360px;
  max-width: 100%;
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
  margin-top: var(--spacing-1);
  font-style: italic;
}
</style>
