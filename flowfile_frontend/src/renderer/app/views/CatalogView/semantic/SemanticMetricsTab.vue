<template>
  <div class="metrics-tab">
    <div class="toolbar">
      <p class="hint">
        Metrics are named calculations. Ask for one by name and everyone gets the same number.
      </p>
      <button
        class="btn btn-primary btn-sm"
        :disabled="model.tables.length === 0"
        @click="openEditor(null)"
      >
        <i class="fa-solid fa-plus"></i> Add metric
      </button>
    </div>

    <p v-if="groups.length === 0" class="empty">No metrics yet.</p>

    <section v-for="group in groups" :key="group.table.table_id" class="metric-group">
      <h3 class="group-title">
        <i class="fa-solid fa-table"></i> {{ group.table.name }}
        <el-tooltip content="Open table" placement="top" :show-after="300">
          <button
            class="open-table-btn"
            type="button"
            aria-label="Open table"
            @click="router.push(tablePageLocation(group.table.table_id))"
          >
            <i class="fa-solid fa-arrow-up-right-from-square"></i>
          </button>
        </el-tooltip>
      </h3>
      <div class="cards">
        <article v-for="metric in group.metrics" :key="metric.key" class="metric-card">
          <header class="card-head">
            <span class="metric-name">{{ metric.name }}</span>
            <SemanticStatusChip :status="metric.status" />
            <span v-if="metric.version" class="version">v{{ metric.version }}</span>
            <button class="icon-btn" type="button" title="Edit metric" @click="openEditor(metric)">
              <i class="fa-solid fa-pen"></i>
            </button>
          </header>
          <p v-if="metric.description" class="metric-desc">{{ metric.description }}</p>
          <pre class="expression">{{ metric.expression }}</pre>
          <dl class="meta">
            <template v-if="metric.filter">
              <dt>Filter</dt>
              <dd>
                <code>{{ metric.filter }}</code>
              </dd>
            </template>
            <template v-if="metric.time_column">
              <dt>Time</dt>
              <dd>{{ metric.time_column }}</dd>
            </template>
            <template v-if="metric.dimensions.length">
              <dt>By</dt>
              <dd>{{ metric.dimensions.join(", ") }}</dd>
            </template>
            <template v-if="metric.approved_by">
              <dt>Approved by</dt>
              <dd>{{ metric.approved_by }}</dd>
            </template>
          </dl>
          <p v-if="metric.pending_group" class="pending-note">
            <i class="fa-solid fa-clock"></i> A change is waiting in the Review tab.
          </p>
        </article>
      </div>
    </section>

    <MetricEditorDialog
      v-model="showEditor"
      :model="model"
      :metric="editing"
      :default-table-id="editing?.table_id ?? null"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useRouter } from "vue-router";
import type { SemanticMetric, SemanticModel } from "../../../types/semantic.types";
import MetricEditorDialog from "./MetricEditorDialog.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import { tablePageLocation } from "./semanticFormat";

const props = defineProps<{ model: SemanticModel }>();
const router = useRouter();

const showEditor = ref(false);
const editing = ref<SemanticMetric | null>(null);

const groups = computed(() =>
  props.model.tables
    .filter((t) => t.metrics.length > 0)
    .map((t) => ({ table: t, metrics: t.metrics })),
);

function openEditor(metric: SemanticMetric | null) {
  editing.value = metric;
  showEditor.value = true;
}
</script>

<style scoped>
.metrics-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.hint,
.empty {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.open-table-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  margin-left: var(--spacing-1);
  border: none;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  font-size: var(--font-size-xs);
  vertical-align: middle;
}

.open-table-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-primary);
}

.group-title {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: var(--spacing-3);
}

.metric-card {
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
}

.card-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.metric-name {
  font-weight: var(--font-weight-semibold);
}

.version {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.icon-btn {
  margin-left: auto;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
}

.icon-btn:hover {
  color: var(--color-primary);
}

.metric-desc {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.expression {
  margin: var(--spacing-2) 0;
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-xs);
  white-space: pre-wrap;
  word-break: break-word;
}

.meta {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 2px var(--spacing-3);
  margin: 0;
  font-size: var(--font-size-xs);
}

.meta dt {
  color: var(--color-text-muted);
}

.meta dd {
  margin: 0;
}

.pending-note {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-warning-dark);
}
</style>
