<template>
  <div class="stack">
    <SemanticSuggestMore
      :kinds="['metric']"
      label="Suggest metrics"
      @review="emit('open-tab', 'review')"
    >
      <template #hint>
        <p class="hint">
          Metrics are named calculations. Ask for one by name and everyone gets the same number.
        </p>
      </template>
      <button
        class="btn btn-primary btn-sm"
        :disabled="model.tables.length === 0"
        @click="openEditor(null)"
      >
        <i class="fa-solid fa-plus"></i> Add metric
      </button>
    </SemanticSuggestMore>

    <p v-if="tables.length === 0" class="hint">No metrics yet.</p>

    <section v-for="table in tables" :key="table.table_id">
      <h3 class="group-title">
        <i class="fa-solid fa-table"></i> {{ table.name }}
        <el-tooltip content="Open table" placement="top" :show-after="300">
          <button
            class="open-table-btn"
            type="button"
            aria-label="Open table"
            @click="router.push(tablePageLocation(table.table_id))"
          >
            <i class="fa-solid fa-arrow-up-right-from-square"></i>
          </button>
        </el-tooltip>
      </h3>
      <div class="cards">
        <article v-for="metric in table.metrics" :key="metric.key" class="box">
          <header class="line">
            <span class="name">{{ metric.name }}</span>
            <SemanticStatusChip :status="metric.status" />
            <span v-if="metric.version" class="version">v{{ metric.version }}</span>
            <button
              class="icon-btn edit-btn"
              type="button"
              title="Edit metric"
              @click="openEditor(metric)"
            >
              <i class="fa-solid fa-pen"></i>
            </button>
            <button
              v-if="metric.can_remove"
              class="icon-btn icon-btn-danger"
              type="button"
              title="Remove metric"
              aria-label="Remove metric"
              data-testid="semantic-metric-card-remove"
              :disabled="removing !== null"
              @click="removeMetric(metric)"
            >
              <i
                :class="
                  removing === metric.key ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-trash'
                "
              ></i>
            </button>
          </header>
          <p v-if="metric.description" class="hint metric-desc">{{ metric.description }}</p>
          <pre class="code expression">{{ metric.expression }}</pre>
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
            <dt>By</dt>
            <dd>{{ metric.dimensions.length ? metric.dimensions.join(", ") : "any column" }}</dd>
            <template v-if="metric.approved_by">
              <dt>Approved by</dt>
              <dd>{{ metric.approved_by }}</dd>
            </template>
          </dl>
          <p v-if="metric.pending_group" class="note warn pending-note">
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
import SemanticSuggestMore from "./SemanticSuggestMore.vue";
import { tablePageLocation } from "./semanticFormat";
import { useSemanticRemoval } from "./semanticRemoval";

const props = defineProps<{ model: SemanticModel }>();
const emit = defineEmits<{ "open-tab": [tab: string] }>();
const router = useRouter();

const { removing, removeMetric } = useSemanticRemoval();
const showEditor = ref(false);
const editing = ref<SemanticMetric | null>(null);

const tables = computed(() => props.model.tables.filter((t) => t.metrics.length > 0));

function openEditor(metric: SemanticMetric | null) {
  editing.value = metric;
  showEditor.value = true;
}
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.open-table-btn {
  width: 22px;
  height: 22px;
  margin-left: var(--spacing-1);
}

.cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: var(--spacing-3);
}

.version {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.edit-btn {
  margin-left: auto;
}

.metric-desc {
  margin-top: var(--spacing-1);
}

.expression {
  margin: var(--spacing-2) 0;
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
  margin-top: var(--spacing-2);
}
</style>
