<template>
  <el-dialog
    :model-value="modelValue"
    :title="metric ? `Edit ${metric.name}` : 'Add a metric'"
    width="620px"
    append-to-body
    @update:model-value="emit('update:modelValue', $event)"
    @open="reset"
  >
    <p class="intro">
      A metric is a named calculation, such as total revenue, that everyone computes the same way.
    </p>
    <el-form label-position="top" @submit.prevent>
      <div class="row">
        <el-form-item label="Table">
          <el-select v-model="tableId" filterable :disabled="!!metric" @change="onTableChange">
            <el-option
              v-for="t in model.tables"
              :key="t.table_id"
              :label="t.name"
              :value="t.table_id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="Name">
          <el-input v-model="name" :disabled="!!metric" placeholder="total_revenue" />
        </el-form-item>
      </div>
      <el-form-item label="What it means">
        <el-input v-model="description" placeholder="Revenue from completed orders, before tax" />
      </el-form-item>
      <el-form-item label="Expression">
        <div class="editor">
          <codemirror
            v-model="expression"
            placeholder="SUM(amount)"
            :extensions="extensions"
            :indent-with-tab="false"
            :tab-size="2"
            :style="{ minHeight: '60px' }"
          />
        </div>
      </el-form-item>
      <el-form-item label="Filter (optional)">
        <el-input v-model="filter" placeholder="status = 'completed'" />
      </el-form-item>
      <div class="row">
        <el-form-item label="Time column (optional)">
          <el-select v-model="timeColumn" clearable filterable placeholder="None">
            <el-option v-for="c in columns" :key="c" :label="c" :value="c" />
          </el-select>
        </el-form-item>
        <el-form-item label="Number format (optional)">
          <el-input v-model="format" placeholder="#,##0.00" />
        </el-form-item>
      </div>
      <el-form-item label="Allowed dimensions">
        <el-select
          v-model="dimensions"
          multiple
          filterable
          collapse-tags-tooltip
          placeholder="Columns this metric can be broken down by"
        >
          <el-option v-for="c in columns" :key="c" :label="c" :value="c" />
        </el-select>
      </el-form-item>
    </el-form>

    <p v-if="error" class="form-error" role="alert">{{ error }}</p>

    <template #footer>
      <button class="btn btn-secondary" @click="emit('update:modelValue', false)">Cancel</button>
      <button class="btn btn-primary" :disabled="!valid || saving" @click="save">
        {{ saving ? "Saving…" : metric ? "Save changes" : "Add metric" }}
      </button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import { Codemirror } from "vue-codemirror";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import type { Extension } from "@codemirror/state";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import type { SemanticMetric, SemanticModel } from "../../../types/semantic.types";
import { errorText } from "./semanticFormat";

const props = defineProps<{
  modelValue: boolean;
  model: SemanticModel;
  metric?: SemanticMetric | null;
  defaultTableId?: number | null;
}>();
const emit = defineEmits<(e: "update:modelValue", value: boolean) => void>();

const store = useSemanticModelStore();
const extensions: Extension[] = [sql({ upperCaseKeywords: true }), oneDark];

const tableId = ref<number | null>(null);
const name = ref("");
const description = ref("");
const expression = ref("");
const filter = ref("");
const timeColumn = ref("");
const format = ref("");
const dimensions = ref<string[]>([]);
const saving = ref(false);
const error = ref("");

const columns = computed(
  () =>
    props.model.tables.find((t) => t.table_id === tableId.value)?.fields.map((f) => f.name) ?? [],
);

const valid = computed(
  () => tableId.value !== null && name.value.trim() !== "" && expression.value.trim() !== "",
);

function onTableChange() {
  dimensions.value = [];
  timeColumn.value = "";
}

function reset() {
  const m = props.metric;
  tableId.value = m?.table_id ?? props.defaultTableId ?? props.model.tables[0]?.table_id ?? null;
  name.value = m?.name ?? "";
  description.value = m?.description ?? "";
  expression.value = m?.expression ?? "";
  filter.value = m?.filter ?? "";
  timeColumn.value = m?.time_column ?? "";
  format.value = m?.format ?? "";
  dimensions.value = [...(m?.dimensions ?? [])];
  error.value = "";
}

async function save() {
  if (!valid.value || tableId.value === null) return;
  saving.value = true;
  error.value = "";
  try {
    const facts = await store.propose([
      {
        kind: "metric",
        table_id: tableId.value,
        name: name.value.trim(),
        expression: expression.value.trim(),
        description: description.value.trim() || null,
        filter: filter.value.trim() || null,
        time_column: timeColumn.value || null,
        dimensions: dimensions.value.length ? dimensions.value : null,
        format: format.value.trim() || null,
        edit: !!props.metric,
      },
    ]);
    if (facts.some((f) => f.status !== "accepted")) {
      ElMessage.info("Proposed. It waits in the Review tab.");
    }
    emit("update:modelValue", false);
  } catch (e) {
    error.value = errorText(e, "Could not save the metric");
  } finally {
    saving.value = false;
  }
}
</script>

<style scoped>
.intro {
  margin: 0 0 var(--spacing-4);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-3);
}

.editor {
  width: 100%;
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.form-error {
  margin: 0;
  color: var(--color-danger);
  font-size: var(--font-size-sm);
}
</style>
