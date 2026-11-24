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
      <el-form-item label="Expression" :error="expressionError">
        <div class="editor">
          <codemirror
            v-model="expression"
            placeholder="SUM(amount)"
            :extensions="expressionExtensions"
            :indent-with-tab="false"
            :tab-size="2"
            :style="{ minHeight: '60px' }"
          />
        </div>
        <p v-if="usableLine" class="usable">{{ usableLine }}</p>
      </el-form-item>
      <el-form-item label="Filter (optional)" :error="filterError">
        <div class="editor one-line">
          <codemirror
            v-model="filter"
            placeholder="status = 'completed'"
            :extensions="filterExtensions"
            :indent-with-tab="false"
            :tab-size="2"
          />
        </div>
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
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { Codemirror } from "vue-codemirror";
import type { Completion } from "@codemirror/autocomplete";
import type { SQLNamespace } from "@codemirror/lang-sql";
import { EditorState, type Extension } from "@codemirror/state";
import { useCatalogStore } from "../../../stores/catalog-store";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import type { SemanticMetric, SemanticModel } from "../../../types/semantic.types";
import { sqlEditorExtensions, sqlFunctionCompletions } from "../sqlCompletion";
import { reachLine, reachableTables } from "./metricReach";
import { errorText } from "./semanticFormat";

const props = defineProps<{
  modelValue: boolean;
  model: SemanticModel;
  metric?: SemanticMetric | null;
  defaultTableId?: number | null;
}>();
const emit = defineEmits<(e: "update:modelValue", value: boolean) => void>();

const store = useSemanticModelStore();
const catalogStore = useCatalogStore();

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
const expressionError = ref("");
const filterError = ref("");

const baseTable = computed(() => props.model.tables.find((t) => t.table_id === tableId.value));

const columns = computed(() => baseTable.value?.fields.map((f) => f.name) ?? []);

const editingPending = computed(
  () => !!props.metric && (props.metric.status !== "accepted" || !!props.metric.pending_group),
);

const reached = computed(() =>
  baseTable.value
    ? reachableTables(baseTable.value.key, props.model.joins, editingPending.value)
    : [],
);

const treeColumns = computed(() => {
  const map = new Map<string, Completion[]>();
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      for (const t of schema.tables ?? []) {
        const key = t.qualified_name ?? `${catalog.name}.${schema.name}.${t.name}`;
        map.set(
          key,
          (t.schema_columns ?? []).map((c) => ({
            label: c.name,
            type: "property",
            detail: c.dtype,
          })),
        );
      }
    }
  }
  return map;
});

function columnsOf(key: string): Completion[] {
  const t = props.model.tables.find((x) => x.key === key);
  if (!t) return treeColumns.value.get(key) ?? [];
  return t.fields.map((f) => {
    const meaning = f.facts.description?.value;
    const detail = [f.dtype, typeof meaning === "string" ? meaning : ""]
      .filter(Boolean)
      .join(" · ");
    return { label: f.name, type: "property", detail: detail || undefined };
  });
}

const shortName = (key: string) => key.slice(key.lastIndexOf(".") + 1);

/** Each reachable table by its short name, or under its schema when two share one. */
const completionSchema = computed<SQLNamespace>(() => {
  const schema: Record<string, SQLNamespace> = {};
  const counts = new Map<string, number>();
  for (const key of reached.value)
    counts.set(shortName(key), (counts.get(shortName(key)) ?? 0) + 1);
  for (const key of reached.value) {
    const short = shortName(key);
    if ((counts.get(short) ?? 0) === 1) {
      schema[short] = columnsOf(key);
      continue;
    }
    const parts = key.split(".");
    const schemaName = parts[parts.length - 2] ?? short;
    const holder = (schema[schemaName] ?? {}) as Record<string, SQLNamespace>;
    holder[short] = columnsOf(key);
    schema[schemaName] = holder;
  }
  return schema;
});

const languageExtensions = computed<Extension[]>(() => [
  ...sqlEditorExtensions(
    completionSchema.value,
    baseTable.value ? shortName(baseTable.value.key) : undefined,
  ),
  sqlFunctionCompletions,
]);

const oneLine = EditorState.transactionFilter.of((tr) => {
  if (!tr.docChanged || tr.newDoc.lines === 1) return tr;
  const text = tr.newDoc.toString().replace(/\s*\n\s*/g, " ");
  return { changes: { from: 0, to: tr.startState.doc.length, insert: text } };
});

const expressionExtensions = computed<Extension[]>(() => languageExtensions.value);
const filterExtensions = computed<Extension[]>(() => [...languageExtensions.value, oneLine]);

const usableLine = computed(() => reachLine(reached.value));

const valid = computed(
  () => tableId.value !== null && name.value.trim() !== "" && expression.value.trim() !== "",
);

watch(expression, () => (expressionError.value = ""));
watch(filter, () => (filterError.value = ""));

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
  expressionError.value = "";
  filterError.value = "";
}

/** Puts a refusal under the field it names ("… in the filter)"), else in the dialog's error line. */
function showRefusal(message: string) {
  if (message.includes(" in the filter)")) filterError.value = message;
  else if (message.includes(" in the expression)")) expressionError.value = message;
  else error.value = message;
}

async function save() {
  if (!valid.value || tableId.value === null) return;
  saving.value = true;
  error.value = "";
  expressionError.value = "";
  filterError.value = "";
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
    showRefusal(errorText(e, "Could not save the metric"));
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

.usable {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-xs);
  line-height: 1.4;
  color: var(--color-text-muted);
}

.form-error {
  margin: 0;
  color: var(--color-danger);
  font-size: var(--font-size-sm);
}
</style>
