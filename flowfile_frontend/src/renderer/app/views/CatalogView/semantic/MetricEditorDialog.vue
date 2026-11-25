<template>
  <el-dialog
    :model-value="modelValue"
    :title="metric ? `Edit ${metric.name}` : 'Add a metric'"
    width="620px"
    append-to-body
    @update:model-value="emit('update:modelValue', $event)"
    @open="reset"
  >
    <p class="hint intro">
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
      <el-form-item label="Also called (optional)">
        <el-input
          v-model="synonyms"
          placeholder="Other words for it, separated by commas"
          data-testid="metric-synonyms"
        />
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
        <p v-if="usableLine" class="note usable">{{ usableLine }}</p>
      </el-form-item>
      <el-form-item label="Filter (optional)" :error="filterError">
        <div class="editor">
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
          placeholder="Any column"
        >
          <el-option v-for="c in dimensionChoices" :key="c" :label="c" :value="c" />
        </el-select>
        <p class="note usable">
          Leave empty to let this metric be grouped by any column it can reach.
        </p>
      </el-form-item>
    </el-form>

    <p v-if="error" class="error" role="alert">{{ error }}</p>

    <template #footer>
      <div class="line">
        <button
          v-if="metric && metric.can_remove"
          class="btn btn-ghost btn-danger"
          data-testid="semantic-metric-remove"
          :disabled="saving || removing !== null"
          @click="remove"
        >
          <i class="fa-solid fa-trash"></i>
          {{ removing ? "Removing…" : "Remove metric" }}
        </button>
        <span class="grow"></span>
        <button class="btn btn-secondary" @click="emit('update:modelValue', false)">Cancel</button>
        <button
          class="btn btn-primary"
          :disabled="!valid || saving || removing !== null"
          @click="save"
        >
          {{ saving ? "Saving…" : metric ? "Save changes" : "Add metric" }}
        </button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { Codemirror } from "vue-codemirror";
import type { Completion } from "@codemirror/autocomplete";
import type { SQLNamespace } from "@codemirror/lang-sql";
import { EditorState, type Extension } from "@codemirror/state";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import { errorText, type SemanticMetric, type SemanticModel } from "../../../types/semantic.types";
import { sqlEditorExtensions, sqlFunctionCompletions } from "../sqlCompletion";
import { reachLine, reachableTables, tableColumns } from "./metricReach";
import { notePending, shortName, splitWords } from "./semanticFormat";
import { useSemanticRemoval } from "./semanticRemoval";

const props = defineProps<{
  modelValue: boolean;
  model: SemanticModel;
  metric?: SemanticMetric | null;
  defaultTableId?: number | null;
}>();
const emit = defineEmits<(e: "update:modelValue", value: boolean) => void>();

const store = useSemanticModelStore();

const tableId = ref<number | null>(null);
const name = ref("");
const description = ref("");
const expression = ref("");
const filter = ref("");
const timeColumn = ref("");
const format = ref("");
const synonyms = ref("");
const dimensions = ref<string[]>([]);
const saving = ref(false);
const error = ref("");
const expressionError = ref("");
const filterError = ref("");
const { removing, removeMetric } = useSemanticRemoval((message) => (error.value = message));

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

const columnsOf = (key: string): Completion[] =>
  tableColumns(props.model, key).map((c) => ({
    label: c.name,
    type: "property",
    detail: [c.dtype, c.meaning].filter((x) => typeof x === "string" && x).join(" · ") || undefined,
  }));

// Two reachable tables can share a short name; a column of either then needs its schema.
const isAmbiguous = (key: string) =>
  reached.value.filter((k) => shortName(k) === shortName(key)).length > 1;

/** The base table's columns, then `table.column` for each joined table whose short name is its own. */
const dimensionChoices = computed(() => {
  const joined = reached.value.slice(1).filter((key) => !isAmbiguous(key));
  return [
    ...new Set([
      ...columns.value,
      ...joined.flatMap((key) => columnsOf(key).map((c) => `${shortName(key)}.${c.label}`)),
      ...dimensions.value,
    ]),
  ];
});

/** Each reachable table by its short name, or under its schema when two share one. */
const completionSchema = computed<SQLNamespace>(() => {
  const schema: Record<string, SQLNamespace> = {};
  for (const key of reached.value) {
    const short = shortName(key);
    const parent = isAmbiguous(key) ? (key.split(".").slice(-2, -1)[0] ?? short) : null;
    if (parent === null) schema[short] = columnsOf(key);
    else schema[parent] = { ...(schema[parent] as object), [short]: columnsOf(key) };
  }
  return schema;
});

const expressionExtensions = computed<Extension[]>(() => [
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

const filterExtensions = computed<Extension[]>(() => [...expressionExtensions.value, oneLine]);

const usableLine = computed(() => reachLine(reached.value));

const valid = computed(
  () => tableId.value !== null && name.value.trim() !== "" && expression.value.trim() !== "",
);

function clearErrors() {
  error.value = expressionError.value = filterError.value = "";
}

watch(expression, () => (expressionError.value = ""));
watch(filter, () => (filterError.value = ""));
watch(tableId, () => (expressionError.value = filterError.value = ""));

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
  synonyms.value = (m?.synonyms ?? []).join(", ");
  dimensions.value = [...(m?.dimensions ?? [])];
  clearErrors();
}

/** Puts a refusal under the field it names ("… in the filter)"), else in the dialog's error line. */
function showRefusal(message: string) {
  if (message.includes(" in the filter)")) filterError.value = message;
  else if (message.includes(" in the expression)")) expressionError.value = message;
  else error.value = message;
}

async function remove() {
  error.value = "";
  if (props.metric && (await removeMetric(props.metric))) emit("update:modelValue", false);
}

async function save() {
  if (!valid.value || tableId.value === null) return;
  saving.value = true;
  clearErrors();
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
        synonyms: splitWords(synonyms.value),
        edit: !!props.metric,
      },
    ]);
    notePending(facts);
    emit("update:modelValue", false);
  } catch (e) {
    showRefusal(errorText(e, "Could not save the metric"));
  } finally {
    saving.value = false;
  }
}
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.intro {
  margin-bottom: var(--spacing-4);
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
  margin-top: var(--spacing-1);
  line-height: 1.4;
  color: var(--color-text-muted);
}

/* A refusal is a sentence or two, so it takes its own room instead of overlaying the next field. */
:deep(.el-form-item__error) {
  position: static;
  padding-top: var(--spacing-1);
  line-height: 1.4;
}
</style>
