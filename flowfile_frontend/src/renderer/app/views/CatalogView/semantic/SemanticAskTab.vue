<template>
  <div class="ask-tab">
    <slot name="question" />

    <section class="question" data-testid="ask-question">
      <template v-if="aiReady">
        <form class="question-row" @submit.prevent="submitQuestion">
          <el-input
            v-model="question"
            class="question-input"
            placeholder="Ask a question about your data"
            :disabled="asking"
            data-testid="ask-question-input"
          />
          <button
            class="btn btn-primary"
            type="submit"
            :disabled="!question.trim() || asking"
            data-testid="ask-question-submit"
          >
            <i
              :class="asking ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-wand-magic-sparkles'"
            ></i>
            Ask
          </button>
        </form>
        <p v-if="!asking && readAs" class="read-as" data-testid="ask-read-as">{{ readAs }}</p>
        <p v-if="examples.length" class="examples">
          Try:
          <a v-for="ex in examples" :key="ex" href="#" @click.prevent="useExample(ex)">{{ ex }}</a>
        </p>
        <p v-if="asking" class="q-note" role="status">Reading your approved metrics…</p>
        <template v-else-if="answer">
          <p v-if="answer.no_metric_fits" class="q-note" data-testid="ask-no-metric">
            No approved metric answers this.
            <a href="#" @click.prevent="emit('open-tab', 'metrics')"
              >Propose one in the Metrics tab.</a
            >
          </p>
          <p v-else-if="answer.degraded" class="q-note warn" data-testid="ask-degraded">
            {{ degradedText(answer.reason) }} The controls below still work.
          </p>
          <ul
            v-if="otherNotes.length"
            class="note-list"
            :class="{ warn: answer.partial }"
            data-testid="ask-notes"
          >
            <li v-for="n in otherNotes" :key="n">{{ n }}</li>
          </ul>
          <p v-if="answer.query" class="q-note">Answered by {{ answerModel }}.</p>
        </template>
      </template>
      <template v-else>
        <LocalModelSetupCard
          v-if="canOfferLocal || notSetUp"
          compact
          title="Let AI on this computer answer questions"
          description="A small model that runs here. It only picks an approved metric; the numbers come from your data."
          @installed="notSetUp = false"
          @open-settings="router.push({ name: 'ai' })"
        />
        <p class="q-note">Questions need AI. The controls below work without it.</p>
      </template>
    </section>

    <p v-if="metrics.length === 0" class="empty" data-testid="ask-empty">
      A metric has to exist before you can ask for a number.
      <a href="#" @click.prevent="emit('open-tab', 'metrics')">Add one in the Metrics tab.</a>
    </p>

    <template v-else>
      <section class="controls">
        <div class="row">
          <label class="field grow">
            <span class="label">Metric</span>
            <el-select
              v-model="metricName"
              filterable
              placeholder="Pick a metric"
              @change="onMetric"
            >
              <el-option
                v-for="m in acceptedMetrics"
                :key="m.name"
                :label="m.name"
                :value="m.name"
              />
              <template v-if="pendingMetrics.length">
                <el-option disabled value="__divider" label="Not approved yet" class="divider" />
                <el-option
                  v-for="m in pendingMetrics"
                  :key="m.name"
                  :label="m.name"
                  :value="m.name"
                />
              </template>
            </el-select>
          </label>
          <label class="field grow2">
            <span class="label">Group by</span>
            <el-select
              v-model="dimensions"
              multiple
              filterable
              collapse-tags
              collapse-tags-tooltip
              placeholder="No grouping"
              :disabled="!metric"
            >
              <el-option
                v-for="d in dimensionOptions"
                :key="d.value"
                :label="d.label"
                :value="d.value"
              />
            </el-select>
          </label>
          <label v-if="metric?.time_column" class="field">
            <span class="label">Time grain</span>
            <el-select v-model="grain" clearable placeholder="None">
              <el-option v-for="g in GRAINS" :key="g" :label="g" :value="g" />
            </el-select>
          </label>
        </div>

        <p v-if="isPending" class="pending-note">
          <i class="fa-solid fa-clock"></i> {{ metricName }} is not approved yet. The answer is a
          preview.
        </p>

        <div class="filters">
          <span class="label">Filters</span>
          <div v-for="(f, i) in filters" :key="i" class="filter-row">
            <el-select v-model="f.field" filterable class="f-field" placeholder="Field">
              <el-option
                v-for="d in dimensionOptions"
                :key="d.value"
                :label="d.label"
                :value="d.value"
              />
            </el-select>
            <el-select v-model="f.op" class="f-op">
              <el-option v-for="o in OPS" :key="o.op" :label="o.label" :value="o.op" />
            </el-select>
            <template v-if="needsValue(f.op)">
              <el-input v-model="f.value" class="f-value" :placeholder="valueHint(f.op)" />
              <template v-if="f.op === 'between'">
                <span class="and">and</span>
                <el-input v-model="f.value2" class="f-value" />
              </template>
            </template>
            <button type="button" class="icon-btn" title="Remove" @click="filters.splice(i, 1)">
              <i class="fa-solid fa-xmark"></i>
            </button>
          </div>
          <button type="button" class="btn btn-secondary btn-sm" @click="addFilter">
            <i class="fa-solid fa-plus"></i> Add filter
          </button>
        </div>

        <div class="run-row">
          <button
            class="btn btn-primary"
            :disabled="!metric || running"
            data-testid="ask-run"
            @click="runManual()"
          >
            <i :class="running ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-play'"></i> Run
          </button>
        </div>
      </section>

      <p v-if="failure" class="failure" role="alert">
        <i class="fa-solid fa-circle-exclamation"></i> {{ failure }}
      </p>

      <section v-if="result" class="result" data-testid="ask-result">
        <p v-if="answer?.partial" class="pending-note" data-testid="ask-partial">
          <i class="fa-solid fa-triangle-exclamation"></i> This answers only part of the question.
          See what was left out below.
        </p>
        <div class="provenance" :class="{ warn: resultPending }" data-testid="ask-provenance">
          <i
            :class="resultPending ? 'fa-solid fa-triangle-exclamation' : 'fa-solid fa-circle-check'"
          ></i>
          <span v-if="resultPending">
            Preview with <strong>{{ result.metric.name }}</strong> · not approved yet
          </span>
          <span v-else>
            Answered with <strong>{{ result.metric.name }}</strong>
            <template v-if="result.metric.approved_by">
              · approved by {{ result.metric.approved_by }}</template
            >
            <template v-if="result.metric.version"> · version {{ result.metric.version }}</template>
          </span>
        </div>

        <p v-if="result.error" class="failure" role="alert">
          <i class="fa-solid fa-circle-exclamation"></i> {{ result.error }}
        </p>
        <template v-else>
          <div class="grid-wrap">
            <table class="grid">
              <thead>
                <tr>
                  <th
                    v-for="c in result.columns"
                    :key="c"
                    :class="{ num: c === result.metric.name }"
                  >
                    {{ c }}
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, ri) in result.rows" :key="ri">
                  <td
                    v-for="(cell, ci) in row"
                    :key="ci"
                    :class="{ num: result.columns[ci] === result.metric.name }"
                  >
                    {{ cellText(cell, result.columns[ci]) }}
                  </td>
                </tr>
                <tr v-if="result.rows.length === 0">
                  <td :colspan="result.columns.length || 1" class="none">No rows.</td>
                </tr>
              </tbody>
            </table>
          </div>
          <p class="count">
            {{ result.total_rows.toLocaleString() }} rows
            <span v-if="result.truncated">(showing {{ result.rows.length.toLocaleString() }})</span>
          </p>
        </template>

        <div class="sql-block">
          <button type="button" class="sql-toggle" @click="showSql = !showSql">
            <i :class="showSql ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"></i> SQL
          </button>
          <button v-if="showSql" type="button" class="btn btn-secondary btn-sm" @click="copySql">
            <i class="fa-regular fa-copy"></i> Copy
          </button>
          <pre v-if="showSql" class="sql">{{ result.sql }}</pre>
        </div>

        <div class="keep">
          <template v-if="!keeping">
            <button class="btn btn-secondary btn-sm" @click="keeping = true">
              <i class="fa-solid fa-diagram-project"></i> Keep as a flow
            </button>
          </template>
          <template v-else>
            <el-input v-model="flowName" class="flow-name" placeholder="Flow name" />
            <button
              class="btn btn-primary btn-sm"
              :disabled="!flowName.trim() || saving"
              @click="keepAsFlow"
            >
              {{ saving ? "Saving…" : "Save" }}
            </button>
            <button class="btn btn-secondary btn-sm" @click="keeping = false">Cancel</button>
          </template>
          <p v-if="savedFlowId !== null" class="saved">
            <i class="fa-solid fa-circle-check"></i> Saved as flow {{ savedFlowId }}.
          </p>
          <p v-if="saveError" class="failure" role="alert">{{ saveError }}</p>
        </div>
      </section>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import LocalModelSetupCard from "../../../components/ai/LocalModelSetupCard.vue";
import { useSemanticAi } from "../../../composables/useSemanticAi";
import { SemanticModelApi } from "../../../api/semanticModel.api";
import { useCatalogStore } from "../../../stores/catalog-store";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import {
  isAiNotSetUp,
  type SemanticAskOut,
  type SemanticAskReason,
} from "../../../types/semantic.types";
import type {
  MetricFilterOp,
  MetricGrain,
  MetricQuery,
  MetricQueryResult,
  SemanticMetric,
  SemanticModel,
} from "../../../types/semantic.types";
import { errorText } from "./semanticFormat";

const props = defineProps<{ model: SemanticModel }>();
const emit = defineEmits<{ "open-tab": [tab: string] }>();

const GRAINS: MetricGrain[] = ["day", "week", "month", "quarter", "year"];
const OPS: { op: MetricFilterOp; label: string }[] = [
  { op: "=", label: "is" },
  { op: "!=", label: "is not" },
  { op: "<", label: "is less than" },
  { op: "<=", label: "is at most" },
  { op: ">", label: "is greater than" },
  { op: ">=", label: "is at least" },
  { op: "in", label: "is one of" },
  { op: "not in", label: "is none of" },
  { op: "between", label: "is between" },
  { op: "is null", label: "is empty" },
  { op: "is not null", label: "is not empty" },
];

interface FilterRow {
  field: string;
  op: MetricFilterOp;
  value: string;
  value2: string;
}
interface DimensionOption {
  value: string;
  label: string;
}

const router = useRouter();
const { usable, canOfferLocal, pick } = useSemanticAi();
const question = ref("");
const asking = ref(false);
const answer = ref<SemanticAskOut | null>(null);
const notSetUp = ref(false);
const aiReady = computed(() => usable.value && !notSetUp.value);
const answerModel = computed(() => {
  const a = answer.value;
  if (!a) return "";
  return a.model ? `${a.model} (${a.provider})` : a.provider;
});

const readAs = computed(() => answer.value?.notes.find((n) => n.startsWith("Read as:")) ?? "");
const otherNotes = computed(() => {
  const notes = answer.value?.notes ?? [];
  const i = notes.findIndex((n) => n.startsWith("Read as:"));
  return i < 0 ? notes : notes.filter((_, j) => j !== i);
});

const DEGRADED_TEXT: Record<SemanticAskReason, string> = {
  no_accepted_metrics: "There are no approved metrics yet, so there is nothing to answer with.",
  parse_error: "The model's answer could not be read.",
  unknown_metric: "The model picked a metric that does not exist.",
  invalid_query: "The model asked for something the metric cannot do.",
  query_failed: "The question was understood, but the query failed to run.",
  timeout: "The model took too long to answer.",
  provider_error: "The AI provider returned an error.",
};
const degradedText = (reason: SemanticAskReason | null) =>
  (reason && DEGRADED_TEXT[reason]) || "Could not answer this question.";

const store = useSemanticModelStore();
const catalogStore = useCatalogStore();

const metricName = ref("");
const dimensions = ref<string[]>([]);
const grain = ref<MetricGrain | null>(null);
const filters = ref<FilterRow[]>([]);
const preview = ref(false);
const running = ref(false);
const result = ref<MetricQueryResult | null>(null);
const resultPending = ref(false);
const failure = ref("");
const showSql = ref(false);
const keeping = ref(false);
const flowName = ref("");
const saving = ref(false);
const saveError = ref("");
const savedFlowId = ref<number | null>(null);
const lastQuery = ref<MetricQuery | null>(null);

const metrics = computed(() => props.model.tables.flatMap((t) => t.metrics));
const acceptedMetrics = computed(() => metrics.value.filter((m) => m.status === "accepted"));
const pendingMetrics = computed(() => metrics.value.filter((m) => m.status !== "accepted"));
const examples = computed(() => {
  const [first, second] = acceptedMetrics.value;
  const list: string[] = [];
  if (first) list.push(`What is the ${first.name.replace(/_/g, " ")}?`);
  if (first?.dimensions.length) {
    list.push(`${first.name.replace(/_/g, " ")} by ${first.dimensions[0].replace(/_/g, " ")}`);
  }
  if (second) list.push(`What is the ${second.name.replace(/_/g, " ")}?`);
  return list.slice(0, 3);
});
const metric = computed(() => metrics.value.find((m) => m.name === metricName.value) ?? null);
const isPending = computed(() => !!metric.value && metric.value.status !== "accepted");

const short = (key: string) => key.slice(key.lastIndexOf(".") + 1);

const treeColumns = computed(() => {
  const map = new Map<string, string[]>();
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      for (const t of schema.tables ?? []) {
        map.set(
          `${catalog.name}.${schema.name}.${t.name}`,
          (t.schema_columns ?? []).map((c) => c.name),
        );
      }
    }
  }
  return map;
});

function columnsOfTable(key: string): string[] {
  const t = props.model.tables.find((x) => x.key === key || x.name === key);
  if (t) return t.fields.map((f) => f.name);
  return treeColumns.value.get(key) ?? [];
}

function reachable(from: string, allowPending: boolean): string[] {
  const seen = [from];
  for (let i = 0; i < seen.length; i++) {
    for (const j of props.model.joins) {
      if (j.from_table !== seen[i] || seen.includes(j.to_table)) continue;
      if (j.status !== "accepted" && !allowPending) continue;
      if (j.status === "rejected" || j.status === "retracted" || j.status === "superseded")
        continue;
      seen.push(j.to_table);
    }
  }
  return seen;
}

const dimensionOptions = computed<DimensionOption[]>(() => {
  const m = metric.value;
  if (!m) return [];
  if (m.dimensions.length) return m.dimensions.map((d) => ({ value: d, label: d }));
  const base =
    props.model.tables.find((t) => t.key === m.table || t.name === m.table)?.key ?? m.table;
  const options: DimensionOption[] = [];
  for (const key of reachable(base, isPending.value || preview.value)) {
    for (const column of columnsOfTable(key)) {
      const label = `${short(key)}.${column}`;
      options.push({ value: key === base ? column : label, label: key === base ? column : label });
    }
  }
  return options;
});

const needsValue = (op: MetricFilterOp) => op !== "is null" && op !== "is not null";
const valueHint = (op: MetricFilterOp) => (op === "in" || op === "not in" ? "a, b, c" : "Value");

function addFilter() {
  filters.value.push({ field: "", op: "=", value: "", value2: "" });
}

function onMetric() {
  preview.value = isPending.value;
  dimensions.value = [];
  filters.value = [];
  grain.value = null;
}

const scalar = (text: string): string | number => {
  const t = text.trim();
  return t !== "" && !Number.isNaN(Number(t)) ? Number(t) : t;
};

function buildQuery(): MetricQuery {
  return {
    metric: metricName.value,
    dimensions: dimensions.value,
    filters: filters.value
      .filter((f) => f.field)
      .map((f) => {
        if (!needsValue(f.op)) return { field: f.field, op: f.op };
        if (f.op === "in" || f.op === "not in")
          return {
            field: f.field,
            op: f.op,
            value: f.value
              .split(",")
              .map((v) => v.trim())
              .filter((v) => v !== "")
              .map(scalar),
          };
        if (f.op === "between")
          return { field: f.field, op: f.op, value: [scalar(f.value), scalar(f.value2)] };
        return { field: f.field, op: f.op, value: scalar(f.value) };
      }),
    grain: metric.value?.time_column ? grain.value : null,
    order: null,
    limit: 1000,
    preview: preview.value || isPending.value,
  };
}

async function run(query?: MetricQuery) {
  const body = query ?? buildQuery();
  const id = store.namespaceId;
  if (id === null) return;
  running.value = true;
  failure.value = "";
  result.value = null;
  savedFlowId.value = null;
  saveError.value = "";
  try {
    result.value = await SemanticModelApi.queryMetric(id, body);
    lastQuery.value = body;
    resultPending.value = result.value.metric.status !== "accepted";
    if (!flowName.value) flowName.value = `${body.metric} query`;
  } catch (e) {
    failure.value = errorText(e, "Could not run the query");
  } finally {
    running.value = false;
  }
}

/** A run from the controls is the user's own query, so the last AI answer's notes no longer apply. */
function runManual() {
  answer.value = null;
  void run();
}

/** Sets every control from a query, then runs it, or shows `ready` when the answer came with one. */
async function applyQuery(query: MetricQuery, ready?: MetricQueryResult | null) {
  setControls(query);
  if (ready) showResult(query, ready);
  else await run(query);
}

function showResult(query: MetricQuery, res: MetricQueryResult) {
  failure.value = "";
  savedFlowId.value = null;
  saveError.value = "";
  result.value = res;
  lastQuery.value = query;
  resultPending.value = res.metric.status !== "accepted";
  if (!flowName.value) flowName.value = `${query.metric} query`;
}

async function submitQuestion() {
  const text = question.value.trim();
  const id = store.namespaceId;
  if (!text || id === null) return;
  asking.value = true;
  answer.value = null;
  try {
    const out = await store.askQuestion(text, pick());
    answer.value = out;
    if (out.query) await applyQuery(out.query, out.result);
  } catch (e) {
    if (isAiNotSetUp(e)) notSetUp.value = true;
    else ElMessage.error(errorText(e, "Could not answer the question"));
  } finally {
    asking.value = false;
  }
}

function useExample(text: string) {
  question.value = text;
  void submitQuestion();
}

function setControls(query: MetricQuery) {
  metricName.value = query.metric;
  dimensions.value = [...query.dimensions];
  grain.value = query.grain;
  preview.value = query.preview;
  filters.value = query.filters.map((f) => {
    const v = f.value;
    const list = Array.isArray(v) ? v : [];
    return {
      field: f.field,
      op: f.op,
      value: Array.isArray(v)
        ? f.op === "between"
          ? String(list[0] ?? "")
          : list.join(", ")
        : String(v ?? ""),
      value2: f.op === "between" ? String(list[1] ?? "") : "",
    };
  });
}

function consumePending() {
  const q = store.pendingAsk;
  if (!q) return;
  store.pendingAsk = null;
  void applyQuery(q);
}

onMounted(consumePending);
watch(() => store.pendingAsk, consumePending);

function formatNumber(value: number, format: string): string {
  const decimals = format.includes(".") ? (format.split(".")[1].match(/[0#]/g)?.length ?? 0) : 0;
  const percent = format.includes("%");
  const n = percent ? value * 100 : value;
  const body = n.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
    useGrouping: format.includes(","),
  });
  const first = format.search(/[0#]/);
  const prefix = first > 0 ? format.slice(0, first).replace(/%/g, "") : "";
  const suffix = percent ? "%" : "";
  return `${prefix}${body}${suffix}`;
}

function cellText(cell: unknown, column: string): string {
  if (cell === null || cell === undefined) return "";
  const m: SemanticMetric | null = metric.value;
  if (m?.format && column === result.value?.metric.name && typeof cell === "number") {
    return formatNumber(cell, m.format);
  }
  return typeof cell === "number" ? cell.toLocaleString() : String(cell);
}

async function copySql() {
  try {
    await navigator.clipboard.writeText(result.value?.sql ?? "");
    ElMessage.success("SQL copied");
  } catch {
    ElMessage.error("Could not copy");
  }
}

async function keepAsFlow() {
  const id = store.namespaceId;
  if (id === null || !lastQuery.value) return;
  saving.value = true;
  saveError.value = "";
  savedFlowId.value = null;
  try {
    const out = await SemanticModelApi.saveMetricQueryAsFlow(id, {
      query: lastQuery.value,
      name: flowName.value.trim(),
    });
    savedFlowId.value = out.flow_id;
    keeping.value = false;
  } catch (e) {
    saveError.value = errorText(e, "Could not save the flow");
  } finally {
    saving.value = false;
  }
}

defineExpose({ applyQuery });
</script>

<style scoped>
.ask-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.empty {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.question {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  padding: var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
}

.question-row {
  display: flex;
  gap: var(--spacing-2);
}

.question-input {
  flex: 1;
}

.examples,
.q-note {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.examples a {
  margin-left: var(--spacing-2);
}

.q-note.warn {
  color: var(--color-warning-dark);
}

.read-as {
  margin: 0;
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.note-list {
  margin: 0;
  padding: 0 0 0 var(--spacing-4);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.note-list.warn {
  padding-left: var(--spacing-3);
  border-left: 3px solid var(--color-warning-dark);
  list-style-position: inside;
  color: var(--color-warning-dark);
}

.controls {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
}

.row {
  display: flex;
  gap: var(--spacing-3);
  flex-wrap: wrap;
}

.field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  min-width: 160px;
}

.grow {
  flex: 1;
}

.grow2 {
  flex: 2;
}

.label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.pending-note {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-warning-dark);
}

.filters {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: var(--spacing-2);
}

.filter-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-wrap: wrap;
}

.f-field {
  width: 220px;
}

.f-op {
  width: 150px;
}

.f-value {
  width: 160px;
}

.and {
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.icon-btn {
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
}

.icon-btn:hover {
  color: var(--color-danger, var(--color-primary));
}

.run-row {
  display: flex;
  justify-content: flex-end;
}

.failure {
  margin: 0;
  padding: var(--spacing-3);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  color: var(--color-danger, #c0392b);
  font-size: var(--font-size-sm);
  white-space: pre-wrap;
}

.result {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.provenance {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  border-left: 4px solid var(--color-success, #2e9e5b);
  font-size: var(--font-size-base);
}

.provenance.warn {
  border-left-color: var(--color-warning-dark);
  color: var(--color-warning-dark);
}

.grid-wrap {
  max-height: 420px;
  overflow: auto;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.grid {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
}

.grid th,
.grid td {
  padding: var(--spacing-2) var(--spacing-3);
  border-bottom: 1px solid var(--color-border-light);
  text-align: left;
}

.grid th {
  position: sticky;
  top: 0;
  background: var(--color-background-secondary);
  font-weight: var(--font-weight-semibold);
}

.grid .num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.none {
  color: var(--color-text-muted);
}

.count {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.sql-block {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--spacing-2);
}

.sql-toggle {
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-size: var(--font-size-sm);
}

.sql {
  flex-basis: 100%;
  margin: 0;
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-xs);
  white-space: pre-wrap;
  word-break: break-word;
}

.keep {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--spacing-2);
}

.flow-name {
  width: 260px;
}

.saved {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-success, #2e9e5b);
}
</style>
