<template>
  <el-dialog
    :model-value="modelValue"
    title="Build a page with AI"
    width="640px"
    append-to-body
    :close-on-click-modal="false"
    :close-on-press-escape="!running"
    :before-close="onBeforeClose"
  >
    <div class="dash-ai" data-testid="dashboard-ai-builder">
      <template v-if="!result">
        <label class="dash-ai__label" for="dash-ai-sources">Tables and saved queries</label>
        <el-select
          id="dash-ai-sources"
          v-model="tableIds"
          multiple
          filterable
          :multiple-limit="MAX_SOURCES"
          :disabled="running"
          :placeholder="sources.length ? 'Select what the page reads' : 'No catalog tables yet'"
          class="dash-ai__sources"
          data-testid="dashboard-ai-sources"
        >
          <el-option-group v-for="group in sourceGroups" :key="group.label" :label="group.label">
            <el-option
              v-for="table in group.tables"
              :key="table.id"
              :label="table.name"
              :value="table.id"
            >
              <span>{{ table.name }}</span>
              <span v-if="table.full_table_name" class="dash-ai__option-path">
                {{ table.full_table_name }}
              </span>
            </el-option>
          </el-option-group>
        </el-select>
        <p class="dash-ai__hint">
          Up to {{ MAX_SOURCES }}. Each chart reads one of them; to combine two, save a query that
          joins them and select it.
        </p>

        <label class="dash-ai__label" for="dash-ai-question">Question</label>
        <textarea
          id="dash-ai-question"
          v-model="prompt"
          class="dash-ai__question"
          rows="3"
          :maxlength="MAX_PROMPT"
          :disabled="running"
          placeholder="What should this page answer? For example: how are sales doing, and what drives them?"
          data-testid="dashboard-ai-question"
          @keydown="onKeydown"
        ></textarea>
        <p class="dash-ai__hint" data-testid="dashboard-ai-data-line">
          {{ aiStore.sendsData ? SENDS_LINE : NAMES_ONLY_LINE }}
          {{ aiStore.sendsData ? READS_LINE : NO_READS_LINE }}
        </p>
        <p class="dash-ai__hint" data-testid="dashboard-ai-models">
          {{ modelsLine }} Change them under Settings → AI.
        </p>
        <p v-if="usesLocal" class="dash-ai__hint is-warning">
          The on-device model is small: expect a simpler page, and some charts may fail to build.
        </p>

        <ol v-if="running" class="dash-ai__steps" data-testid="dashboard-ai-progress">
          <li
            v-for="step in steps"
            :key="step.key"
            class="dash-ai__step"
            :class="`is-${step.state}`"
            :data-state="step.state"
          >
            <span class="material-icons dash-ai__step-icon" aria-hidden="true">{{
              stepIcon(step.state)
            }}</span>
            <span>{{ step.label }}</span>
            <ul v-if="step.key === 'building' && store.checks.length" class="dash-ai__checks">
              <li
                v-for="item in store.checks"
                :key="item.id"
                class="dash-ai__check"
                :class="`is-${item.status}`"
                :data-status="item.status"
              >
                <span class="material-icons dash-ai__step-icon" aria-hidden="true">{{
                  checkIcon(item.status)
                }}</span>
                <span>{{ item.title }}</span>
                <span v-if="item.message" class="dash-ai__detail">{{ item.message }}</span>
              </li>
            </ul>
          </li>
        </ol>

        <div
          v-if="message"
          class="dash-ai__message"
          :class="`is-${message.tone}`"
          role="status"
          data-testid="dashboard-ai-message"
        >
          <span>{{ message.text }}</span>
          <span v-if="message.detail" class="dash-ai__detail">{{ message.detail }}</span>
        </div>

        <div class="dash-ai__actions">
          <el-button v-if="running" data-testid="dashboard-ai-cancel" @click="onCancel">
            Cancel
          </el-button>
          <el-button v-else data-testid="dashboard-ai-close" @click="close">Close</el-button>
          <el-button
            type="primary"
            :disabled="!canBuild"
            :loading="running"
            data-testid="dashboard-ai-build"
            @click="onBuild"
          >
            Build page
          </el-button>
        </div>
      </template>

      <template v-else>
        <div class="dash-ai__result" data-testid="dashboard-ai-result">
          <p class="dash-ai__headline">{{ headline }}</p>
          <p v-if="!result.page.grounded" class="dash-ai__hint">
            The text was written without numbers read from the data, so it describes the charts and
            draws no conclusions.
          </p>
          <p v-if="result.placed.filters.length" data-testid="dashboard-ai-filters">
            {{ result.placed.filters.length === 1 ? "Filter" : "Filters" }} in the strip above the
            page: {{ listPhrases(result.placed.filters) }}.
          </p>
          <section v-if="result.page.relations.length" data-testid="dashboard-ai-relations">
            <h4 class="dash-ai__section">Relations tested</h4>
            <ul class="dash-ai__notes">
              <li v-for="(note, i) in result.page.relations" :key="i">
                <strong>{{ note.title }}</strong
                >: {{ note.reason }}
              </li>
            </ul>
          </section>
          <section v-if="result.page.leftOut.length" data-testid="dashboard-ai-left-out">
            <h4 class="dash-ai__section">Built, then left off the page</h4>
            <ul class="dash-ai__notes">
              <li v-for="(note, i) in result.page.leftOut" :key="i">
                <strong>{{ note.title }}</strong
                >: {{ note.reason }}
              </li>
            </ul>
          </section>
          <section v-if="notBuilt.length" data-testid="dashboard-ai-failed">
            <h4 class="dash-ai__section">Could not be built</h4>
            <ul class="dash-ai__notes">
              <li v-for="(note, i) in notBuilt" :key="i">
                <strong>{{ note.title }}</strong
                >: {{ note.reason }}
              </li>
            </ul>
          </section>
          <section v-if="result.page.warnings.length" data-testid="dashboard-ai-warnings">
            <h4 class="dash-ai__section">Notes</h4>
            <ul class="dash-ai__notes">
              <li v-for="(warning, i) in result.page.warnings" :key="i">{{ warning }}</li>
            </ul>
          </section>
          <p class="dash-ai__hint">
            Undo takes the whole page back. The charts stay in the chart library either way.
          </p>
        </div>
        <div class="dash-ai__actions">
          <el-button type="primary" data-testid="dashboard-ai-done" @click="close">Done</el-button>
        </div>
      </template>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";

import { useAiDashboardStore, type CheckStatus } from "../../stores/ai-dashboard-store";
import { useAiStore } from "../../stores/ai-store";
import { useCatalogStore } from "../../stores/catalog-store";
import type { CatalogTable } from "../../types";
import {
  AI_OFF_MESSAGE,
  NAMES_ONLY_LINE,
  SENDS_LINE,
  listPhrases,
  type ChartPromptMessage,
} from "../CatalogView/chartPrompt";
import { isHubRow } from "../CatalogView/hubRows";
import { BUILD_CANCELLED_MESSAGE, type BuiltPage, type PageNote } from "./aiDashboard";
import { createChartConverter } from "./aiDashboardCharts";

/** What the editor did with a built page. */
export interface PlacedPage {
  charts: number;
  kpis: number;
  /** The names of the filters that were added to the dashboard's filter strip. */
  filters: string[];
  /** Kept charts that could not be saved, so are not on the page. */
  failed: PageNote[];
}

const props = defineProps<{
  modelValue: boolean;
  /** Saves the page's charts and puts its tiles on the dashboard; rejects when nothing was placed. */
  place: (page: BuiltPage) => Promise<PlacedPage>;
  /** Tables selected when the dialog opens. */
  initialTableIds?: number[];
}>();
const emit = defineEmits<{ (e: "update:modelValue", open: boolean): void }>();

const MAX_SOURCES = 6;
const MAX_PROMPT = 2000;
const READS_LINE = "It also reads what each chart returns, to write what the page says.";
const NO_READS_LINE = "The page's text then describes the charts without stating what they show.";
const LOCAL_PROVIDER = "local";

type StepKey = "planning" | "building" | "writing" | "placing";
type StepState = "done" | "active" | "todo";

const store = useAiDashboardStore();
const aiStore = useAiStore();
const catalogStore = useCatalogStore();

const tableIds = ref<number[]>([...(props.initialTableIds ?? [])]);
const prompt = ref("");
const message = ref<ChartPromptMessage | null>(null);
const placing = ref(false);
const result = ref<{ page: BuiltPage; placed: PlacedPage } | null>(null);

const sources = computed<CatalogTable[]>(() => catalogStore.allTables.filter((t) => !isHubRow(t)));
const isSavedQuery = (t: CatalogTable) => t.table_type === "virtual" && Boolean(t.sql_query);
const sourceGroups = computed(() =>
  [
    { label: "Tables", tables: sources.value.filter((t) => !isSavedQuery(t)) },
    { label: "Saved queries", tables: sources.value.filter(isSavedQuery) },
  ].filter((group) => group.tables.length),
);

const running = computed(() => store.busy || placing.value);
const canBuild = computed(
  () =>
    !running.value &&
    store.available &&
    tableIds.value.length > 0 &&
    tableIds.value.length <= MAX_SOURCES &&
    prompt.value.trim().length > 0,
);

const targetLabel = (target: { provider: string | null; model: string | null }): string =>
  target.provider ? `${target.provider}${target.model ? ` (${target.model})` : ""}` : "no provider";
const modelsLine = computed(
  () =>
    `Page planned and written by ${targetLabel(store.dashboardTarget)}; charts built by ${targetLabel(store.chartTarget)}.`,
);
const usesLocal = computed(
  () =>
    store.dashboardTarget.provider === LOCAL_PROVIDER ||
    store.chartTarget.provider === LOCAL_PROVIDER,
);

const ORDER: StepKey[] = ["planning", "building", "writing", "placing"];
const current = computed<StepKey | null>(() => {
  if (placing.value) return "placing";
  return store.stage === "idle" ? null : store.stage;
});
const steps = computed(() => {
  const at = current.value ? ORDER.indexOf(current.value) : -1;
  const settled = store.checks.filter((c) => c.status === "built" || c.status === "failed").length;
  const labels: Record<StepKey, string> = {
    planning: "Planning the page",
    building: store.checks.length
      ? `Building the charts (${settled} of ${store.checks.length})`
      : "Building the charts",
    writing: aiStore.sendsData
      ? "Reading what the charts show and writing the page"
      : "Writing the page",
    placing: "Adding the page to the dashboard",
  };
  return ORDER.map((key, i) => ({
    key,
    label: labels[key],
    state: (i < at ? "done" : i === at ? "active" : "todo") as StepState,
  }));
});
const stepIcon = (state: StepState): string =>
  state === "done" ? "check_circle" : state === "active" ? "autorenew" : "radio_button_unchecked";
const checkIcon = (status: CheckStatus): string =>
  status === "built"
    ? "check"
    : status === "failed"
      ? "close"
      : status === "building"
        ? "autorenew"
        : "more_horiz";

const plural = (n: number, word: string): string => `${n} ${word}${n === 1 ? "" : "s"}`;
const headline = computed(() => {
  if (!result.value) return "";
  const { page, placed } = result.value;
  const parts = [plural(placed.charts, "chart")];
  if (placed.kpis) parts.push(plural(placed.kpis, "KPI"));
  if (placed.filters.length) parts.push(plural(placed.filters.length, "filter"));
  return `“${page.title}” was added with ${listPhrases(parts)}.`;
});
const notBuilt = computed<PageNote[]>(() =>
  result.value ? [...result.value.page.failed, ...result.value.placed.failed] : [],
);

const close = () => emit("update:modelValue", false);

const onCancel = () => store.cancel();

const onBeforeClose = (done: () => void) => {
  if (store.busy) store.cancel();
  done();
  close();
};

const onBuild = async () => {
  if (!canBuild.value) return;
  message.value = null;
  const outcome = await store.build({
    tableIds: [...tableIds.value],
    prompt: prompt.value.trim(),
    convert: createChartConverter(),
  });
  if (outcome.status === "cancelled") {
    message.value = BUILD_CANCELLED_MESSAGE;
    return;
  }
  if (outcome.status === "disabled") {
    message.value = AI_OFF_MESSAGE;
    return;
  }
  if (outcome.status === "failed") {
    message.value = outcome.message;
    return;
  }
  placing.value = true;
  try {
    const placed = await props.place(outcome.page);
    result.value = { page: outcome.page, placed };
  } catch (error) {
    message.value = {
      tone: "error",
      text: "The page was built, but it could not be added to the dashboard.",
      ...(error instanceof Error && error.message ? { detail: error.message } : {}),
    };
  } finally {
    placing.value = false;
  }
};

const onKeydown = (event: KeyboardEvent) => {
  if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
    event.preventDefault();
    void onBuild();
  }
};

watch(
  () => props.modelValue,
  (open) => {
    if (!open) return;
    result.value = null;
    message.value = null;
    aiStore.loadProviders().catch(() => undefined);
    if (!catalogStore.allTables.length) void catalogStore.loadAllTables();
  },
  { immediate: true },
);

onBeforeUnmount(() => store.cancel());
</script>

<style scoped>
.dash-ai {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.dash-ai__label {
  font-size: 13px;
  font-weight: 600;
  color: var(--color-text-primary);
}
.dash-ai__sources {
  width: 100%;
}
.dash-ai__option-path {
  margin-left: 8px;
  font-size: 12px;
  color: var(--color-text-muted);
}
.dash-ai__question {
  width: 100%;
  box-sizing: border-box;
  padding: 6px 10px;
  font: inherit;
  font-size: 13px;
  line-height: 20px;
  color: var(--color-text-primary);
  background: var(--el-fill-color-blank);
  border: 1px solid var(--el-border-color);
  border-radius: 4px;
  resize: vertical;
}
.dash-ai__question:focus-visible {
  outline: 2px solid var(--color-focus-ring-accent, var(--color-accent));
  outline-offset: 1px;
}
.dash-ai__hint {
  margin: 0;
  font-size: 12px;
  line-height: 18px;
  color: var(--color-text-muted);
}
.dash-ai__hint.is-warning {
  color: var(--el-color-warning-dark-2);
}
.dash-ai__steps,
.dash-ai__checks,
.dash-ai__notes {
  margin: 0;
  padding: 0;
  list-style: none;
}
.dash-ai__steps {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 12px;
  border: 1px dashed var(--el-border-color);
  border-radius: 6px;
  font-size: 13px;
}
.dash-ai__step,
.dash-ai__check {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  color: var(--color-text-muted);
}
.dash-ai__step.is-active,
.dash-ai__step.is-done,
.dash-ai__check.is-built,
.dash-ai__check.is-building {
  color: var(--color-text-primary);
}
.dash-ai__step-icon {
  font-size: 16px;
}
.dash-ai__step.is-done .dash-ai__step-icon,
.dash-ai__check.is-built .dash-ai__step-icon {
  color: var(--el-color-success);
}
.dash-ai__step.is-active > .dash-ai__step-icon,
.dash-ai__check.is-building .dash-ai__step-icon {
  color: var(--color-accent);
}
.dash-ai__check.is-failed {
  color: var(--el-color-danger);
}
.dash-ai__checks {
  flex-basis: 100%;
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding-left: 22px;
  font-size: 12px;
}
.dash-ai__detail {
  flex-basis: 100%;
  font-size: 12px;
  color: var(--color-text-muted);
  white-space: pre-wrap;
}
.dash-ai__message {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  padding: 8px 10px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 6px;
  font-size: 13px;
  line-height: 18px;
}
.dash-ai__message.is-error {
  color: var(--el-color-danger);
  background: var(--el-color-danger-light-9);
  border-color: var(--el-color-danger-light-7);
}
.dash-ai__message.is-warning {
  background: var(--el-color-warning-light-9);
  border-color: var(--el-color-warning-light-5);
}
.dash-ai__actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 6px;
}
.dash-ai__result {
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.dash-ai__headline {
  margin: 0;
  font-size: 14px;
  font-weight: 600;
  color: var(--color-text-primary);
}
.dash-ai__section {
  margin: 0 0 4px;
  font-size: 12px;
  font-weight: 600;
  text-transform: none;
  color: var(--color-text-secondary);
}
.dash-ai__notes {
  display: flex;
  flex-direction: column;
  gap: 4px;
  font-size: 13px;
  line-height: 18px;
  color: var(--color-text-primary);
}
</style>
