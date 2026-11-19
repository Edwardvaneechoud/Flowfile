<template>
  <div v-if="visible" class="chart-prompt" data-testid="chart-prompt-bar">
    <div class="chart-prompt__row">
      <span class="material-icons chart-prompt__icon" aria-hidden="true">auto_awesome</span>
      <el-input
        v-model="prompt"
        type="textarea"
        :autosize="{ minRows: 1, maxRows: 4 }"
        :placeholder="placeholder"
        :maxlength="MAX_PROMPT_LENGTH"
        resize="none"
        class="chart-prompt__input"
        data-testid="chart-prompt-input"
        aria-label="Describe a chart for the AI to build"
        @focus="refresh"
        @keydown.enter.meta.prevent="submit"
        @keydown.enter.ctrl.prevent="submit"
        @keydown="keepUndoInField"
      />
      <button
        type="button"
        class="chart-prompt__generate"
        data-testid="chart-prompt-generate"
        :disabled="!canGenerate"
        :title="ready ? 'Generate (⌘/Ctrl+Enter)' : 'The chart builder is still loading'"
        @click="submit"
      >
        <span class="material-icons" aria-hidden="true">auto_awesome</span>
        <span>Generate</span>
      </button>
    </div>

    <div class="chart-prompt__meta">
      <span data-testid="chart-prompt-sends">{{
        aiStore.sendsData ? SENDS_LINE : NAMES_ONLY_LINE
      }}</span>
      <span class="chart-prompt__keys">⌘/Ctrl+Enter</span>
    </div>

    <div v-if="store.loading" class="chart-prompt__line is-busy" data-testid="chart-prompt-busy">
      <el-icon class="is-loading"><Loading /></el-icon>
      <span>Building the chart…</span>
      <button
        type="button"
        class="chart-prompt__link"
        data-testid="chart-prompt-cancel"
        @click="onCancel"
      >
        Cancel
      </button>
    </div>

    <div
      v-if="message"
      class="chart-prompt__line"
      :class="`is-${message.tone}`"
      data-testid="chart-prompt-message"
      role="status"
    >
      <span class="material-icons chart-prompt__line-icon" aria-hidden="true">{{
        iconFor(message.tone)
      }}</span>
      <div class="chart-prompt__line-body">
        <span>{{ message.text }}</span>
        <span v-if="message.detail" class="chart-prompt__detail">{{ message.detail }}</span>
        <template v-if="!explanation">
          <span
            v-for="(warning, i) in warnings"
            :key="i"
            class="chart-prompt__detail"
            data-testid="chart-prompt-warning"
            >{{ warning }}</span
          >
        </template>
      </div>
      <button
        v-if="held"
        type="button"
        class="chart-prompt__link"
        data-testid="chart-prompt-apply-anyway"
        @click="applyHeld"
      >
        Apply anyway
      </button>
    </div>

    <div
      v-if="explanation"
      class="chart-prompt__line is-explanation"
      data-testid="chart-prompt-explanation"
    >
      <span class="material-icons chart-prompt__line-icon" aria-hidden="true">auto_awesome</span>
      <div class="chart-prompt__line-body">
        <span>{{ explanation }}</span>
        <span
          v-for="(warning, i) in warnings"
          :key="i"
          class="chart-prompt__detail"
          data-testid="chart-prompt-warning"
          >{{ warning }}</span
        >
      </div>
    </div>

    <div
      v-if="carryNotice && !store.loading"
      class="chart-prompt__line is-warning"
      data-testid="chart-prompt-carry-notice"
    >
      <span class="material-icons chart-prompt__line-icon" aria-hidden="true">info</span>
      <div class="chart-prompt__line-body">
        <span>{{ carryNotice.text }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, shallowRef } from "vue";
import { Loading } from "@element-plus/icons-vue";
import type { IChart } from "@kanaries/graphic-walker/interfaces";
import type { ChartGenerationResponse } from "../../api/ai.api";
import { useAiStore } from "../../stores/ai-store";
import { useAiChartStore } from "../../stores/ai-chart-store";
import {
  AI_OFF_MESSAGE,
  APPLY_FAILED_MESSAGE,
  BUILDER_RELOADED_MESSAGE,
  CANCELLED_MESSAGE,
  EDITED_MEANWHILE_MESSAGE,
  PLACEHOLDER_CHANGE,
  PLACEHOLDER_NEW,
  cannotCarryOverMessage,
  chartFromAnswer,
  chartKey,
  planRequest,
  NAMES_ONLY_LINE,
  SENDS_LINE,
  type ChartBuilderHandle,
  type ChartPromptMessage,
} from "./chartPrompt";

const MAX_PROMPT_LENGTH = 2000;
const POLL_MS = 1000;

const props = defineProps<{
  /** The catalog table the chart reads; null hides the bar (a chart on a SQL query). */
  tableId: number | null;
  /** The builder now on screen; read on every use because the host can remount it. */
  builder: () => ChartBuilderHandle | null;
}>();

const emit = defineEmits<{
  (e: "applied", value: { title: string | null; chart: IChart }): void;
}>();

const aiStore = useAiStore();
const store = useAiChartStore();

const prompt = ref("");
const ready = ref(false);
const hasChart = ref(false);
const message = ref<ChartPromptMessage | null>(null);
const explanation = ref<string | null>(null);
const warnings = ref<string[]>([]);
const carryNotice = ref<ChartPromptMessage | null>(null);
// An answer that arrived after a hand edit, kept so "Apply anyway" can still use it; shallow so the
// handle stays the builder's own object and compares equal to it.
const held = shallowRef<{ response: ChartGenerationResponse; handle: ChartBuilderHandle } | null>(
  null,
);

let seenHandle: ChartBuilderHandle | null = null;
let seenKey: string | null = null;
let appliedKey: string | null = null;
let pollTimer: ReturnType<typeof setInterval> | null = null;
let unmounted = false;

const visible = computed(() => props.tableId != null && store.available);
const placeholder = computed(() => (hasChart.value ? PLACEHOLDER_CHANGE : PLACEHOLDER_NEW));
const canGenerate = computed(() => ready.value && prompt.value.trim().length > 0);

const iconFor = (tone: ChartPromptMessage["tone"]) =>
  tone === "error" ? "error_outline" : tone === "warning" ? "warning_amber" : "info";

/** Re-read the builder: readiness, whether a chart is on screen and whether it can go along. */
const refresh = (): void => {
  const handle = props.builder();
  const gw = handle?.graphicWalker() ?? null;
  const fields = handle?.datasetFields() ?? null;
  ready.value = Boolean(handle && gw && fields && fields.length);
  if (!handle || !gw || !fields || !ready.value) {
    hasChart.value = false;
    carryNotice.value = null;
    seenHandle = null;
    seenKey = null;
    return;
  }
  const current = handle.currentChart();
  const key = chartKey(current);
  if (handle === seenHandle && key === seenKey) return;
  seenHandle = handle;
  seenKey = key;
  hasChart.value = current !== null;
  const plan = planRequest(current, { fields, gw });
  carryNotice.value = plan.unsupported.length ? cannotCarryOverMessage(plan.unsupported) : null;
  if (appliedKey !== null && key !== appliedKey) {
    appliedKey = null;
    explanation.value = null;
    warnings.value = [];
  }
};

const clearResult = () => {
  message.value = null;
  explanation.value = null;
  warnings.value = [];
  held.value = null;
};

/** Draw the answer on `handle`; returns false and sets the message when it cannot. */
const apply = (response: ChartGenerationResponse, handle: ChartBuilderHandle): boolean => {
  const gw = handle.graphicWalker();
  const fields = handle.datasetFields();
  if (!gw || !fields) {
    message.value = BUILDER_RELOADED_MESSAGE;
    return false;
  }
  const answer = chartFromAnswer(response, { fields, current: handle.currentChart(), gw });
  if (!answer.ok) {
    message.value = answer.message;
    return false;
  }
  if (!handle.applyChart(answer.chart)) {
    message.value = APPLY_FAILED_MESSAGE;
    return false;
  }
  seenKey = null;
  refresh();
  appliedKey = seenKey;
  explanation.value = answer.explanation ?? "Done.";
  warnings.value = response.warnings ?? [];
  emit("applied", { title: answer.title, chart: answer.chart });
  return true;
};

const submit = async (): Promise<void> => {
  refresh();
  const text = prompt.value.trim();
  const handle = props.builder();
  const gw = handle?.graphicWalker() ?? null;
  const fields = handle?.datasetFields() ?? null;
  if (!text || !ready.value || props.tableId == null || !handle || !gw || !fields) return;

  const current = handle.currentChart();
  const sentKey = chartKey(current);
  const plan = planRequest(current, { fields, gw });
  clearResult();

  const outcome = await store.generate({
    tableId: props.tableId,
    prompt: text,
    currentChart: plan.currentChart,
  });
  if (unmounted || outcome.status === "superseded") return;
  if (outcome.status === "cancelled") {
    message.value = CANCELLED_MESSAGE;
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
  const response = outcome.response;
  if (props.builder() !== handle) {
    message.value = BUILDER_RELOADED_MESSAGE;
    return;
  }
  if (response.degraded || !response.chart) {
    apply(response, handle);
    warnings.value = response.warnings ?? [];
    return;
  }
  if (chartKey(handle.currentChart()) !== sentKey) {
    message.value = EDITED_MEANWHILE_MESSAGE;
    held.value = { response, handle };
    return;
  }
  apply(response, handle);
};

const applyHeld = () => {
  const pending = held.value;
  held.value = null;
  message.value = null;
  if (!pending) return;
  if (props.builder() !== pending.handle) {
    message.value = BUILDER_RELOADED_MESSAGE;
    return;
  }
  apply(pending.response, pending.handle);
};

const onCancel = () => store.cancel();

// Graphic Walker undoes the chart on Ctrl+Z anywhere in the page; in the prompt it is the text's undo.
const keepUndoInField = (event: KeyboardEvent) => {
  if ((event.ctrlKey || event.metaKey) && /^[zy]$/i.test(event.key)) event.stopPropagation();
};

onMounted(() => {
  if (!aiStore.providersLoading) void aiStore.loadProviders();
  refresh();
  pollTimer = setInterval(refresh, POLL_MS);
});

onBeforeUnmount(() => {
  unmounted = true;
  if (pollTimer !== null) clearInterval(pollTimer);
  if (store.loading) store.cancel();
});
</script>

<style scoped>
.chart-prompt {
  display: flex;
  flex-direction: column;
  gap: 6px;
  flex-shrink: 0;
  padding: 8px 10px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
  background: var(--el-fill-color-extra-light, var(--color-background-secondary));
}
.chart-prompt__row {
  display: flex;
  align-items: flex-start;
  gap: 8px;
}
.chart-prompt__icon {
  font-size: 18px;
  color: var(--color-accent);
  margin-top: 6px;
}
.chart-prompt__input {
  flex: 1;
  min-width: 0;
}
.chart-prompt__input :deep(.el-textarea__inner) {
  font-size: 13px;
  line-height: 20px;
  padding: 5px 10px;
}
.chart-prompt__generate {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  height: 32px;
  padding: 0 12px;
  font-size: 13px;
  font-weight: 500;
  color: var(--color-accent);
  background-color: var(--color-accent-subtle);
  border: 1px solid var(--color-accent);
  border-radius: 4px;
  cursor: pointer;
  white-space: nowrap;
  transition:
    background-color 0.15s,
    color 0.15s;
}
.chart-prompt__generate:hover:not(:disabled) {
  background-color: var(--color-accent);
  color: var(--color-text-inverse);
}
.chart-prompt__generate:focus-visible {
  outline: 2px solid var(--color-focus-ring-accent, var(--color-accent));
  outline-offset: 2px;
}
.chart-prompt__generate:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}
.chart-prompt__generate .material-icons {
  font-size: 15px;
}
.chart-prompt__meta {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  padding-left: 26px;
  font-size: 11px;
  color: var(--color-text-muted);
}
.chart-prompt__keys {
  white-space: nowrap;
}
.chart-prompt__line {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  margin-left: 26px;
  padding: 6px 8px;
  border-radius: 6px;
  font-size: 12px;
  line-height: 18px;
  color: var(--color-text-primary);
  background: var(--el-fill-color-blank);
  border: 1px solid var(--el-border-color-lighter);
}
.chart-prompt__line.is-busy {
  align-items: center;
  color: var(--color-text-secondary);
  border-style: dashed;
}
.chart-prompt__line.is-error {
  color: var(--el-color-danger);
  background: var(--el-color-danger-light-9);
  border-color: var(--el-color-danger-light-7);
}
.chart-prompt__line.is-warning {
  background: var(--el-color-warning-light-9);
  border-color: var(--el-color-warning-light-5);
}
.chart-prompt__line.is-warning .chart-prompt__line-icon {
  color: var(--el-color-warning);
}
.chart-prompt__line.is-explanation .chart-prompt__line-icon,
.chart-prompt__line.is-info .chart-prompt__line-icon {
  color: var(--color-accent);
}
.chart-prompt__line-icon {
  color: var(--color-warning);
}
.chart-prompt__line.is-explanation .chart-prompt__line-icon,
.chart-prompt__line.is-info .chart-prompt__line-icon {
  color: var(--color-accent);
}
.chart-prompt__line.is-explanation {
  color: var(--color-text-primary);
}
.chart-prompt__line-icon {
  font-size: 16px;
  line-height: 18px;
}
.chart-prompt__line-body {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
  flex: 1;
}
.chart-prompt__detail {
  color: var(--color-text-muted);
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
.chart-prompt__link {
  border: none;
  background: transparent;
  padding: 0;
  font-size: 12px;
  color: var(--color-accent);
  cursor: pointer;
  text-decoration: underline;
  white-space: nowrap;
}
.chart-prompt__link:hover {
  color: var(--color-accent-hover);
}
</style>
