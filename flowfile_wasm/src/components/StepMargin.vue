<template>
  <aside class="margin">
    <div class="margin-tabs" role="tablist" aria-label="Step detail">
      <button
        v-if="showBackground"
        role="tab"
        class="margin-tab"
        :class="{ active: tab === 'why' }"
        :aria-selected="tab === 'why'"
        @click="$emit('update:tab', 'why')"
      >
        Why
      </button>
      <button
        role="tab"
        class="margin-tab"
        :class="{ active: tab === 'data' }"
        :aria-selected="tab === 'data'"
        @click="$emit('update:tab', 'data')"
      >
        Data
        <span v-if="deltaCounts" class="tab-badge">{{ deltaCounts.before }}→{{ deltaCounts.after }}</span>
      </button>
      <button
        role="tab"
        class="margin-tab"
        :class="{ active: tab === 'output', failed: runResult?.failed }"
        :aria-selected="tab === 'output'"
        @click="$emit('update:tab', 'output')"
      >
        Output
        <span v-if="runResult && tab !== 'output'" class="tab-dot"></span>
      </button>
    </div>

    <div ref="bodyEl" class="margin-body">
      <!-- Opt-in: the prose is off until asked for, and then it stays on. -->
      <button
        v-if="!showBackground"
        class="background-cta"
        @click="$emit('toggle-background')"
      >
        <span class="background-cta-glyph">?</span>
        <span class="background-cta-label">Why does it look like this?</span>
        <span class="background-cta-note">show background</span>
      </button>

      <section v-show="tab === 'why'" class="concept">
        <template v-if="concept">
          <div class="concept-head">
            <h4 class="concept-title">{{ concept.title }}</h4>
            <button class="concept-hide" @click="$emit('toggle-background')">Hide background</button>
          </div>
          <p v-for="(paragraph, index) in concept.body" :key="index" class="concept-para">
            {{ paragraph }}
          </p>
          <pre v-if="concept.sketch" class="concept-sketch">{{ concept.sketch.join("\n") }}</pre>
          <p v-if="concept.takeaway" class="concept-takeaway">{{ concept.takeaway }}</p>
        </template>
        <p v-else class="pane-note">No background for this step.</p>
      </section>

      <section v-show="tab === 'data'" class="step-data">
        <p v-if="stale && dataState === 'ready'" class="data-stale">
          The script changed since these rows were captured.
          <button class="data-relink" @click="$emit('trace')">Trace it again</button>
        </p>
        <div v-if="dataState === 'idle'" class="data-hint">
          <button class="data-run" :disabled="!canRun" @click="$emit('trace')">
            {{ canRun ? "Show the data at each step" : "Waiting for Python…" }}
          </button>
          <span class="data-note">Runs the script as it is now, here in your browser — nothing leaves the page.</span>
        </div>
        <div v-else-if="dataState === 'running'" class="data-hint">Working it out…</div>
        <div v-else-if="dataState === 'failed'" class="data-failed">
          <p class="data-hint failed">
            {{ traceError ? "The script raised before any step finished:" : "The trace did not produce any data." }}
          </p>
          <pre v-if="traceError" class="run-error">{{ traceError }}</pre>
          <button class="data-run" :disabled="!canRun" @click="$emit('trace')">Try again</button>
        </div>
        <div v-else-if="!tables.length" class="data-hint-block">
          <p class="data-hint">{{ blockedReason }}</p>
          <pre v-if="traceError" class="run-error">{{ traceError }}</pre>
        </div>
        <template v-else>
          <div class="tables">
            <div v-for="table in tables" :key="table.label" class="table-wrap">
              <div class="table-head">
                <span class="table-label">{{ table.label }}</span>
                <span class="table-count">
                  {{ table.total }} row{{ table.total === 1 ? "" : "s"
                  }}<template v-if="table.total > table.rows.length"> · first {{ table.rows.length }} captured</template>
                </span>
              </div>
              <RowTable :rows="table.rows" :limit="6" :total="table.total" flush />
            </div>
          </div>
          <p v-if="delta" class="delta">{{ delta }}</p>
        </template>
      </section>

      <section v-show="tab === 'output'" class="run-output">
        <template v-if="runResult">
          <div class="run-output-head" :class="{ failed: runResult.failed }">
            <span class="run-summary">
              {{ runResult.failed ? "The script raised" : `Returned ${runResult.rows.length} row(s)` }}
            </span>
            <button
              v-if="!runResult.failed"
              class="run-compare"
              title="Check this result's rows and values against what the canvas produced"
              @click="$emit('compare')"
            >
              Compare to canvas
            </button>
            <button class="run-close" title="Hide output" @click="$emit('close-run')">✕</button>
          </div>
          <p v-if="comparison" class="comparison" :class="comparison.status">
            {{ comparison.message }}
          </p>
          <pre v-if="runResult.failed" class="run-error">{{ runResult.error }}</pre>
          <div v-else-if="runResult.rows.length" class="run-table-wrap">
            <table class="run-table">
              <thead>
                <tr>
                  <th v-for="column in runColumns" :key="column">{{ column }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(row, index) in runResult.rows.slice(0, 50)" :key="index">
                  <td v-for="column in runColumns" :key="column">{{ formatCell(row[column]) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <div v-else class="pane-note">No rows.</div>
        </template>
        <p v-else class="pane-note">Press ▶ to run this script here in the browser.</p>
      </section>
    </div>
  </aside>
</template>

<script setup lang="ts">
import { computed, ref } from 'vue'
import RowTable from './RowTable.vue'
import type { Concept } from '../composables/usePlainPythonGeneration'
import type { PlainRunResult, StepTable } from '../composables/usePlainStep'
import type { CompareResult } from '../composables/usePlainTrace'

export type MarginTab = 'why' | 'data' | 'output'

const props = defineProps<{
  tab: MarginTab
  concept: Concept | null
  tables: StepTable[]
  delta: string
  deltaCounts: { before: number; after: number } | null
  blockedReason: string
  dataState: 'idle' | 'running' | 'ready' | 'failed'
  canRun: boolean
  /** The buffer no longer matches the script the tables were traced from. */
  stale: boolean
  /** Cleaned traceback of the last trace's failure, if it raised unexpectedly. */
  traceError: string | null
  showBackground: boolean
  runResult: PlainRunResult | null
  comparison: CompareResult | null
}>()

defineEmits<{
  'update:tab': [MarginTab]
  trace: []
  compare: []
  'close-run': []
  'toggle-background': []
}>()

const bodyEl = ref<HTMLElement | null>(null)

const runColumns = computed(() => {
  const seen: string[] = []
  for (const row of props.runResult?.rows.slice(0, 50) ?? []) {
    for (const column of Object.keys(row)) if (!seen.includes(column)) seen.push(column)
  }
  return seen
})

const formatCell = (value: unknown): string =>
  value === null || value === undefined ? '—' : typeof value === 'object' ? JSON.stringify(value) : String(value)

defineExpose({
  resetScroll: () => {
    if (bodyEl.value) bodyEl.value.scrollTop = 0
  }
})
</script>

<style scoped>
.margin {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-height: 120px;
  flex: 1 1 0;
  background: var(--color-background-secondary);
}

.margin-tabs {
  flex: 0 0 auto;
  display: flex;
  align-items: flex-end;
  gap: 2px;
  height: 34px;
  padding: 0 12px;
  overflow: hidden;
  border-bottom: 1px solid var(--color-border-primary);
}

.margin-tab {
  display: flex;
  align-items: center;
  gap: 5px;
  padding: 6px 10px;
  border: 0;
  border-bottom: 2px solid transparent;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 12px;
  cursor: pointer;
}

.margin-tab:hover:not(.active) {
  color: var(--color-text-primary);
}

.margin-tab.active {
  color: var(--color-text-primary);
  border-bottom-color: var(--color-accent);
}

.margin-tab.failed {
  color: var(--color-danger);
}

.tab-badge {
  font-variant-numeric: tabular-nums;
  opacity: 0.75;
  font-size: 11px;
}

.tab-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--color-accent);
}

/* SCROLLER #2 — the only vertical scroller in this column. */
.margin-body {
  flex: 1 1 auto;
  min-height: 0;
  overflow-y: auto;
  overflow-x: hidden;
  overscroll-behavior: contain;
  padding: 12px 16px 22px;
}

.background-cta {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%;
  margin-bottom: 12px;
  padding: 6px 10px;
  border: 1px dashed var(--color-border-primary);
  border-radius: 6px;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 12px;
  text-align: left;
  cursor: pointer;
}

.background-cta:hover {
  border-color: var(--color-accent);
  border-style: solid;
  color: var(--color-text-primary);
}

.background-cta-glyph {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  border: 1px solid currentColor;
  font-size: 10px;
  font-weight: 600;
}

.background-cta-label {
  font-weight: 500;
}

.background-cta-note {
  margin-left: auto;
  font-size: 11px;
  opacity: 0.7;
}

.concept-head {
  display: flex;
  align-items: baseline;
  gap: 10px;
}

.concept-title {
  margin: 0 0 8px;
  font-size: 15px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.concept-hide {
  margin-left: auto;
  padding: 0;
  border: 0;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 11px;
  text-decoration: underline;
  cursor: pointer;
}

.concept-hide:hover {
  color: var(--color-text-primary);
}

.concept-para {
  margin: 0 0 8px;
  font-size: 13px;
  line-height: 1.65;
  color: var(--color-text-secondary);
  max-width: 68ch;
}

/* Nested scrollers in here are HORIZONTAL only: never pair overflow-x: auto
   with a visible y, which the spec computes back to auto. */
.concept-sketch {
  margin: 12px 0;
  padding: 12px 14px;
  border-left: 3px solid var(--color-accent);
  border-radius: 0 4px 4px 0;
  background: var(--color-background-tertiary);
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11.5px;
  line-height: 1.65;
  color: var(--color-text-primary);
  white-space: pre;
  overflow-x: auto;
  overflow-y: hidden;
}

.concept-takeaway {
  margin: 10px 0 0;
  padding: 8px 12px;
  border-radius: 4px;
  background: var(--color-background-tertiary);
  font-size: 12.5px;
  line-height: 1.6;
  color: var(--color-text-primary);
  max-width: 68ch;
}

.data-stale {
  margin: 0 0 12px;
  padding: 8px 10px;
  border-radius: 4px;
  background: var(--color-background-tertiary);
  font-size: 11.5px;
  line-height: 1.55;
  color: var(--color-text-secondary);
}

.data-relink {
  margin-left: 6px;
  padding: 0;
  border: 0;
  background: transparent;
  color: var(--color-accent);
  font-size: 11.5px;
  text-decoration: underline;
  cursor: pointer;
}

.data-hint {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  font-size: 12px;
  color: var(--color-text-secondary);
}

.data-hint.failed {
  color: var(--color-danger);
}

.data-failed,
.data-hint-block {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 10px;
}

.data-run {
  padding: 6px 14px;
  border: 1px solid var(--color-accent);
  border-radius: 6px;
  background: transparent;
  color: var(--color-accent);
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
}

.data-run:hover:not(:disabled) {
  background: var(--color-accent);
  color: #fff;
}

.data-run:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.data-note {
  font-size: 11px;
  opacity: 0.75;
}

.tables {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.table-head {
  display: flex;
  align-items: baseline;
  gap: 8px;
  margin-bottom: 4px;
}

.table-label {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11.5px;
  color: var(--color-text-primary);
}

.table-count {
  font-size: 11px;
  color: var(--color-text-secondary);
}

.delta {
  margin: 12px 0 0;
  font-size: 12.5px;
  color: var(--color-text-primary);
}

.run-output-head {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
  font-size: 12px;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.run-output-head.failed {
  color: var(--color-danger);
}

/* The verdict gets a full line: the messages now name rows, columns and cells. */
.comparison {
  margin: 0 0 10px;
  font-size: 12px;
  font-weight: 500;
  line-height: 1.5;
}

.comparison.mismatch {
  color: var(--color-danger);
}

.comparison.match {
  color: var(--color-success, #2e9e5b);
}

/* Guidance, not a verdict — neutral on purpose. */
.comparison.info {
  color: var(--color-text-secondary);
  font-weight: 400;
}

.run-compare {
  margin-left: auto;
  padding: 2px 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 11px;
  cursor: pointer;
}

.run-compare:hover {
  border-color: var(--color-accent);
  color: var(--color-text-primary);
}

.run-close {
  border: none;
  background: transparent;
  color: inherit;
  cursor: pointer;
  font-size: 12px;
}

.run-error {
  margin: 0;
  font-family: monospace;
  font-size: 11px;
  line-height: 1.5;
  white-space: pre;
  color: var(--color-danger);
  overflow-x: auto;
  overflow-y: hidden;
  max-width: 100%;
}

.run-table-wrap {
  overflow-x: auto;
  overflow-y: hidden;
}

.run-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.run-table th,
.run-table td {
  padding: 5px 12px;
  text-align: left;
  border-bottom: 1px solid var(--color-border-primary);
  color: var(--color-text-primary);
  white-space: nowrap;
}

.run-table th {
  font-weight: 600;
  color: var(--color-text-secondary);
}

.pane-note {
  margin: 0;
  font-size: 12px;
  color: var(--color-text-secondary);
}
</style>
