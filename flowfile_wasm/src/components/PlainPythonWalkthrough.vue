<template>
  <div class="walkthrough">
    <div v-if="!steps.length" class="empty">
      Add a node or two to the canvas and this becomes a step-by-step walkthrough of the code
      behind them.
    </div>

    <template v-else>
      <!-- Step rail: the flow as chips, so the mapping from picture to code is visible. -->
      <div class="rail">
        <button
          v-for="(step, index) in steps"
          :key="step.nodeId"
          class="chip"
          :class="{ current: index === current, exercise: step.concept === 'exercise' }"
          :title="stepLabel(step)"
          @click="goTo(index)"
        >
          <span class="chip-index">{{ index + 1 }}</span>
          <span class="chip-name">{{ stepLabel(step) }}</span>
          <span v-if="step.concept === 'exercise'" class="chip-exercise" title="Left as an exercise">✎</span>
        </button>
      </div>

      <div class="body">
        <!-- What you are actually looking at. -->
        <section v-if="concept" class="concept">
          <h4 class="concept-title">{{ concept.title }}</h4>
          <p v-for="(paragraph, index) in concept.body" :key="index" class="concept-para">
            {{ paragraph }}
          </p>
          <pre v-if="concept.sketch" class="concept-sketch">{{ concept.sketch.join("\n") }}</pre>
          <p v-if="concept.takeaway" class="concept-takeaway">{{ concept.takeaway }}</p>
        </section>

        <!-- The loop for this node's own settings. -->
        <section class="snippet">
          <h5 class="section-label">Your {{ stepLabel(steps[current]).toLowerCase() }} node, as code</h5>
          <Codemirror
            :model-value="snippet"
            :extensions="readOnlyExtensions"
            :disabled="true"
            class="snippet-editor"
          />
          <p v-if="usedHelpers.length" class="snippet-note">
            Calls {{ usedHelpers.join(", ") }} — small helpers written out in full at the top of the
            script, on the <strong>Plain Python</strong> tab.
          </p>
        </section>

        <!-- The rows going in and coming out, at this exact point. -->
        <section class="data">
          <div v-if="dataState === 'idle'" class="data-hint">
            <button class="data-run" :disabled="!canRun" @click="$emit('trace')">
              {{ canRun ? "Show the data at each step" : "Waiting for Python…" }}
            </button>
            <span class="data-note">Runs this script in your browser — nothing leaves the page.</span>
          </div>
          <div v-else-if="dataState === 'running'" class="data-hint">Working it out…</div>
          <div v-else-if="dataState === 'failed'" class="data-hint failed">
            The script raised before this step, so there is nothing to show here yet.
          </div>
          <template v-else>
            <div v-if="!tables.length" class="data-hint">
              {{ blockedReason }}
            </div>
            <template v-else>
              <div class="tables">
                <div v-for="table in tables" :key="table.label" class="table-wrap">
                  <div class="table-head">
                    <span class="table-label">{{ table.label }}</span>
                    <span class="table-count">{{ table.rows.length }} row{{ table.rows.length === 1 ? "" : "s" }}</span>
                  </div>
                  <RowTable :rows="table.rows" :limit="6" />
                </div>
              </div>
              <p v-if="delta" class="delta">{{ delta }}</p>
            </template>
          </template>
        </section>
      </div>

      <div class="nav">
        <button class="nav-btn" :disabled="current === 0" @click="goTo(current - 1)">‹ Back</button>
        <span class="nav-count">Step {{ current + 1 }} of {{ steps.length }}</span>
        <button class="nav-btn" :disabled="current === steps.length - 1" @click="goTo(current + 1)">Next ›</button>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, watch } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorState } from '@codemirror/state'
import { EditorView } from '@codemirror/view'
import RowTable from './RowTable.vue'
import { CONCEPTS, HELPER_NAMES } from '../composables/usePlainPythonGeneration'
import type { PlainStep } from '../composables/usePlainPythonGeneration'
import { getNodeDescription } from '../config/nodeDescriptions'

const props = defineProps<{
  steps: PlainStep[]
  snippets: Record<number, string>
  current: number
  /** varName -> the rows it held, from the trace run. */
  captured: Record<string, Record<string, unknown>[]> | null
  dataState: 'idle' | 'running' | 'ready' | 'failed'
  canRun: boolean
}>()

const emit = defineEmits<{ 'update:current': [number]; trace: []; select: [number] }>()

const readOnlyExtensions = [
  python(),
  oneDark,
  EditorView.editable.of(false),
  EditorState.readOnly.of(true),
  EditorView.lineWrapping,
  EditorView.theme({
    '&': { fontSize: '12px' },
    '.cm-content': { padding: '10px' },
    '.cm-focused': { outline: 'none' }
  })
]

const stepLabel = (step: PlainStep): string => getNodeDescription(step.nodeType).title

const concept = computed(() => CONCEPTS[props.steps[props.current]?.concept ?? ''] ?? null)
const snippet = computed(() => props.snippets[props.steps[props.current]?.nodeId] ?? '')

/** The tables to show: each input this step reads, then what it produced. */
const tables = computed(() => {
  const step = props.steps[props.current]
  if (!step || !props.captured) return []
  const out: Array<{ label: string; rows: Record<string, unknown>[] }> = []
  for (const name of step.inputVars) {
    const rows = props.captured[name]
    if (rows) out.push({ label: `in · ${name}`, rows })
  }
  const produced = props.captured[step.varName]
  // A sink (write to file) binds its input straight through, so showing the
  // same table twice as "in" and "out" is noise, not a before/after.
  if (produced && !out.some(table => table.rows === produced)) {
    out.push({ label: `out · ${step.varName}`, rows: produced })
  }
  return out
})

/** Why this step has no data, when an earlier one does. */
const blockedReason = computed(() => {
  const stopped = props.steps.findIndex(step => !props.captured?.[step.varName])
  if (stopped > 0 && props.steps[stopped]?.concept === 'exercise') {
    return `The script stopped at step ${stopped + 1}, which is still an unfilled exercise — so nothing past it has run yet.`
  }
  if (stopped > 0 && stopped < props.current) {
    return `The script stopped at step ${stopped + 1}, so nothing past it has run yet.`
  }
  return 'No data captured for this step.'
})

/** Steps that collapse rows rather than discarding them. */
const AGGREGATING = new Set(['accumulator-dict'])

/** A plain-English "what just happened to the row count". */
const delta = computed(() => {
  const step = props.steps[props.current]
  if (!step || !props.captured || step.inputVars.length !== 1) return ''
  const before = props.captured[step.inputVars[0]]
  const after = props.captured[step.varName]
  if (!before || !after) return ''
  if (before.length === after.length) return `Same ${after.length} rows going out as came in.`
  if (after.length < before.length) {
    // "Dropped 992" would be a lie about a group by: nothing was discarded,
    // the rows were folded together.
    return AGGREGATING.has(step.concept)
      ? `${before.length} rows in, ${after.length} out — every row is still accounted for, folded into ${after.length} groups.`
      : `${before.length} rows in, ${after.length} out — this step dropped ${before.length - after.length}.`
  }
  return `${before.length} rows in, ${after.length} out — this step produced ${after.length - before.length} more.`
})

/** Helpers the snippet calls but does not define; they live in the full script. */
const usedHelpers = computed(() => {
  const code = snippet.value
  return HELPER_NAMES.filter(name => code.includes(`${name}(`))
})

function goTo(index: number) {
  const clamped = Math.max(0, Math.min(props.steps.length - 1, index))
  emit('update:current', clamped)
}

// Keep the canvas selection in step with the walkthrough, so the node being
// explained is the one highlighted behind the panel.
watch(
  () => props.steps[props.current]?.nodeId,
  nodeId => {
    if (nodeId !== undefined) emit('select', nodeId)
  },
  { immediate: true }
)
</script>

<style scoped>
.walkthrough {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.empty {
  padding: 32px 24px;
  font-size: 13px;
  line-height: 1.6;
  color: var(--color-text-secondary);
}

.rail {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  padding: 12px 20px;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
  flex-shrink: 0;
}

.chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  max-width: 190px;
  padding: 4px 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 999px;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 11px;
  cursor: pointer;
  transition: all 0.15s ease;
}

.chip:hover:not(.current) {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.chip.current {
  background: var(--color-accent);
  border-color: var(--color-accent);
  color: #fff;
}

/* A dashed border alone is nearly invisible; the glyph carries the meaning. */
.chip.exercise:not(.current) {
  border-style: dashed;
}

.chip-exercise {
  font-size: 10px;
  opacity: 0.9;
}

.snippet-note {
  margin: 6px 0 0;
  font-size: 11.5px;
  line-height: 1.5;
  color: var(--color-text-secondary);
}

.chip-index {
  opacity: 0.65;
  font-variant-numeric: tabular-nums;
}

.chip-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.body {
  flex: 1;
  overflow: auto;
  padding: 18px 20px 24px;
}

.concept-title {
  margin: 0 0 8px;
  font-size: 15px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.concept-para {
  margin: 0 0 8px;
  font-size: 13px;
  line-height: 1.65;
  color: var(--color-text-secondary);
  max-width: 68ch;
}

.concept-sketch {
  margin: 12px 0;
  padding: 12px 14px;
  border-left: 3px solid var(--color-accent);
  border-radius: 0 4px 4px 0;
  background: var(--color-background-secondary);
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11.5px;
  line-height: 1.65;
  color: var(--color-text-primary);
  overflow-x: auto;
  white-space: pre;
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

.section-label {
  margin: 22px 0 8px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.5px;
  text-transform: uppercase;
  color: var(--color-text-secondary);
}

.snippet-editor {
  border-radius: 4px;
  overflow: hidden;
}

.data {
  margin-top: 18px;
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

.nav {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 20px;
  border-top: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
  flex-shrink: 0;
}

.nav-btn {
  padding: 6px 16px;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
  font-size: 12px;
  cursor: pointer;
}

.nav-btn:hover:not(:disabled) {
  border-color: var(--color-accent);
}

.nav-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.nav-count {
  font-size: 12px;
  color: var(--color-text-secondary);
  font-variant-numeric: tabular-nums;
}
</style>
