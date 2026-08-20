<template>
  <DraggableItem
    v-if="isVisible"
    id="code-panel"
    title="Code"
    :tabs="tabs"
    :active-tab="mode"
    :show-right="true"
    initial-position="right"
    :initial-width="620"
    :initial-top="topOffset"
    height-behaviour="scale"
    :allow-full-screen="true"
    :flush-content="true"
    :on-close="closePanel"
    @update:active-tab="setMode($event as CodeMode)"
  >
    <template #actions>
      <button
        v-if="teachingMode && !learning.enabled"
        class="icon-button learn-button"
        title="Learning Python? Step through this flow's code, one node at a time"
        @click="enableLearning"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21.4 10.9a1 1 0 0 0 0-1.83L12.8 5.18a2 2 0 0 0-1.66 0L2.6 9.08a1 1 0 0 0 0 1.83l8.57 3.91a2 2 0 0 0 1.66 0z"></path>
          <path d="M22 10v6"></path>
          <path d="M6 12.5V16a6 3 0 0 0 12 0v-3.5"></path>
        </svg>
      </button>
      <button
        v-if="isWalkthrough"
        class="icon-button run-button"
        :disabled="running || loading || !pyodideStore.isReady"
        :title="pyodideStore.isReady ? 'Run this script here in the browser' : 'Python is still starting up'"
        @click="runScript"
      >
        <svg v-if="!running" width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
          <polygon points="6 4 20 12 6 20 6 4"></polygon>
        </svg>
        <span v-if="running" class="spinner"></span>
      </button>
      <button
        v-if="isWalkthrough && edited"
        class="icon-button reset-button"
        title="Discard your edits and regenerate"
        @click="resetEdits"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M3 12a9 9 0 1 0 3-6.7L3 8"></path>
          <path d="M3 3v5h5"></path>
        </svg>
      </button>
      <button class="icon-button refresh-button" :disabled="loading" title="Refresh code" @click="refreshCode">
        <svg
          v-if="!loading"
          width="14"
          height="14"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          stroke-width="2"
        >
          <path d="M23 4v6h-6"></path>
          <path d="M1 20v-6h6"></path>
          <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
        </svg>
        <span v-if="loading" class="spinner"></span>
      </button>
      <button class="icon-button export-button" title="Export as .py file" @click="exportCode">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
          <polyline points="7 10 12 15 17 10"></polyline>
          <line x1="12" y1="15" x2="12" y2="3"></line>
        </svg>
      </button>
    </template>

    <div ref="bodyEl" class="code-body">
      <div v-if="error" class="error-message">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"></circle>
          <line x1="12" y1="8" x2="12" y2="12"></line>
          <line x1="12" y1="16" x2="12.01" y2="16"></line>
        </svg>
        <span>{{ error }}</span>
      </div>

      <!-- The premise is "this code IS your flow" — never let them drift silently. -->
      <div v-if="flowChanged" class="flow-changed" role="status">
        <span>The flow changed — this code describes the previous version.</span>
        <button class="flow-changed-refresh" @click="refreshCode">Refresh</button>
      </div>

      <!-- The flow as chips: a constant-height rail, so stepping never resizes the editor. -->
      <nav v-if="isWalkthrough && steps.length" class="step-rail" aria-label="Steps">
        <button class="rail-arrow" :disabled="stepIndex === 0" title="Previous step" @click="goTo(stepIndex - 1)">
          ‹
        </button>
        <div ref="railEl" class="rail-track">
          <button
            v-for="(step, index) in steps"
            :key="step.nodeId"
            :ref="element => setChipRef(element, index)"
            class="chip"
            :class="{ current: index === stepIndex }"
            :title="stepLabel(step)"
            @click="goTo(index)"
          >
            <span class="chip-index">{{ index + 1 }}</span>
            <span class="chip-name">{{ stepLabel(step) }}</span>
          </button>
        </div>
        <button
          class="rail-arrow"
          :disabled="stepIndex === steps.length - 1"
          title="Next step"
          @click="goTo(stepIndex + 1)"
        >
          ›
        </button>
        <span class="rail-count">{{ stepIndex + 1 }} / {{ steps.length }}</span>
      </nav>

      <div v-if="isWalkthrough && !steps.length && !error" class="empty-state">
        Add a node or two to the canvas and this becomes a step-by-step walkthrough of the code
        behind them.
      </div>

      <div
        v-else
        ref="workbenchEl"
        class="workbench"
        :class="{ split: isWalkthrough, 'is-wide': isWide }"
        :style="{ '--split-x': `${split.x}%`, '--split-y': `${split.y}%` }"
      >
        <section ref="benchEl" class="bench">
          <div v-if="isWalkthrough" class="bench-head">
            <span v-if="steps.length" class="bench-label">
              step {{ stepIndex + 1 }}<template v-if="labelLines">
                · lines {{ labelLines.from }}–{{ labelLines.to }}</template>
            </span>
            <div class="flavour-toggle" role="group" aria-label="Walkthrough flavour">
              <button
                :class="{ active: flavour === 'plain' }"
                :disabled="loading || running"
                title="The same flow in plain Python — lists, dicts and loops, no dataframe library"
                @click="setFlavour('plain')"
              >
                Plain Python
              </button>
              <button
                :class="{ active: flavour === 'polars' }"
                :disabled="loading || running"
                title="The same steps written with the Polars dataframe library"
                @click="setFlavour('polars')"
              >
                Polars
              </button>
            </div>
            <button v-if="highlightOffScreen" class="bench-recentre" @click="recentre">
              ↩ step {{ stepIndex + 1 }}
            </button>
          </div>
          <div class="bench-editor">
            <!-- indent-with-tab off: Tab must move focus, not indent — the
                 vue-codemirror default is a keyboard trap (WCAG 2.1.2). -->
            <Codemirror
              v-model="code"
              :extensions="extensions"
              :disabled="!isWalkthrough"
              :indent-with-tab="false"
              :style="{ height: '100%', fontSize: '13px' }"
              @ready="onEditorReady"
            />
          </div>
        </section>

        <div
          v-if="isWalkthrough"
          class="split-divider"
          role="separator"
          tabindex="0"
          :aria-orientation="isWide ? 'vertical' : 'horizontal'"
          title="Drag to resize — double-click to reset"
          @pointerdown="startDrag"
          @dblclick="resetSplit"
          @keydown="nudgeSplit"
        ></div>

        <StepMargin
          v-if="isWalkthrough"
          ref="marginEl"
          v-model:tab="marginTab"
          :concept="concept"
          :polars-doc="polarsDoc"
          :tables="tables"
          :delta="delta"
          :delta-counts="deltaCounts"
          :data-loading="stepDataLoading"
          :show-background="learning.showBackground"
          :run-result="runResult"
          :comparison="comparison"
          @compare="compareToCanvas"
          @close-run="runResult = null"
          @toggle-background="toggleBackground"
        />
      </div>
    </div>
  </DraggableItem>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView } from '@codemirror/view'
import { useFlowStore } from '../stores/flow-store'
import { usePyodideStore } from '../stores/pyodide-store'
import { useCodeGeneration } from '../composables/useCodeGeneration'
import { EXPR_TRANSFORMER_PACKAGE, translateFormulaNodes } from '../composables/useFormulaTranslation'
import { buildPolarsWalkthrough, POLARS_DOC_FOR_NODE } from '../composables/usePolarsWalkthrough'
import { usePlainPythonGeneration } from '../composables/usePlainPythonGeneration'
import type { PlainWalkthrough } from '../composables/usePlainPythonGeneration'
import { usePlainStep, stepLabel, type PlainRunResult, type CapturedTrace } from '../composables/usePlainStep'
import { glossaryTooltip } from '../composables/usePythonGlossary'
import { compareTables, type CompareResult } from '../composables/usePlainTrace'
import {
  allStepLines,
  seedStepsIn,
  showStep,
  stepHighlight,
  stepLines,
  stepVisible,
  type StepRange
} from '../composables/useStepHighlight'
import { useLearningStore } from '../stores/learning-store'
import DraggableItem from './common/DraggableItem/DraggableItem.vue'
import StepMargin, { type MarginTab } from './StepMargin.vue'
import type { NodeReadSettings } from '../types'

const props = withDefaults(
  defineProps<{
    isVisible: boolean
    /** Offer the plain-Python teaching flavour alongside the Polars one. */
    teachingMode?: boolean
    /** Container-local top edge, so the panel clears the in-canvas toolbar. */
    topOffset?: number
  }>(),
  { teachingMode: true, topOffset: 0 }
)

const emit = defineEmits<{
  close: []
  /** The walkthrough moved to this node — the canvas should bring it into view. */
  'focus-node': [nodeId: number]
}>()

const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()
const learning = useLearningStore()
const { generateCode } = useCodeGeneration()
const { buildWalkthrough } = usePlainPythonGeneration()

type CodeMode = 'polars' | 'walkthrough'
type WalkthroughFlavour = 'plain' | 'polars'
const MODE_KEY = 'flowfile-codegen-mode'
const FLAVOUR_KEY = 'flowfile-walkthrough-flavour'
const MARGIN_TAB_KEY = 'flowfile-codegen-margin-tab'
const SPLIT_KEY = 'flowfile-codegen-split'
/** Panel width at which the bench and the margin sit side by side. */
const WIDE_AT = 880
const DEFAULT_SPLIT = { x: 60, y: 64 }
const FLOORS = { bench: 200, margin: 120, benchX: 460, marginX: 340 }

function initialMode(): CodeMode {
  // The walkthrough is opt-in: without Learning mode the panel is a plain
  // Polars view, whatever tab an earlier learning session left behind.
  if (!learning.enabled) return 'polars'
  const saved = localStorage.getItem(MODE_KEY)
  // 'plain' and 'walkthrough' were the two halves of what is now one tab.
  if (saved === 'plain') {
    localStorage.setItem(MODE_KEY, 'walkthrough')
    return 'walkthrough'
  }
  if (saved === 'walkthrough' || saved === 'polars') return saved
  return 'walkthrough'
}

function initialFlavour(): WalkthroughFlavour {
  return localStorage.getItem(FLAVOUR_KEY) === 'polars' ? 'polars' : 'plain'
}
// Written ONLY by setFlavour, which always routes through generateCodeFromFlow —
// that is what lets steps/trace/run state stay single rather than per-flavour.
const flavour = ref<WalkthroughFlavour>(initialFlavour())

const code = ref('')
const loading = ref(false)
const error = ref<string | null>(null)
const mode = ref<CodeMode>('polars')
const running = ref(false)
const runResult = ref<PlainRunResult | null>(null)
const comparison = ref<CompareResult | null>(null)

const isWalkthrough = computed(() => mode.value === 'walkthrough')

// Header tab strip. Empty ⇒ DraggableItem shows the plain "Code" title.
// The walkthrough tab exists only after the user opts into Learning mode, so
// the default panel stays a plain code view.
const tabs = computed(() =>
  props.teachingMode && learning.enabled
    ? [
        { id: 'polars', label: 'Polars', title: 'Production code using the Polars dataframe library' },
        {
          id: 'walkthrough',
          label: 'Python walkthrough',
          title: 'Step through the flow one node at a time — in plain Python or in Polars'
        }
      ]
    : []
)

// Editing the generated script is the point in walkthrough mode, so track
// whether the buffer still matches what we generated — that is what "reset" undoes.
// Per-flavour slots: each walkthrough flavour keeps its own generated/draft/ranges
// so switching never cross-contaminates. `generated` must be per-flavour too —
// it keeps `edited` consistent during the polars flavour's async build, and both
// builders emit one step per node, so a shared draftRanges would pass seedNow's
// length guard while describing the OTHER flavour's document.
interface FlavourSlot {
  generated: string
  draft: string | null
  draftRanges: StepRange[] | null
}
const flavourState = reactive<Record<WalkthroughFlavour, FlavourSlot>>({
  plain: { generated: '', draft: null, draftRanges: null },
  polars: { generated: '', draft: null, draftRanges: null }
})
const generated = computed({
  get: () => flavourState[flavour.value].generated,
  set: value => {
    flavourState[flavour.value].generated = value
  }
})
// Survives a trip to another tab or flavour; cleared by Reset only.
const draft = computed({
  get: () => flavourState[flavour.value].draft,
  set: value => {
    flavourState[flavour.value].draft = value
  }
})
// The step ranges as the highlight field last mapped them through the edits.
// Stashed with the draft, because reseeding a draft from the generator's
// pristine line numbers would misplace every highlight below an edit.
const draftRanges = computed({
  get: () => flavourState[flavour.value].draftRanges,
  set: value => {
    flavourState[flavour.value].draftRanges = value
  }
})
const edited = computed(() => isWalkthrough.value && code.value !== generated.value)

const stashDraft = () => {
  draft.value = code.value
  if (editorView) draftRanges.value = allStepLines(editorView.state)
}
const clearDraft = () => {
  draft.value = null
  draftRanges.value = null
}

const walkthrough = ref<PlainWalkthrough>({ script: '', steps: [] })
const steps = computed(() => walkthrough.value.steps)
const stepIndex = ref(0)
// Per-varName tables for the Data tab, filled from the CANVAS run's node
// previews (never by re-executing anything — see loadStepData).
const captured = ref<CapturedTrace | null>(null)
const stepDataLoading = ref(false)
// Bumped whenever the document is replaced programmatically; drives re-seeding.
const scriptRevision = ref(0)
// Bumped at the start of every regeneration. An in-flight ▶ run that finishes
// after a flavour switch must throw its result away — the flavours bind the
// same variable names, so stale tables would render as valid data.
const buildEpoch = ref(0)

const { concept, tables, delta, deltaCounts } = usePlainStep(steps, stepIndex, captured)

// Polars flavour: the Why tab links to the operation's reference page instead
// of the plain flavour's teaching card.
const polarsDoc = computed(() =>
  isWalkthrough.value && flavour.value === 'polars'
    ? (POLARS_DOC_FOR_NODE[steps.value[stepIndex.value]?.nodeType ?? ''] ?? null)
    : null
)

// Step varName -> the node that PRODUCES that table (first writer wins, so a
// sink step reusing its input's name never shadows the real producer).
const varToNode = computed(() => {
  const map = new Map<string, number>()
  for (const step of steps.value) {
    if (!map.has(step.varName)) map.set(step.varName, step.nodeId)
  }
  return map
})

/**
 * Fill the current step's Data tab from the canvas run.
 *
 * Strictly preview-only: it reads existing node results and materialized
 * previews (fetchNodePreview never reaches an execute_* bridge — the
 * no-auto-run contract). Nothing here runs the flow; until the user does,
 * the tab says so.
 */
const loadStepData = async () => {
  const active = steps.value[stepIndex.value]
  if (!active || !props.isVisible || !isWalkthrough.value) return
  const epoch = buildEpoch.value
  const names = [...active.inputVars, active.varName]
  stepDataLoading.value = true
  try {
    for (const name of names) {
      if (captured.value?.tables[name]) continue
      const nodeId = varToNode.value.get(name)
      if (nodeId === undefined) continue
      if (!flowStore.nodeResults.get(nodeId)?.success) continue
      const preview = await flowStore.fetchNodePreview(nodeId).catch(() => null)
      if (epoch !== buildEpoch.value) return
      const data = preview?.data
      if (!data?.columns) continue
      const rows = (data.data ?? []).map((cells: unknown[]) =>
        Object.fromEntries(data.columns.map((column: string, i: number) => [column, cells[i]]))
      )
      const bucket = captured.value ?? { tables: {}, counts: {} }
      bucket.tables[name] = rows
      bucket.counts[name] = data.total_rows ?? rows.length
      captured.value = { ...bucket }
    }
  } finally {
    if (epoch === buildEpoch.value) stepDataLoading.value = false
  }
}

watch([stepIndex, steps, () => props.isVisible], () => void loadStepData())
// A finished canvas run supersedes whatever previews were captured before it.
watch(
  () => flowStore.isExecuting,
  executing => {
    if (executing) return
    captured.value = null
    void loadStepData()
  }
)

/**
 * A cheap identity of the flow's shape and settings. updateNodeSettings
 * replaces the settings object, so this recomputes only when something real
 * changed — and Lite flows are small enough to stringify.
 */
const flowFingerprint = computed(() => {
  const nodes = [...flowStore.nodes.values()]
    .map(node => `${node.id}:${node.type}:${JSON.stringify(node.settings ?? null)}`)
    .join('|')
  return `${nodes}§${flowStore.edges.map(edge => edge.id).join(',')}`
})
const generatedFingerprint = ref<string | null>(null)
// The generated code no longer describes the canvas. A banner, not an
// auto-regenerate: regenerating on its own would fight draft preservation.
const flowChanged = computed(
  () =>
    props.isVisible &&
    generatedFingerprint.value !== null &&
    flowFingerprint.value !== generatedFingerprint.value
)

const bodyEl = ref<HTMLElement | null>(null)
const workbenchEl = ref<HTMLElement | null>(null)
const benchEl = ref<HTMLElement | null>(null)
const railEl = ref<HTMLElement | null>(null)
const marginEl = ref<InstanceType<typeof StepMargin> | null>(null)
const chipRefs: (HTMLElement | null)[] = []
const setChipRef = (element: unknown, index: number) => {
  chipRefs[index] = (element as HTMLElement | null) ?? null
}

function initialTab(): MarginTab {
  const saved = localStorage.getItem(MARGIN_TAB_KEY)
  if (saved === 'why') return learning.showBackground ? 'why' : 'data'
  if (saved === 'data' || saved === 'output') return saved
  return 'data'
}
const marginTab = ref<MarginTab>(initialTab())
watch(marginTab, tab => localStorage.setItem(MARGIN_TAB_KEY, tab))

const toggleBackground = () => {
  learning.toggleBackground()
  if (learning.showBackground) marginTab.value = 'why'
  else if (marginTab.value === 'why') marginTab.value = 'data'
}

const split = reactive({ ...DEFAULT_SPLIT })
try {
  const saved = JSON.parse(localStorage.getItem(SPLIT_KEY) ?? 'null')
  if (saved && typeof saved.x === 'number') split.x = Math.min(85, Math.max(20, saved.x))
  if (saved && typeof saved.y === 'number') split.y = Math.min(85, Math.max(20, saved.y))
} catch {
  /* a corrupt ratio is not worth a broken panel */
}
const persistSplit = () => localStorage.setItem(SPLIT_KEY, JSON.stringify({ x: split.x, y: split.y }))

const isWide = ref(false)

const setMode = (next: CodeMode) => {
  if (mode.value === next) return
  // Whatever they typed is often an exercise solution; losing it on a tab
  // click is the one unforgivable thing this panel could do.
  if (isWalkthrough.value && edited.value) stashDraft()
  mode.value = next
  localStorage.setItem(MODE_KEY, next)
  runResult.value = null
  comparison.value = null
  generateCodeFromFlow()
}

const setFlavour = (next: WalkthroughFlavour) => {
  if (flavour.value === next || loading.value) return
  const keepNodeId = steps.value[stepIndex.value]?.nodeId
  // Stash BEFORE flipping: the draft computeds route by the current flavour.
  if (edited.value) stashDraft()
  flavour.value = next
  localStorage.setItem(FLAVOUR_KEY, next)
  // Instantly consistent buffer for the async build window that follows. The
  // old highlight anchors would map through the doc swap into garbage, so
  // clear them now; seedNow reseeds once the build lands.
  code.value = draft.value ?? generated.value
  if (editorView) {
    seedStepsIn(editorView, [], -1)
    syncReadouts()
  }
  runResult.value = null
  comparison.value = null
  generateCodeFromFlow({ preserveStepNodeId: keepNodeId })
}

const resetEdits = () => {
  clearDraft()
  code.value = generated.value
  runResult.value = null
  comparison.value = null
  scriptRevision.value++
}

/** The panel's own opt-in: turn Learning mode on and land on the walkthrough. */
const enableLearning = () => {
  if (!learning.enabled) learning.toggle()
  setMode('walkthrough')
}

// Learning mode can also be flipped from the icon rail while the panel is
// open; follow it, so the toggle always has a visible effect.
watch(
  () => learning.enabled,
  enabled => {
    if (!props.isVisible || !props.teachingMode) return
    setMode(enabled ? 'walkthrough' : 'polars')
  }
)

let editorView: EditorView | null = null
const labelLines = ref<StepRange | null>(null)
const highlightOffScreen = ref(false)

const syncReadouts = () => {
  const view = editorView
  if (!view || !isWalkthrough.value) {
    labelLines.value = null
    highlightOffScreen.value = false
    return
  }
  labelLines.value = stepLines(view.state)
  highlightOffScreen.value = !stepVisible(view)
}

let scrollRaf = 0
const onEditorScroll = () => {
  cancelAnimationFrame(scrollRaf)
  scrollRaf = requestAnimationFrame(syncReadouts)
}

const onEditorReady = (payload: { view: EditorView }) => {
  editorView = payload.view
  payload.view.scrollDOM.addEventListener('scroll', onEditorScroll, { passive: true })
  void seedNow()
}

const releaseEditor = () => {
  // The field's edit-mapped anchors die with the view (minimize unmounts the
  // body); keep them so the remount can reseed an edited buffer correctly.
  if (isWalkthrough.value && edited.value && editorView) {
    draftRanges.value = allStepLines(editorView.state)
  }
  editorView?.scrollDOM.removeEventListener('scroll', onEditorScroll)
  editorView = null
}

const extensions = [
  python(),
  oneDark,
  glossaryTooltip(),
  stepHighlight(),
  EditorView.updateListener.of(update => {
    // geometryChanged subsumes docChanged; focus-only updates are skipped.
    if (update.geometryChanged || update.transactions.length > 0) syncReadouts()
  }),
  EditorView.theme({
    '&': { height: '100%' },
    '.cm-scroller': { overflow: 'auto', overscrollBehavior: 'contain' },
    '.cm-content': { padding: '16px' },
    '.cm-focused': { outline: 'none' }
  }),
  EditorView.lineWrapping
]

/** Re-derive the anchors from the generator's line numbers after a doc swap. */
const seedNow = async () => {
  await nextTick()
  const view = editorView
  if (!view) return
  if (!isWalkthrough.value) {
    seedStepsIn(view, [], -1)
    syncReadouts()
    return
  }
  // vue-codemirror pushes model-value on its own tick.
  if (view.state.doc.length !== code.value.length) await nextTick()
  // An edited buffer must be seeded from the ranges the field last mapped for
  // it, not from the generator's line numbers — those describe a different doc.
  const ranges =
    edited.value && draftRanges.value && draftRanges.value.length === steps.value.length
      ? draftRanges.value
      : steps.value.map(step => ({ from: step.lineStart, to: step.lineEnd }))
  seedStepsIn(view, ranges, stepIndex.value)
  showStep(view, stepIndex.value)
  syncReadouts()
}

watch(scriptRevision, seedNow, { flush: 'post' })

const goTo = (index: number) => {
  stepIndex.value = Math.max(0, Math.min(steps.value.length - 1, index))
}

const recentre = () => {
  if (editorView) showStep(editorView, stepIndex.value)
}

watch(stepIndex, index => {
  const chip = chipRefs[index]
  const rail = railEl.value
  // scrollIntoView would drag ancestors along with it; this only moves the rail.
  if (chip && rail) {
    rail.scrollTo({ left: chip.offsetLeft - (rail.clientWidth - chip.offsetWidth) / 2, behavior: 'smooth' })
  }
  if (editorView) showStep(editorView, index)
  marginEl.value?.resetScroll()
})

// Keep the canvas in step: the node being explained is selected AND panned
// into view behind the docked panel. selectNode (not a bare assignment) keeps
// the store's preview memory management and the Table panel's gated fetch
// working; it never executes anything.
const syncCanvasToStep = () => {
  const nodeId = steps.value[stepIndex.value]?.nodeId
  if (nodeId === undefined || !props.isVisible || !isWalkthrough.value) return
  // A step can outlive its node until the next refresh regenerates the script.
  if (!flowStore.nodes.has(nodeId)) return
  flowStore.selectNode(nodeId)
  emit('focus-node', nodeId)
}
// The watcher alone is not enough: reopening the panel rebuilds the steps but
// often lands on the SAME nodeId, which a value-keyed watcher ignores —
// generateCodeFromFlow calls syncCanvasToStep explicitly for that case.
watch(() => steps.value[stepIndex.value]?.nodeId, syncCanvasToStep, { immediate: true })

let resizeRaf = 0
const observer =
  typeof ResizeObserver === 'undefined'
    ? null
    : new ResizeObserver(entries => {
        for (const entry of entries) {
          if (entry.target === bodyEl.value) isWide.value = entry.contentRect.width >= WIDE_AT
        }
        cancelAnimationFrame(resizeRaf)
        resizeRaf = requestAnimationFrame(() => {
          // CodeMirror caches geometry, so a resized panel renders stale until asked.
          editorView?.requestMeasure()
          if (editorView && isWalkthrough.value) showStep(editorView, stepIndex.value)
          syncReadouts()
        })
      })

watch([bodyEl, benchEl], ([body, bench]) => {
  // Minimizing unmounts the body, taking CodeMirror with it.
  if (!body) releaseEditor()
  if (!observer) return
  observer.disconnect()
  if (body) observer.observe(body)
  if (bench) observer.observe(bench)
})

const startDrag = (event: PointerEvent) => {
  const element = workbenchEl.value
  if (!element) return
  const box = element.getBoundingClientRect()
  const wide = isWide.value
  ;(event.target as Element).setPointerCapture(event.pointerId)
  const move = (moved: PointerEvent) => {
    // Clamp in px, not percent: that is what holds the floors on a short window.
    if (wide) {
      const px = Math.min(box.width - 8 - FLOORS.marginX, Math.max(FLOORS.benchX, moved.clientX - box.left))
      split.x = (px / box.width) * 100
    } else {
      const px = Math.min(box.height - 8 - FLOORS.margin, Math.max(FLOORS.bench, moved.clientY - box.top))
      split.y = (px / box.height) * 100
    }
  }
  const up = () => {
    window.removeEventListener('pointermove', move)
    persistSplit()
  }
  window.addEventListener('pointermove', move)
  window.addEventListener('pointerup', up, { once: true })
}

const resetSplit = () => {
  split.x = DEFAULT_SPLIT.x
  split.y = DEFAULT_SPLIT.y
  persistSplit()
}

const nudgeSplit = (event: KeyboardEvent) => {
  if (event.key === 'Home') {
    event.preventDefault()
    resetSplit()
    return
  }
  const axis = isWide.value ? 'x' : 'y'
  const keys: Record<string, number> = isWide.value
    ? { ArrowLeft: -1, ArrowRight: 1 }
    : { ArrowUp: -1, ArrowDown: 1 }
  const direction = keys[event.key]
  if (!direction) return
  event.preventDefault()
  split[axis] = Math.min(85, Math.max(20, split[axis] + direction * (event.shiftKey ? 5 : 2)))
  persistSplit()
}

const generateCodeFromFlow = async (options?: { preserveStepNodeId?: number }) => {
  loading.value = true
  error.value = null
  buildEpoch.value++

  try {
    if (isWalkthrough.value) {
      // The captured tables belong to the previous script's steps — and the
      // two flavours bind the SAME variable names by design, so this reset
      // must happen before the builder: a throwing build may never reach it.
      captured.value = null
      if (flavour.value === 'polars') {
        const formulaCode = await translateFormulaNodes(flowStore.nodes)
        walkthrough.value = buildPolarsWalkthrough({
          nodes: flowStore.nodes,
          edges: flowStore.edges,
          flowName: 'WASM Flow',
          formulaCode
        })
      } else {
        // One conversion: buildWalkthrough's script is what generatePlainPython
        // would have produced, so the editable buffer and the steps agree.
        walkthrough.value = buildWalkthrough({
          nodes: flowStore.nodes,
          edges: flowStore.edges,
          flowName: 'WASM Flow'
        })
      }
      generated.value = walkthrough.value.script
      code.value = draft.value ?? generated.value
      const keep =
        options?.preserveStepNodeId !== undefined
          ? walkthrough.value.steps.findIndex(step => step.nodeId === options.preserveStepNodeId)
          : -1
      stepIndex.value =
        keep >= 0 ? keep : Math.min(stepIndex.value, Math.max(0, walkthrough.value.steps.length - 1))
      scriptRevision.value++
      syncCanvasToStep()
      return
    }
    const formulaCode = await translateFormulaNodes(flowStore.nodes)
    code.value = generateCode({
      nodes: flowStore.nodes,
      edges: flowStore.edges,
      flowName: 'WASM Flow',
      formulaCode
    })
    scriptRevision.value++
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Failed to generate code'
    code.value = '# Failed to generate code. Please check your flow configuration.'
    if (isWalkthrough.value) {
      // Stale steps must not ghost across flavours: chips, canvas sync, trace
      // captures and the compare target would all describe the other script.
      walkthrough.value = { script: '', steps: [] }
      // Keeps `edited` false, so the placeholder can never be stashed over a
      // real draft — the failed flavour's stash survives for the next success.
      generated.value = code.value
      stepIndex.value = 0
      scriptRevision.value++
    }
  } finally {
    generatedFingerprint.value = flowFingerprint.value
    loading.value = false
  }
}

/**
 * Run the buffer in Pyodide, in its own namespace.
 *
 * This deliberately avoids the engine entirely — no `execute_*` bridge call, no
 * `_lazyframes` entry — so it cannot disturb canvas state. It is a run in the
 * explicit-run-only sense: it only ever happens on this button.
 */
const runScript = async () => {
  if (!pyodideStore.isReady || running.value || loading.value) return
  const epoch = buildEpoch.value
  running.value = true
  runResult.value = null
  // Otherwise last run's verdict sits next to this run's result and contradicts it.
  comparison.value = null
  marginTab.value = 'output'
  try {
    await ensureScriptPackages(code.value)
    // Same length per line, so traceback line numbers still match the editor.
    let source = code.value
    for (const [url, name] of stageReadFiles()) source = source.replaceAll(`"${url}"`, `"${name}"`)
    pyodideStore.setGlobal('_plain_script', source)
    const rows = await pyodideStore.runPythonWithResult(
      [
        'import json, traceback',
        'import polars as pl',
        '_plain_ns = {}',
        // Type-aware: the plain flavour returns list[dict], the Polars flavour
        // a LazyFrame — one harness serves both.
        'def _plain_rows(value):',
        '    if isinstance(value, pl.LazyFrame):',
        '        value = value.collect()',
        '    if isinstance(value, pl.DataFrame):',
        '        return value.to_dicts()',
        '    return value',
        // The wasm Polars build cannot run its file IO here: a file-backed
        // scan_csv aborts the runtime at collect, and a sink cannot spawn its
        // worker thread. Bridge both through Python IO exactly like the canvas
        // engine does (read_csv(StringIO(...)).lazy(); collect + buffer write).
        // Patched only around this run, then restored.
        'import io',
        '_orig_scan_csv = pl.scan_csv',
        'def _scan_csv_no_file_io(source, *args, **kwargs):',
        '    if not isinstance(source, str):',
        '        return _orig_scan_csv(source, *args, **kwargs)',
        '    with open(source, encoding=kwargs.pop("encoding", "utf-8")) as _handle:',
        '        _text = _handle.read()',
        '    return pl.read_csv(io.StringIO(_text), *args, **kwargs).lazy()',
        'def _sink_csv_no_threads(self, path, *args, **kwargs):',
        '    _buffer = io.BytesIO()',
        '    self.collect().write_csv(_buffer, **kwargs)',
        '    with open(path, "wb") as _handle:',
        '        _handle.write(_buffer.getvalue())',
        '_orig_sink_csv = pl.LazyFrame.sink_csv',
        'pl.scan_csv = _scan_csv_no_file_io',
        'pl.LazyFrame.sink_csv = _sink_csv_no_threads',
        'try:',
        // compile() with a filename so traceback line numbers match the editor.
        '    exec(compile(_plain_script, "pipeline.py", "exec"), _plain_ns)',
        '    _plain_out = {"ok": True, "rows": json.dumps(_plain_rows(_plain_ns["run_etl_pipeline"]()), default=str)}',
        'except Exception:',
        '    _plain_out = {"ok": False, "error": traceback.format_exc()}',
        'finally:',
        '    pl.scan_csv = _orig_scan_csv',
        '    pl.LazyFrame.sink_csv = _orig_sink_csv',
        '_plain_out'
      ].join('\n')
    )
    // A regeneration happened mid-run: this result describes the old script.
    if (epoch !== buildEpoch.value) return
    runResult.value = rows?.ok
      ? { rows: JSON.parse(rows.rows), failed: false }
      : { rows: [], failed: true, error: cleanTraceback(rows?.error ?? 'Unknown error') }
  } catch (err) {
    if (epoch !== buildEpoch.value) return
    runResult.value = { rows: [], failed: true, error: err instanceof Error ? err.message : String(err) }
  } finally {
    pyodideStore.deleteGlobal('_plain_script')
    running.value = false
  }
}

/**
 * Drop the frames belonging to the harness that exec'd the script.
 *
 * `File "<exec>", line 5, in <module>` is this component's own wrapper and
 * means nothing to someone reading their own pipeline.
 */
const cleanTraceback = (text: string): string => {
  const lines = text.split('\n')
  const kept = lines.filter((line, index) => {
    if (!line.includes('File "<exec>"')) return true
    // Drop the following source-echo line too, if there is one.
    lines[index + 1] = lines[index + 1]?.trimStart().startsWith('exec(') ? '' : lines[index + 1]
    return false
  })
  return kept.filter(line => line !== '').join('\n')
}

/**
 * Check the script's own output against what the canvas already produced —
 * by VALUES, not just row count, so a wrong answer with the right cardinality
 * cannot pass. The target is the node whose table the script returns: the last
 * walkthrough step, not whichever terminal node happens to iterate first.
 *
 * Reads an existing result only — running the flow is the user's call, so this
 * never reaches an execute_* bridge (see the no-auto-run contract).
 */
const compareToCanvas = async () => {
  comparison.value = null
  const rows = runResult.value?.rows
  if (!rows) return

  const targetId = steps.value.length > 0 ? steps.value[steps.value.length - 1].nodeId : null
  const result = targetId === null ? undefined : flowStore.nodeResults.get(targetId)
  if (targetId === null || !result?.success) {
    comparison.value = { status: 'info', message: 'Run the flow first, then compare.' }
    return
  }

  const preview = await flowStore.fetchNodePreview(targetId).catch(() => null)
  const data = preview?.data ?? result.data
  if (!data?.columns || data.total_rows === null || data.total_rows === undefined) {
    comparison.value = { status: 'info', message: 'No canvas result to compare against yet.' }
    return
  }
  comparison.value = compareTables(rows, {
    columns: data.columns,
    rows: data.data ?? [],
    totalRows: data.total_rows
  })
}

/** The buffer may need micropip packages Pyodide does not preload. */
const ensureScriptPackages = async (script: string) => {
  if (script.includes('polars_expr_transformer')) {
    await pyodideStore.ensurePyPackages([EXPR_TRANSFORMER_PACKAGE])
  }
  // Same pin the engine installs for the Excel writer node.
  if (script.includes('write_excel')) {
    await pyodideStore.ensurePyPackages(['XlsxWriter==3.2.0'])
  }
}

/**
 * Put each Read File node's text content where the generated script looks for
 * it, and return quoted-path substitutions for URL-sourced reads: the wasm
 * Polars build has no `cloud` feature, so scan_csv("https://…") panics — the
 * in-browser run executes against the staged copy instead. The buffer itself
 * is never rewritten; the exported script keeps reading from the URL.
 */
const stageReadFiles = (): Array<[string, string]> => {
  const substitutions: Array<[string, string]> = []
  const fs = pyodideStore.pyodide?.FS
  if (!fs) return substitutions
  for (const node of flowStore.nodes.values()) {
    if (node.type !== 'read') continue
    const content = flowStore.getFileContent(node.id)
    if (content?.kind !== 'text') continue
    const settings = node.settings as NodeReadSettings
    const name = settings.file_name || settings.received_file?.name || 'data.csv'
    try {
      fs.writeFile(name, content.data)
    } catch {
      // A path the virtual FS will not take just surfaces as a Python error below.
      continue
    }
    const path = settings.received_file?.path
    if (path && /^https?:\/\//i.test(path)) substitutions.push([path, name])
  }
  return substitutions
}

const refreshCode = () => {
  // Regenerate from the flow — but never over the learner's typing: an
  // in-progress exercise solution survives as the draft, and the explicit
  // Reset button stays the only path that discards it. Refreshing from the
  // Polars tab must not touch a draft stashed by an earlier tab switch, and
  // after a FAILED build the buffer is a placeholder (edited=false by design)
  // — clearing then would destroy the draft the failed attempt had stashed.
  if (isWalkthrough.value) {
    if (edited.value) stashDraft()
    else if (!error.value) clearDraft()
  }
  generateCodeFromFlow()
}

const exportCode = () => {
  const blob = new Blob([code.value], { type: 'text/plain' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = isWalkthrough.value
    ? flavour.value === 'polars'
      ? 'pipeline_polars.py'
      : 'pipeline_plain_python.py'
    : 'flowfile_pipeline.py'
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

const closePanel = () => {
  emit('close')
}

watch(
  () => props.isVisible,
  isVisible => {
    if (!isVisible) return
    // Edits typed before the close survived in `code`; stash them before the
    // rebuild overwrites the buffer (the ranges were stashed by releaseEditor).
    if (isWalkthrough.value && edited.value) stashDraft()
    // Resolve the landing mode on open, so flipping Learning mode while the
    // panel is closed takes effect the next time it opens.
    mode.value = props.teachingMode ? initialMode() : 'polars'
    generateCodeFromFlow()
  }
)

onBeforeUnmount(() => {
  observer?.disconnect()
  cancelAnimationFrame(resizeRaf)
  cancelAnimationFrame(scrollRaf)
  releaseEditor()
})
</script>

<style scoped>
/* A flex column that never scrolls: the editor and the margin body own the
   only two scrollers in here. Geometry (dock, size, fullscreen) belongs to
   DraggableItem — see layoutGeometry.ts. */
.code-body {
  flex: 1 1 auto;
  min-height: 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  background: var(--color-background-primary);
}

.error-message,
.flow-changed,
.step-rail {
  flex: 0 0 auto;
}

/* Informational, not alarming: the code is still valid, just for the old flow. */
.flow-changed {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 6px 14px;
  font-size: 12px;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.flow-changed-refresh {
  margin-left: auto;
  padding: 2px 12px;
  border: 1px solid var(--color-accent);
  border-radius: 999px;
  background: transparent;
  color: var(--color-accent);
  font-size: 11.5px;
  cursor: pointer;
}

.flow-changed-refresh:hover {
  background: var(--color-accent);
  color: #fff;
}

/* Constant 44px for any number of steps: chips scroll sideways rather than
   wrapping, which is what stops a step change from resizing the editor. */
.step-rail {
  display: flex;
  align-items: center;
  gap: 8px;
  height: 44px;
  padding: 0 12px;
  overflow: hidden;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.rail-track {
  flex: 1 1 auto;
  min-width: 0;
  display: flex;
  flex-wrap: nowrap;
  gap: 6px;
  overflow-x: auto;
  overflow-y: hidden;
  scroll-behavior: smooth;
  scrollbar-width: thin;
}

.chip {
  flex: 0 0 auto;
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
.chip-index {
  opacity: 0.65;
  font-variant-numeric: tabular-nums;
}

.chip-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.rail-arrow {
  flex: 0 0 auto;
  width: 22px;
  height: 22px;
  padding: 0;
  border: 1px solid var(--color-border-primary);
  border-radius: 50%;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 13px;
  line-height: 1;
  cursor: pointer;
}

.rail-arrow:hover:not(:disabled) {
  border-color: var(--color-accent);
  color: var(--color-text-primary);
}

.rail-arrow:disabled {
  opacity: 0.35;
  cursor: not-allowed;
}

.rail-count {
  flex: 0 0 auto;
  font-variant-numeric: tabular-nums;
  font-size: 12px;
  color: var(--color-text-secondary);
}

.empty-state {
  flex: 1 1 auto;
  padding: 32px 24px;
  font-size: 13px;
  line-height: 1.6;
  color: var(--color-text-secondary);
}

/* The split container. Never scrolls; min-height: 0 is what lets it shrink. */
.workbench {
  flex: 1 1 auto;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.bench {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-height: 200px;
  flex: 0 0 var(--split-y, 64%);
}

/* Polars tab: no margin, so the bench is everything. */
.workbench:not(.split) .bench {
  flex: 1 1 auto;
}

.bench-head {
  flex: 0 0 auto;
  display: flex;
  align-items: baseline;
  gap: 10px;
  height: 26px;
  padding: 0 14px;
  overflow: hidden;
  font-size: 11px;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.bench-label {
  flex: 0 0 auto;
  min-width: 0;
  font-weight: 600;
  letter-spacing: 0.3px;
  text-transform: uppercase;
}

.bench-recentre {
  flex: 0 0 auto;
  padding: 1px 8px;
  border: 1px solid var(--color-accent);
  border-radius: 999px;
  background: transparent;
  color: var(--color-accent);
  font-size: 10.5px;
  cursor: pointer;
}

.flavour-toggle {
  flex: 0 0 auto;
  align-self: center;
  display: inline-flex;
  border: 1px solid var(--color-border-primary);
  border-radius: 999px;
  overflow: hidden;
}

.flavour-toggle button {
  padding: 1px 9px;
  border: 0;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 10.5px;
  letter-spacing: 0.2px;
  cursor: pointer;
  white-space: nowrap;
}

.flavour-toggle button.active {
  background: var(--color-accent);
  color: #fff;
}

.flavour-toggle button:disabled {
  cursor: default;
  opacity: 0.6;
}

.bench-editor {
  flex: 1 1 auto;
  min-height: 0;
  overflow: hidden;
  background: var(--color-code-bg);
}

/* Height, not max-height: the editor's box comes from the split, never from
   its content, so CodeMirror always measures a settled box. */
.bench-editor :deep(.cm-editor) {
  height: 100%;
}

/* SCROLLER #1. */
.bench-editor :deep(.cm-scroller) {
  overflow: auto;
}

.split-divider {
  flex: 0 0 8px;
  cursor: row-resize;
  touch-action: none;
  background: var(--color-background-secondary);
  border-block: 1px solid var(--color-border-primary);
}

.split-divider:hover,
.split-divider:focus-visible {
  background: var(--color-accent);
  outline: none;
}

/* Wide: columns. Driven by a Vue-bound class, not @container — container-type
   would make this a containing block for the glossary's fixed tooltips.
   Scoped to .split: without a margin beside it the bench must fill the panel. */
.workbench.split.is-wide {
  flex-direction: row;
}

.workbench.split.is-wide .bench {
  flex: 0 0 var(--split-x, 60%);
  min-width: 460px;
  min-height: 0;
}

.workbench.split.is-wide .split-divider {
  cursor: col-resize;
  border-block: none;
  border-inline: 1px solid var(--color-border-primary);
}

.workbench.split.is-wide :deep(.margin) {
  min-width: 340px;
  min-height: 0;
  border-left: 1px solid var(--color-border-primary);
}

.run-button:hover:not(:disabled) {
  background: var(--color-success, #2e9e5b) !important;
  border-color: var(--color-success, #2e9e5b) !important;
  color: #fff;
}

/* Sized for the 35px DraggableItem header. */
.icon-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.icon-button:hover:not(:disabled) {
  background: var(--color-accent);
  border-color: var(--color-accent);
}

.icon-button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.error-message {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 20px;
  background: var(--color-danger-light);
  border-bottom: 1px solid var(--color-danger);
  color: var(--color-danger);
  font-size: 14px;
}

.error-message svg {
  flex-shrink: 0;
}

.spinner {
  display: inline-block;
  width: 14px;
  height: 14px;
  border: 2px solid currentColor;
  border-radius: 50%;
  border-top-color: transparent;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
