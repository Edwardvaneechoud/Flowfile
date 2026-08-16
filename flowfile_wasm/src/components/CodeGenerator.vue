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
        title="Learning Python? Rebuild this flow as plain Python, step by step"
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
        :disabled="running || !pyodideStore.isReady"
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
            :class="{ current: index === stepIndex, exercise: step.concept === 'exercise' }"
            :title="stepLabel(step)"
            @click="goTo(index)"
          >
            <span class="chip-index">{{ index + 1 }}</span>
            <span class="chip-name">{{ stepLabel(step) }}</span>
            <span v-if="step.concept === 'exercise'" class="chip-exercise" title="Left as an exercise">✎</span>
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

      <div v-if="isWalkthrough && !steps.length" class="empty-state">
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
            <span class="bench-label">
              step {{ stepIndex + 1 }}<template v-if="labelLines">
                · lines {{ labelLines.from }}–{{ labelLines.to }}</template>
            </span>
            <button v-if="highlightOffScreen" class="bench-recentre" @click="recentre">
              ↩ step {{ stepIndex + 1 }}
            </button>
            <span class="bench-hint">{{ glossaryHint }}</span>
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
          :tables="tables"
          :delta="delta"
          :delta-counts="deltaCounts"
          :blocked-reason="blockedReason"
          :data-state="traceState"
          :can-run="pyodideStore.isReady"
          :stale="traceStale"
          :trace-error="visibleTraceError"
          :show-background="learning.showBackground"
          :run-result="runResult"
          :comparison="comparison"
          @trace="runTrace"
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
import { usePlainPythonGeneration } from '../composables/usePlainPythonGeneration'
import type { PlainWalkthrough } from '../composables/usePlainPythonGeneration'
import { usePlainStep, stepLabel, type PlainRunResult, type CapturedTrace } from '../composables/usePlainStep'
import { COARSE_POINTER, glossaryTooltip } from '../composables/usePythonGlossary'
import {
  compareTables,
  instrumentScript,
  type CompareResult,
  type StepCapture
} from '../composables/usePlainTrace'
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
import type { NodeFormulaSettings, NodeReadSettings } from '../types'

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
const MODE_KEY = 'flowfile-codegen-mode'
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

const code = ref('')
const loading = ref(false)
const error = ref<string | null>(null)
const mode = ref<CodeMode>('polars')
const running = ref(false)
const runResult = ref<PlainRunResult | null>(null)
const comparison = ref<CompareResult | null>(null)
const glossaryHint = COARSE_POINTER ? 'tap any name for what it does' : 'hover any name for what it does'

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
          title: 'Step through the flow one node at a time, in plain Python — no dataframe library'
        }
      ]
    : []
)

// Editing the generated script is the point in walkthrough mode, so track
// whether the buffer still matches what we generated — that is what "reset" undoes.
const generated = ref('')
// Survives a trip to another tab; cleared by Reset only.
const draft = ref<string | null>(null)
// The step ranges as the highlight field last mapped them through the edits.
// Stashed with the draft, because reseeding a draft from the generator's
// pristine line numbers would misplace every highlight below an edit.
const draftRanges = ref<StepRange[] | null>(null)
const edited = computed(() => isWalkthrough.value && code.value !== generated.value)

const stashDraft = () => {
  draft.value = code.value
  if (editorView) draftRanges.value = allStepLines(editorView.state)
}
const clearDraft = () => {
  draft.value = null
  draftRanges.value = null
}

const walkthrough = ref<PlainWalkthrough>({ script: '', traceScript: '', steps: [], snippets: {} })
const steps = computed(() => walkthrough.value.steps)
const stepIndex = ref(0)
const captured = ref<CapturedTrace | null>(null)
const traceState = ref<'idle' | 'running' | 'ready' | 'failed'>('idle')
// The exact buffer text the last trace instrumented — staleness is "buffer ≠ this".
const tracedCode = ref<string | null>(null)
const traceError = ref<string | null>(null)
// Bumped whenever the document is replaced programmatically; drives re-seeding.
const scriptRevision = ref(0)

const traceStale = computed(
  () => traceState.value === 'ready' && tracedCode.value !== null && code.value !== tracedCode.value
)
// A raising exercise stub is the expected way a trace ends — not an error to show.
const visibleTraceError = computed(() =>
  traceError.value && !traceError.value.includes('NotImplementedError') ? traceError.value : null
)

const { concept, tables, delta, deltaCounts, blockedReason } = usePlainStep(steps, stepIndex, captured)

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
    if (update.docChanged || update.geometryChanged || update.transactions.length > 0) syncReadouts()
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

// Translate each formula node's expression to Polars code (to_polars_code).
// Best-effort: the handler falls back to runtime translation on failure.
const translateFormulaNodes = async (): Promise<Record<number, string>> => {
  const out: Record<number, string> = {}
  const formulaNodes = [...flowStore.nodes.values()].filter(n => n.type === 'formula')
  if (formulaNodes.length === 0 || !pyodideStore.isReady) return out
  try {
    await pyodideStore.ensurePyPackages(['polars-expr-transformer==0.5.6'])
    for (const node of formulaNodes) {
      const expr = (node.settings as NodeFormulaSettings)?.function?.function?.trim()
      if (!expr) continue
      const polars = await pyodideStore.runPythonWithResult(
        'import json\n' +
          'from polars_expr_transformer.process.polars_expr_transformer import to_polars_code\n' +
          `to_polars_code(json.loads(${JSON.stringify(JSON.stringify(expr))}))`
      )
      if (typeof polars === 'string' && polars.trim()) out[node.id] = polars.trim()
    }
  } catch {
    // handler falls back to runtime translation
  }
  return out
}

const generateCodeFromFlow = async () => {
  loading.value = true
  error.value = null

  try {
    if (isWalkthrough.value) {
      // One conversion: buildWalkthrough's script is what generatePlainPython
      // would have produced, so the editable buffer and the steps agree.
      walkthrough.value = buildWalkthrough({
        nodes: flowStore.nodes,
        edges: flowStore.edges,
        flowName: 'WASM Flow'
      })
      generated.value = walkthrough.value.script
      code.value = draft.value ?? generated.value
      stepIndex.value = Math.min(stepIndex.value, Math.max(0, walkthrough.value.steps.length - 1))
      // The captured tables belong to the previous shape of the flow.
      captured.value = null
      traceState.value = 'idle'
      tracedCode.value = null
      traceError.value = null
      scriptRevision.value++
      syncCanvasToStep()
      return
    }
    const formulaCode = await translateFormulaNodes()
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
  if (!pyodideStore.isReady || running.value) return
  running.value = true
  runResult.value = null
  // Otherwise last run's verdict sits next to this run's result and contradicts it.
  comparison.value = null
  marginTab.value = 'output'
  try {
    stageReadFiles()
    pyodideStore.setGlobal('_plain_script', code.value)
    const rows = await pyodideStore.runPythonWithResult(
      [
        'import json, traceback',
        '_plain_ns = {}',
        'try:',
        // compile() with a filename so traceback line numbers match the editor.
        '    exec(compile(_plain_script, "pipeline.py", "exec"), _plain_ns)',
        '    _plain_out = {"ok": True, "rows": json.dumps(_plain_ns["run_etl_pipeline"](), default=str)}',
        'except Exception:',
        '    _plain_out = {"ok": False, "error": traceback.format_exc()}',
        '_plain_out'
      ].join('\n')
    )
    runResult.value = rows?.ok
      ? { rows: JSON.parse(rows.rows), failed: false }
      : { rows: [], failed: true, error: cleanTraceback(rows?.error ?? 'Unknown error') }
  } catch (err) {
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

/** How many rows of each intermediate table cross the JS bridge. The counts stay exact. */
const TRACE_ROW_CAP = 50

/**
 * Instrument the buffer AS IT IS and keep every intermediate table.
 *
 * The captures are inserted into the live text at the step ranges the
 * highlight has been mapping through the learner's edits — so a solved
 * exercise gets per-step data, not just the generated script. `__steps__` is
 * pre-seeded in the exec namespace, which is what keeps everything captured
 * before a raising exercise stub readable; the raise itself is expected, and
 * any other exception is kept to show.
 */
const runTrace = async () => {
  if (!pyodideStore.isReady || traceState.value === 'running') return
  const view = editorView
  if (!view) return
  traceState.value = 'running'
  marginTab.value = 'data'
  const source = code.value
  const ranges = allStepLines(view.state)
  const captures: StepCapture[] = steps.value
    .map((step, index) => ({
      fromLine: ranges[index]?.from ?? 0,
      toLine: ranges[index]?.to ?? 0,
      varName: step.varName
    }))
    .filter(capture => capture.toLine > 0)
  try {
    stageReadFiles()
    pyodideStore.setGlobal('_trace_script', instrumentScript(source, captures))
    const result = await pyodideStore.runPythonWithResult(
      [
        'import json, traceback',
        '_trace_ns = {"__steps__": {}}',
        '_trace_error = None',
        'try:',
        '    exec(compile(_trace_script, "pipeline.py", "exec"), _trace_ns)',
        '    _trace_ns["run_etl_pipeline"]()',
        'except Exception:',
        '    _trace_error = traceback.format_exc()',
        '_tables = {k: v for k, v in _trace_ns.get("__steps__", {}).items() if isinstance(v, list)}',
        'json.dumps({',
        `    "tables": {k: v[:${TRACE_ROW_CAP}] for k, v in _tables.items()},`,
        '    "counts": {k: len(v) for k, v in _tables.items()},',
        '    "error": _trace_error,',
        '}, default=str)'
      ].join('\n')
    )
    const parsed = JSON.parse(result)
    captured.value = { tables: parsed.tables ?? {}, counts: parsed.counts ?? {} }
    traceError.value = parsed.error ? cleanTraceback(parsed.error) : null
    tracedCode.value = source
    traceState.value = Object.keys(captured.value.tables).length > 0 ? 'ready' : 'failed'
  } catch (err) {
    captured.value = null
    traceError.value = err instanceof Error ? err.message : String(err)
    tracedCode.value = source
    traceState.value = 'failed'
  } finally {
    pyodideStore.deleteGlobal('_trace_script')
  }
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

/** Put each Read File node's text content where the generated script looks for it. */
const stageReadFiles = () => {
  const fs = pyodideStore.pyodide?.FS
  if (!fs) return
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
    }
  }
}

const refreshCode = () => {
  // Regenerate from the flow — but never over the learner's typing: an
  // in-progress exercise solution survives as the draft, and the explicit
  // Reset button stays the only path that discards it. Refreshing from the
  // Polars tab must not touch a draft stashed by an earlier tab switch.
  if (isWalkthrough.value) {
    if (edited.value) stashDraft()
    else clearDraft()
  }
  generateCodeFromFlow()
}

const exportCode = () => {
  const blob = new Blob([code.value], { type: 'text/plain' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = isWalkthrough.value ? 'pipeline_plain_python.py' : 'flowfile_pipeline.py'
  document.body.appendChild(a)
  a.click()
  document.body.removeChild(a)
  URL.revokeObjectURL(url)
}

const closePanel = () => {
  emit('close')
}

const teachingModeAvailable = computed(() => props.teachingMode)

watch(
  () => props.isVisible,
  isVisible => {
    if (!isVisible) return
    // Resolve the landing mode on open, so flipping Learning mode while the
    // panel is closed takes effect the next time it opens.
    mode.value = teachingModeAvailable.value ? initialMode() : 'polars'
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
.chip.exercise:not(.current) {
  border-style: dashed;
}

.chip-exercise {
  font-size: 10px;
  opacity: 0.9;
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

.bench-hint {
  margin-left: auto;
  flex: 1 1 auto;
  min-width: 0;
  text-align: right;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  opacity: 0.85;
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
   would make this a containing block for the glossary's fixed tooltips. */
.workbench.is-wide {
  flex-direction: row;
}

.workbench.is-wide .bench {
  flex: 0 0 var(--split-x, 60%);
  min-width: 460px;
  min-height: 0;
}

.workbench.is-wide .split-divider {
  cursor: col-resize;
  border-block: none;
  border-inline: 1px solid var(--color-border-primary);
}

.workbench.is-wide :deep(.margin) {
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
