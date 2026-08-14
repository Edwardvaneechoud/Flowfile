<template>
  <div
    v-if="isVisible"
    class="code-generator-overlay"
    :class="{ docked: mode === 'walkthrough' }"
    :style="mode === 'walkthrough' ? { zIndex: Z_INDEX.DOCKED_PANEL } : undefined"
    @click.self="closePanel"
  >
    <div class="code-generator-panel">
      <div class="code-header">
        <h3>{{ mode === "walkthrough" ? "Walkthrough" : "Generated Python Code" }}</h3>
        <div v-if="teachingMode" class="mode-switch" role="tablist" aria-label="Code flavour">
          <button
            role="tab"
            class="mode-button"
            :class="{ active: mode === 'polars' }"
            :aria-selected="mode === 'polars'"
            title="Production code using the Polars dataframe library"
            @click="setMode('polars')"
          >
            Polars
          </button>
          <button
            role="tab"
            class="mode-button"
            :class="{ active: mode === 'plain' }"
            :aria-selected="mode === 'plain'"
            title="The same flow written with lists, dicts and for loops — no dataframe library"
            @click="setMode('plain')"
          >
            Plain Python
          </button>
          <button
            role="tab"
            class="mode-button"
            :class="{ active: mode === 'walkthrough' }"
            :aria-selected="mode === 'walkthrough'"
            title="Step through the flow one node at a time, with the data at each point"
            @click="setMode('walkthrough')"
          >
            Walkthrough
          </button>
        </div>
        <div class="header-actions">
          <button
            v-if="mode === 'plain'"
            class="icon-button run-button"
            :disabled="running || !pyodideStore.isReady"
            :title="pyodideStore.isReady ? 'Run this script here in the browser' : 'Python is still starting up'"
            @click="runScript"
          >
            <svg v-if="!running" width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
              <polygon points="6 4 20 12 6 20 6 4"></polygon>
            </svg>
            <span v-if="running" class="spinner"></span>
          </button>
          <button
            v-if="mode === 'plain' && edited"
            class="icon-button"
            title="Discard your edits and regenerate"
            @click="resetEdits"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M3 12a9 9 0 1 0 3-6.7L3 8"></path>
              <path d="M3 3v5h5"></path>
            </svg>
          </button>
          <button class="icon-button refresh-button" :disabled="loading" @click="refreshCode" title="Refresh code">
            <svg
              v-if="!loading"
              width="16"
              height="16"
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
          <button class="icon-button export-button" @click="exportCode" title="Export as .py file">
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
            >
              <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
              <polyline points="7 10 12 15 17 10"></polyline>
              <line x1="12" y1="15" x2="12" y2="3"></line>
            </svg>
          </button>
          <button class="icon-button close-button" @click="closePanel" title="Close">
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              stroke-width="2"
            >
              <line x1="18" y1="6" x2="6" y2="18"></line>
              <line x1="6" y1="6" x2="18" y2="18"></line>
            </svg>
          </button>
        </div>
      </div>

      <div v-if="error" class="error-message">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <circle cx="12" cy="12" r="10"></circle>
          <line x1="12" y1="8" x2="12" y2="12"></line>
          <line x1="12" y1="16" x2="12.01" y2="16"></line>
        </svg>
        <span>{{ error }}</span>
      </div>

      <PlainPythonWalkthrough
        v-if="mode === 'walkthrough'"
        v-model:current="stepIndex"
        :steps="walkthrough.steps"
        :script="walkthrough.script"
        :captured="captured"
        :data-state="traceState"
        :can-run="pyodideStore.isReady"
        class="walkthrough-slot"
        @trace="runTrace"
        @select="onStepSelect"
      />

      <template v-else>
        <div v-if="mode === 'plain'" class="mode-note">
          The same flow with no dataframe library — every table is a list of dicts and every node is
          a loop. Edit it if you like, then press ▶ to run it right here.
        </div>

        <div class="code-editor-container">
          <Codemirror
            v-model="code"
            :extensions="extensions"
            :disabled="mode !== 'plain'"
            :style="{ height: '100%', fontSize: '13px' }"
            @ready="onEditorReady"
          />
        </div>
      </template>

      <div v-if="mode === 'plain' && runResult" class="run-output" :class="{ failed: runResult.failed }">
        <div class="run-output-header">
          <span>{{ runResult.failed ? "The script raised" : `Returned ${runResult.rows.length} row(s)` }}</span>
          <span v-if="comparison" class="comparison" :class="{ match: comparison.match }">
            {{ comparison.message }}
          </span>
          <button
            v-if="!runResult.failed"
            class="run-output-compare"
            title="Check this against what the canvas produced"
            @click="compareToCanvas"
          >
            Compare to canvas
          </button>
          <button class="run-output-close" title="Hide output" @click="runResult = null">✕</button>
        </div>
        <pre v-if="runResult.failed" class="run-error">{{ runResult.error }}</pre>
        <table v-else-if="runResult.rows.length" class="run-table">
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
        <div v-else class="run-empty">No rows.</div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, nextTick, watch } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView } from '@codemirror/view'
import { useFlowStore } from '../stores/flow-store'
import { usePyodideStore } from '../stores/pyodide-store'
import { useCodeGeneration } from '../composables/useCodeGeneration'
import { usePlainPythonGeneration } from '../composables/usePlainPythonGeneration'
import type { PlainWalkthrough } from '../composables/usePlainPythonGeneration'
import { glossaryTooltip } from '../composables/usePythonGlossary'
import { stepHighlight } from '../composables/useStepHighlight'
import { useLearningStore } from '../stores/learning-store'
import { useDesignerUiStore } from '../stores/designer-ui-store'
import { Z_INDEX } from './common/DraggableItem/zIndex'
import PlainPythonWalkthrough from './PlainPythonWalkthrough.vue'
import type { NodeFormulaSettings, NodeReadSettings } from '../types'

const props = withDefaults(
  defineProps<{
    isVisible: boolean
    /** Offer the plain-Python teaching flavour alongside the Polars one. */
    teachingMode?: boolean
  }>(),
  { teachingMode: true }
)

const emit = defineEmits<{
  close: []
}>()

const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()
const learning = useLearningStore()
const uiStore = useDesignerUiStore()
const { generateCode } = useCodeGeneration()
const { generatePlainPython, buildWalkthrough } = usePlainPythonGeneration()

type CodeMode = 'polars' | 'plain' | 'walkthrough'
const MODE_KEY = 'flowfile-codegen-mode'

function initialMode(): CodeMode {
  const saved = localStorage.getItem(MODE_KEY)
  if (saved === 'plain' || saved === 'walkthrough' || saved === 'polars') return saved
  // Learning mode is what decides where a first-time visitor lands.
  return learning.enabled ? 'walkthrough' : 'polars'
}

const code = ref('')
const loading = ref(false)
const error = ref<string | null>(null)
const mode = ref<CodeMode>('polars')
const running = ref(false)
const runResult = ref<{ rows: Record<string, unknown>[]; failed: boolean; error?: string } | null>(null)
const comparison = ref<{ match: boolean; message: string } | null>(null)

// Editing the generated script is the point in plain mode, so track whether the
// buffer still matches what we generated — that is what "reset" undoes.
const generated = ref('')
// Survives a trip to another tab; cleared by Reset or by regenerating.
const draft = ref<string | null>(null)
const edited = computed(() => mode.value === 'plain' && code.value !== generated.value)

const walkthrough = ref<PlainWalkthrough>({ script: '', traceScript: '', steps: [], snippets: {} })
const stepIndex = ref(0)
const captured = ref<Record<string, Record<string, unknown>[]> | null>(null)
const traceState = ref<'idle' | 'running' | 'ready' | 'failed'>('idle')

const runColumns = computed(() => {
  const seen: string[] = []
  for (const row of runResult.value?.rows.slice(0, 50) ?? []) {
    for (const column of Object.keys(row)) if (!seen.includes(column)) seen.push(column)
  }
  return seen
})

const formatCell = (value: unknown): string =>
  value === null || value === undefined ? '—' : typeof value === 'object' ? JSON.stringify(value) : String(value)

const setMode = (next: CodeMode) => {
  if (mode.value === next) return
  // Whatever they typed is often an exercise solution; losing it on a tab
  // click is the one unforgivable thing this panel could do.
  if (mode.value === 'plain' && edited.value) draft.value = code.value
  mode.value = next
  localStorage.setItem(MODE_KEY, next)
  runResult.value = null
  comparison.value = null
  generateCodeFromFlow()
}

const resetEdits = () => {
  draft.value = null
  code.value = generated.value
  runResult.value = null
  comparison.value = null
}

/** Selecting the node behind the panel keeps the canvas in step with the walkthrough. */
const onStepSelect = (nodeId: number) => {
  flowStore.selectedNodeId = nodeId
}

let editorView: EditorView | null = null
const onEditorReady = (payload: { view: EditorView }) => {
  editorView = payload.view
}

/** Open the plain flavour on the pipeline, not on the helper functions above it. */
const scrollToPipeline = async () => {
  await nextTick()
  if (!editorView || mode.value !== 'plain') return
  const offset = code.value.indexOf('def run_etl_pipeline')
  if (offset <= 0) return
  const line = editorView.state.doc.lineAt(offset)
  editorView.dispatch({ effects: EditorView.scrollIntoView(line.from, { y: 'start' }) })
}

const extensions = [
  python(),
  oneDark,
  glossaryTooltip(),
  stepHighlight(), // brings the tooltip theme with it
  EditorView.theme({
    '&': { height: '100%' },
    '.cm-scroller': { overflow: 'auto' },
    '.cm-content': { padding: '16px' },
    '.cm-focused': { outline: 'none' },
  }),
  EditorView.lineWrapping
]

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
    if (mode.value === 'walkthrough') {
      walkthrough.value = buildWalkthrough({
        nodes: flowStore.nodes,
        edges: flowStore.edges,
        flowName: 'WASM Flow'
      })
      stepIndex.value = Math.min(stepIndex.value, Math.max(0, walkthrough.value.steps.length - 1))
      // The captured tables belong to the previous shape of the flow.
      captured.value = null
      traceState.value = 'idle'
      return
    }
    if (mode.value === 'plain') {
      // The plain flavour never refuses a flow: unsupported nodes become exercises.
      generated.value = generatePlainPython({
        nodes: flowStore.nodes,
        edges: flowStore.edges,
        flowName: 'WASM Flow'
      })
      code.value = draft.value ?? generated.value
      void scrollToPipeline()
      return
    }
    const formulaCode = await translateFormulaNodes()
    code.value = generateCode({
      nodes: flowStore.nodes,
      edges: flowStore.edges,
      flowName: 'WASM Flow',
      formulaCode
    })
  } catch (err) {
    error.value = err instanceof Error ? err.message : 'Failed to generate code'
    code.value = '# Failed to generate code. Please check your flow configuration.'
  } finally {
    loading.value = false
  }
}

/**
 * Run the generated script in Pyodide, in its own namespace.
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

/**
 * Run the instrumented build once and keep every intermediate table.
 *
 * A raising exercise stub is expected, not exceptional: __steps__ is populated
 * as the script goes, so the steps before the stub still have their data.
 */
const runTrace = async () => {
  if (!pyodideStore.isReady || traceState.value === 'running') return
  traceState.value = 'running'
  try {
    stageReadFiles()
    pyodideStore.setGlobal('_trace_script', walkthrough.value.traceScript)
    const result = await pyodideStore.runPythonWithResult(
      [
        'import json',
        '_trace_ns = {}',
        'try:',
        '    exec(compile(_trace_script, "pipeline.py", "exec"), _trace_ns)',
        '    _trace_ns["run_etl_pipeline"]()',
        'except Exception:',
        '    pass',
        'json.dumps(_trace_ns.get("__steps__", {}), default=str)'
      ].join('\n')
    )
    captured.value = JSON.parse(result)
    traceState.value = Object.keys(captured.value ?? {}).length > 0 ? 'ready' : 'failed'
  } catch {
    captured.value = null
    traceState.value = 'failed'
  } finally {
    pyodideStore.deleteGlobal('_trace_script')
  }
}

/**
 * Check the script's own output against what the canvas already produced.
 *
 * Reads an existing result only — running the flow is the user's call, so this
 * never reaches an execute_* bridge (see the no-auto-run contract).
 */
const compareToCanvas = async () => {
  comparison.value = null
  const rows = runResult.value?.rows
  if (!rows) return

  const terminal = [...flowStore.nodes.values()].find(node => {
    const consumers = flowStore.edges.filter(edge => Number(edge.source) === node.id)
    return consumers.length === 0
  })
  const result = terminal ? flowStore.nodeResults.get(terminal.id) : undefined
  if (!terminal || !result?.success) {
    comparison.value = { match: false, message: 'Run the flow first, then compare.' }
    return
  }

  const preview = await flowStore.fetchNodePreview(terminal.id).catch(() => null)
  const canvasRows = preview?.data?.total_rows ?? result.data?.total_rows ?? null
  if (canvasRows === null || canvasRows === undefined) {
    comparison.value = { match: false, message: 'No canvas result to compare against yet.' }
    return
  }
  comparison.value =
    rows.length === canvasRows
      ? { match: true, message: `Matches the canvas — ${canvasRows} rows.` }
      : { match: false, message: `Canvas produced ${canvasRows} rows; this produced ${rows.length}.` }
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
  // Refresh means "regenerate from the flow", so it supersedes any edits.
  draft.value = null
  generateCodeFromFlow()
}

const exportCode = () => {
  const blob = new Blob([code.value], { type: 'text/plain' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = mode.value === 'plain' ? 'pipeline_plain_python.py' : 'flowfile_pipeline.py'
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
    // Resolve the landing mode on open, so flipping Learning mode while the
    // panel is closed takes effect the next time it opens.
    mode.value = teachingModeAvailable.value ? initialMode() : 'polars'
    generateCodeFromFlow()
  }
)

const teachingModeAvailable = computed(() => props.teachingMode)

// Canvas hides its bottom-right widget while the walkthrough is docked over it.
watch([mode, () => props.isVisible], ([current, visible]) => {
  uiStore.codePanelMode = visible ? (current as 'polars' | 'plain' | 'walkthrough') : 'polars'
})
</script>

<style scoped>
.code-generator-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  /* Above the canvas's floating widgets (demo bubble, layout controls), which
     sit at --z-index-dropdown and would otherwise paint over the panel. */
  z-index: var(--z-index-modal, 1050);
  backdrop-filter: blur(2px);
}

.code-generator-panel {
  background: var(--color-background-primary);
  border-radius: 8px;
  width: 90%;
  max-width: 1200px;
  height: 85vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
  overflow: hidden;
}

/* Walkthrough docks to the side: the whole point is watching the node you are
   reading about, so the canvas has to stay visible and clickable behind it. */
.code-generator-overlay.docked {
  background: transparent;
  backdrop-filter: none;
  justify-content: flex-end;
  align-items: stretch;
  /* Clear the app header and tab strip: docking beside the canvas is pointless
     if it buries the Run and Code buttons you need to drive it. */
  padding: 100px 12px 12px;
  pointer-events: none;
}

.code-generator-overlay.docked .code-generator-panel {
  pointer-events: auto;
  width: min(620px, 45vw);
  max-width: none;
  height: 100%;
}

/* Below this the canvas is too narrow to be worth keeping visible, so the
   walkthrough goes back to being an ordinary modal. The breakpoint also has to
   clear the mode switch, which stops fitting beside the title around here. */
@media (max-width: 1240px) {
  .code-generator-overlay.docked {
    background: rgba(0, 0, 0, 0.5);
    backdrop-filter: blur(2px);
    pointer-events: auto;
    padding: 24px;
  }

  .code-generator-overlay.docked .code-generator-panel {
    width: 100%;
  }
}

/* The three tabs need room; let them wrap under the title rather than clip. */
@media (max-width: 1400px) {
  .code-generator-overlay.docked .code-header {
    flex-wrap: wrap;
    row-gap: 8px;
  }

  .code-generator-overlay.docked .mode-switch {
    order: 3;
    width: 100%;
    margin: 0;
  }
}

.walkthrough-slot {
  flex: 1;
  min-height: 0;
}

.code-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  border-bottom: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.code-header h3 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.header-actions {
  display: flex;
  gap: 8px;
  align-items: center;
}

.mode-switch {
  display: flex;
  margin-left: auto;
  margin-right: 12px;
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  overflow: hidden;
}

.mode-button {
  padding: 6px 14px;
  font-size: 12px;
  font-weight: 500;
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all 0.15s ease;
}

.mode-button:hover:not(.active) {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.mode-button.active {
  background: var(--color-accent);
  color: #fff;
}

.mode-note {
  padding: 10px 20px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.run-button:hover:not(:disabled) {
  background: var(--color-success, #2e9e5b) !important;
  border-color: var(--color-success, #2e9e5b) !important;
  color: #fff;
}

.run-output {
  flex-shrink: 0;
  max-height: 34%;
  overflow: auto;
  border-top: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.run-output-header {
  position: sticky;
  top: 0;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 20px;
  font-size: 12px;
  font-weight: 600;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.run-output.failed .run-output-header {
  color: var(--color-danger);
}

.run-output-close {
  border: none;
  background: transparent;
  color: inherit;
  cursor: pointer;
  font-size: 12px;
}

.run-output-compare {
  margin-left: auto;
  padding: 2px 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: 11px;
  cursor: pointer;
}

.run-output-compare:hover {
  border-color: var(--color-accent);
  color: var(--color-text-primary);
}

.comparison {
  font-size: 11px;
  font-weight: 500;
  color: var(--color-danger);
}

.comparison.match {
  color: var(--color-success, #2e9e5b);
}

.run-error {
  margin: 0;
  padding: 12px 20px;
  font-family: monospace;
  font-size: 11px;
  line-height: 1.5;
  white-space: pre-wrap;
  color: var(--color-danger);
}

.run-empty {
  padding: 12px 20px;
  font-size: 12px;
  color: var(--color-text-secondary);
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

.icon-button {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
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

.close-button:hover {
  background: var(--color-danger) !important;
  border-color: var(--color-danger) !important;
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

.code-editor-container {
  flex: 1;
  overflow: hidden;
  background: var(--color-code-bg);
}

.spinner {
  display: inline-block;
  width: 16px;
  height: 16px;
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
