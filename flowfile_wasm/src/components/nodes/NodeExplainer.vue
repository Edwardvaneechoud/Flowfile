<template>
  <div class="node-explainer">
    <button type="button" class="section-header" :aria-expanded="isExpanded" @click="toggle">
      <svg class="section-chevron" :class="{ open: isExpanded }" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><polyline points="9 18 15 12 9 6"></polyline></svg>
      <span class="section-title">How would I write this myself?</span>
    </button>

    <div v-if="isExpanded" class="section-content">
      <p v-if="explanation?.explanation" class="explainer-prose">{{ explanation.explanation }}</p>
      <!-- A stubbed node has no honest loop — its note is not worth an editor.
           The Polars snippet below is the real code for those. -->
      <template v-if="explanation?.code && !explanation.isStub">
        <div class="block-head">
          <button type="button" class="block-toggle" :aria-expanded="plainOpen" @click="togglePlain">
            <span class="block-chevron" :class="{ open: plainOpen }" aria-hidden="true"></span>
            <span>Plain Python</span>
            <span class="block-lines">{{ lineLabel(explanation.code) }}</span>
          </button>
          <button class="explainer-copy" aria-label="Copy the plain-Python loop" @click="copySnippet">
            {{ copied ? 'Copied ✓' : 'Copy' }}
          </button>
        </div>
        <template v-if="plainOpen">
          <div class="explainer-code">
            <!-- indent-with-tab off: even a read-only editor must not trap Tab. -->
            <Codemirror
              :model-value="explanation.code"
              :extensions="extensions"
              :disabled="true"
              :indent-with-tab="false"
            />
          </div>
          <p v-if="explanation?.helpers.length" class="explainer-hint">
            Calls {{ explanation.helpers.join(", ") }} — defined for you in the full script under Code ▸
            Python walkthrough.
          </p>
        </template>
      </template>
      <p v-else-if="!explanation?.code" class="explainer-hint">
        Connect this node to see the code for your own settings.
      </p>
      <template v-if="explanation?.code && polarsSnippet">
        <p class="explainer-bridge">{{ bridgeLabel }}</p>
        <div class="block-head">
          <button type="button" class="block-toggle" :aria-expanded="polarsOpen" @click="togglePolars">
            <span class="block-chevron" :class="{ open: polarsOpen }" aria-hidden="true"></span>
            <span>Polars</span>
            <span class="block-lines">{{ lineLabel(polarsSnippet) }}</span>
          </button>
          <button class="explainer-copy" aria-label="Copy the Polars snippet" @click="copyPolars">
            {{ copiedPolars ? 'Copied ✓' : 'Copy' }}
          </button>
        </div>
        <div v-if="polarsOpen" class="explainer-code">
          <Codemirror
            :model-value="polarsSnippet"
            :extensions="extensions"
            :disabled="true"
            :indent-with-tab="false"
          />
        </div>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorState } from '@codemirror/state'
import { EditorView } from '@codemirror/view'
import { useFlowStore } from '../../stores/flow-store'
import { usePyodideStore } from '../../stores/pyodide-store'
import { usePlainPythonGeneration } from '../../composables/usePlainPythonGeneration'
import { useCodeGeneration } from '../../composables/useCodeGeneration'
import { translateFormulaNodes } from '../../composables/useFormulaTranslation'
import { glossaryTooltip } from '../../composables/usePythonGlossary'
import type { NodeFormulaSettings } from '../../types'

const props = defineProps<{ nodeId: number }>()

// Someone who wants this open wants it open for every node, not just this one.
const STORAGE_KEY = 'flowfile-node-explainer-open'
const PLAIN_KEY = 'flowfile-node-explainer-plain-open'
const POLARS_KEY = 'flowfile-node-explainer-polars-open'

const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()
const { explainNode } = usePlainPythonGeneration()
const { explainNodePolars } = useCodeGeneration()

const isExpanded = ref(localStorage.getItem(STORAGE_KEY) === '1')
// Each snippet collapses on its own — the plain loop runs long. Open by default.
const plainOpen = ref(localStorage.getItem(PLAIN_KEY) !== '0')
const polarsOpen = ref(localStorage.getItem(POLARS_KEY) !== '0')
const copied = ref(false)
const copiedPolars = ref(false)

const extensions = [
  python(),
  oneDark,
  glossaryTooltip(),
  EditorView.editable.of(false),
  EditorState.readOnly.of(true),
  EditorView.lineWrapping,
  EditorView.theme({
    '&': { fontSize: '11px' },
    '.cm-content': { padding: '10px' },
    '.cm-focused': { outline: 'none' }
  })
]

// Generating a snippet converts the whole flow, so only do it while open.
const explanation = computed(() =>
  isExpanded.value
    ? explainNode({ nodes: flowStore.nodes, edges: flowStore.edges }, props.nodeId)
    : null
)

// Upgrade formula snippets from the runtime-translation fallback to the real
// Polars expression, once Pyodide is ready (never triggers its initialization).
// Whole-flow, not just this node: an untranslated formula stays multi-line,
// which changes chain fusion and so the names in other nodes' snippets.
const formulaCode = ref<Record<number, string>>({})
const formulaKey = computed(() =>
  [...flowStore.nodes.values()]
    .filter(n => n.type === 'formula')
    .map(n => `${n.id}:${(n.settings as NodeFormulaSettings)?.function?.function ?? ''}`)
    .join('|')
)
let translatedKey: string | null = null
watch(
  () => (isExpanded.value && pyodideStore.isReady ? formulaKey.value : null),
  async key => {
    if (!key || key === translatedKey) return
    const translated = await translateFormulaNodes(flowStore.nodes)
    // Only cache a success; a failed fetch may work on the next trigger.
    if (Object.keys(translated).length > 0) translatedKey = key
    formulaCode.value = translated
  },
  { immediate: true }
)

// The same step in Polars. polars_code nodes already are Polars — nothing to bridge.
const polarsSnippet = computed(() => {
  if (!isExpanded.value) return null
  if (flowStore.nodes.get(props.nodeId)?.type === 'polars_code') return null
  return explainNodePolars(
    { nodes: flowStore.nodes, edges: flowStore.edges, formulaCode: formulaCode.value },
    props.nodeId
  )
})

// For a stubbed node the plain block is hidden, so the Polars snippet is the
// step's only code — introduce it as such, and without claiming "what the
// canvas runs": reader stubs (Excel reads) get a snippet that genuinely
// differs from the engine.
const bridgeLabel = computed(() =>
  explanation.value?.isStub
    ? 'This step in Polars:'
    : 'The same step in Polars — the dataframe library the canvas actually runs:'
)

const toggle = () => {
  isExpanded.value = !isExpanded.value
  localStorage.setItem(STORAGE_KEY, isExpanded.value ? '1' : '0')
}

const togglePlain = () => {
  plainOpen.value = !plainOpen.value
  localStorage.setItem(PLAIN_KEY, plainOpen.value ? '1' : '0')
}

const togglePolars = () => {
  polarsOpen.value = !polarsOpen.value
  localStorage.setItem(POLARS_KEY, polarsOpen.value ? '1' : '0')
}

const lineLabel = (code: string | null | undefined) => {
  const lines = code ? code.trimEnd().split('\n').length : 0
  return `${lines} line${lines === 1 ? '' : 's'}`
}

const copySnippet = async () => {
  if (!explanation.value?.code) return
  try {
    await navigator.clipboard.writeText(explanation.value.code)
    copied.value = true
    setTimeout(() => (copied.value = false), 1500)
  } catch {
    /* clipboard blocked; the snippet is selectable anyway */
  }
}

const copyPolars = async () => {
  if (!polarsSnippet.value) return
  try {
    await navigator.clipboard.writeText(polarsSnippet.value)
    copiedPolars.value = true
    setTimeout(() => (copiedPolars.value = false), 1500)
  } catch {
    /* clipboard blocked; the snippet is selectable anyway */
  }
}
</script>

<style scoped>
.node-explainer {
  margin-top: 16px;
}

.section-header {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%;
  height: 32px;
  padding: 0 var(--spacing-4, 16px);
  border: none;
  border-radius: var(--radius-sm, 4px);
  background: var(--color-background-muted, #fafafa);
  color: var(--color-text-primary, #333);
  cursor: pointer;
  user-select: none;
  text-align: left;
}

.section-header:hover {
  background: var(--color-background-secondary, #f0f2f5);
}

.section-header:focus {
  outline: none;
}

.section-header:focus-visible {
  outline: 2px solid var(--color-accent, #4a90d9);
  outline-offset: 1px;
}

.section-title {
  font-size: 12px;
  font-weight: 500;
}

.section-chevron {
  flex: 0 0 auto;
  color: var(--color-text-secondary, #888);
  transition: transform 0.15s ease;
}

.section-chevron.open {
  transform: rotate(90deg);
}

/* No inner scroll: this sits below the settings form, so the panel's own
   scrollbar is the one that should move. A nested one just fights it. */
.section-content {
  padding: 8px 0 16px;
}

.explainer-prose {
  margin: 0 0 8px;
  font-size: 12px;
  line-height: 1.55;
  color: var(--text-primary, #333);
}

.explainer-hint {
  margin: 0;
  font-size: 12px;
  font-style: italic;
  color: var(--text-secondary, #888);
}

.explainer-bridge {
  margin: 14px 0 6px;
  font-size: 12px;
  line-height: 1.55;
  color: var(--text-secondary, #666);
}

.explainer-code {
  border-radius: 4px;
  overflow: hidden;
}

.block-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 6px;
}

.block-toggle {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 2px 0;
  border: none;
  background: none;
  color: var(--text-secondary, #666);
  font-size: 11px;
  cursor: pointer;
}

.block-toggle:focus {
  outline: none;
}

.block-toggle:focus-visible {
  outline: 2px solid var(--color-accent, #4a90d9);
  outline-offset: 2px;
}

.block-chevron {
  width: 0;
  height: 0;
  border-left: 4px solid currentColor;
  border-top: 3.5px solid transparent;
  border-bottom: 3.5px solid transparent;
  transition: transform 0.15s ease;
}

.block-chevron.open {
  transform: rotate(90deg);
}

.block-lines {
  color: var(--text-tertiary, #999);
}

.explainer-copy {
  border: 1px solid var(--border-color, #e0e0e0);
  background: transparent;
  color: var(--text-secondary, #666);
  border-radius: 4px;
  padding: 2px 10px;
  font-size: 11px;
  cursor: pointer;
}

.explainer-copy:hover {
  background: var(--bg-secondary, #f0f0f0);
}

@media (prefers-color-scheme: dark) {
  .explainer-prose {
    color: #ddd;
  }

  .explainer-bridge {
    color: #aaa;
  }
}
</style>
