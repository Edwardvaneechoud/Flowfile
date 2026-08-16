<template>
  <div class="node-explainer">
    <div class="section-header" @click="toggle">
      <span class="section-title">How would I write this myself?</span>
      <span class="expand-icon">{{ isExpanded ? '−' : '+' }}</span>
    </div>

    <div v-if="isExpanded" class="section-content">
      <p v-if="explanation?.explanation" class="explainer-prose">{{ explanation.explanation }}</p>
      <div v-if="explanation?.code" class="explainer-code">
        <!-- indent-with-tab off: even a read-only editor must not trap Tab. -->
        <Codemirror
          :model-value="explanation.code"
          :extensions="extensions"
          :disabled="true"
          :indent-with-tab="false"
        />
      </div>
      <p v-else class="explainer-hint">Connect this node to see the code for your own settings.</p>
      <p v-if="explanation?.helpers.length" class="explainer-hint">
        Calls {{ explanation.helpers.join(", ") }} — defined for you in the full script under Code ▸
        Python walkthrough.
      </p>
      <div v-if="explanation?.code" class="explainer-actions">
        <button class="explainer-copy" @click="copySnippet">{{ copied ? 'Copied ✓' : 'Copy' }}</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorState } from '@codemirror/state'
import { EditorView } from '@codemirror/view'
import { useFlowStore } from '../../stores/flow-store'
import { usePlainPythonGeneration } from '../../composables/usePlainPythonGeneration'
import { glossaryTooltip } from '../../composables/usePythonGlossary'
import { stepHighlight } from '../../composables/useStepHighlight'

const props = defineProps<{ nodeId: number }>()

// Someone who wants this open wants it open for every node, not just this one.
const STORAGE_KEY = 'flowfile-node-explainer-open'

const flowStore = useFlowStore()
const { explainNode } = usePlainPythonGeneration()

const isExpanded = ref(localStorage.getItem(STORAGE_KEY) === '1')
const copied = ref(false)

const extensions = [
  python(),
  oneDark,
  glossaryTooltip(),
  stepHighlight(), // carries the tooltip theme
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

const toggle = () => {
  isExpanded.value = !isExpanded.value
  localStorage.setItem(STORAGE_KEY, isExpanded.value ? '1' : '0')
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
</script>

<style scoped>
.node-explainer {
  margin-top: 16px;
  border-top: 1px solid var(--border-color, #e0e0e0);
  padding-top: 8px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 0;
  cursor: pointer;
  user-select: none;
}

.section-header:hover {
  opacity: 0.8;
}

.section-title {
  font-size: 12px;
  font-weight: 600;
  color: var(--text-secondary, #666);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.expand-icon {
  font-size: 16px;
  font-weight: bold;
  color: var(--text-secondary, #666);
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

.explainer-code {
  border-radius: 4px;
  overflow: hidden;
}

.explainer-actions {
  display: flex;
  justify-content: flex-end;
  margin-top: 6px;
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
  .node-explainer {
    border-top-color: #444;
  }

  .section-title,
  .expand-icon {
    color: #aaa;
  }

  .explainer-prose {
    color: #ddd;
  }
}
</style>
