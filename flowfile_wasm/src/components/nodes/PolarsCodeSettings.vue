<template>
  <div class="polars-editor-root">
    <div v-if="columns.length > 0" class="column-chips">
      <span class="chip-label">Click to insert:</span>
      <span
        v-for="col in columns"
        :key="col.name"
        class="column-chip"
        @click="insertColumn(col.name)"
        :title="'Insert pl.col(&quot;' + col.name + '&quot;)'"
      >
        {{ col.name }}
      </span>
    </div>

    <div class="editor-wrapper">
      <div class="editor-header">
        <span class="editor-title">Polars Code</span>
        <span class="editor-hint">Python/Polars expressions</span>
      </div>
      <codemirror
        v-model="polarsCode"
        placeholder="# Enter Polars code here..."
        :style="{ height: '400px' }"
        :autofocus="false"
        :indent-with-tab="false"
        :tab-size="4"
        :extensions="extensions"
        @ready="handleReady"
        @update:model-value="handleCodeChange"
      />
    </div>

    <div v-if="validationError" class="validation-error">
      {{ validationError }}
    </div>

    <div class="help-section">
      <div class="help-title">Quick Reference</div>
      <div class="help-grid">
        <div class="help-item" @click="insertSnippet(snippets.passthrough)">
          <code>input_df</code>
          <span>Return unchanged</span>
        </div>
        <div class="help-item" @click="insertSnippet(snippets.filter)">
          <code>filter()</code>
          <span>Filter rows</span>
        </div>
        <div class="help-item" @click="insertSnippet(snippets.select)">
          <code>select()</code>
          <span>Select columns</span>
        </div>
        <div class="help-item" @click="insertSnippet(snippets.withColumns)">
          <code>with_columns()</code>
          <span>Add/modify</span>
        </div>
        <div class="help-item" @click="insertSnippet(snippets.groupBy)">
          <code>group_by()</code>
          <span>Aggregate</span>
        </div>
        <div class="help-item" @click="insertSnippet(snippets.sort)">
          <code>sort()</code>
          <span>Sort rows</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, shallowRef } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView, keymap } from '@codemirror/view'
import { EditorState, Extension, Prec } from '@codemirror/state'
import { autocompletion, CompletionSource, acceptCompletion } from '@codemirror/autocomplete'
import { indentMore, indentLess } from '@codemirror/commands'
import { useFlowStore } from '../../stores/flow-store'
import { polarsCompletionVals } from '../../config/polarsCompletions'
import type { PolarsCodeSettings, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: PolarsCodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: PolarsCodeSettings): void
}>()

const flowStore = useFlowStore()
const view = shallowRef<EditorView | null>(null)
const validationError = ref<string | null>(null)

// Code snippets for quick reference
const snippets = {
  passthrough: 'input_df',
  filter: "input_df.filter(pl.col('column') > 0)",
  select: "input_df.select(['column'])",
  withColumns: "input_df.with_columns(pl.col('a').alias('b'))",
  groupBy: "input_df.group_by('col').agg(pl.col('val').sum())",
  sort: "input_df.sort('column', descending=True)"
}

const defaultCode = `# Polars transformation
# The input dataframe is available as 'input_df'
# Return the transformed dataframe

input_df`

// Initialize directly from props
const polarsCode = ref(props.settings.polars_code_input?.polars_code || defaultCode)

const columns = computed<ColumnSchema[]>(() => {
  return flowStore.getNodeInputSchema(props.nodeId)
})

// Polars-specific autocompletions
const polarsCompletions: CompletionSource = (context) => {
  const word = context.matchBefore(/[\w.]*/)
  if (word?.from === word?.to && !context.explicit) {
    return null
  }

  // Convert polarsCompletionVals to CodeMirror format
  const completionOptions = polarsCompletionVals.map(item => ({
    label: item.label,
    type: item.type,
    detail: item.info
  }))

  // Add column completions from input schema
  columns.value.forEach(col => {
    completionOptions.push({
      label: `pl.col("${col.name}")`,
      type: 'variable',
      detail: `Column: ${col.data_type}`
    })
  })

  return {
    from: word?.from || context.pos,
    options: completionOptions,
  }
}

// Custom keymap for tab handling
const tabKeymap = keymap.of([
  {
    key: 'Tab',
    run: (view: EditorView): boolean => {
      if (acceptCompletion(view)) {
        return true
      }
      return indentMore(view)
    },
  },
  {
    key: 'Shift-Tab',
    run: (view: EditorView): boolean => {
      return indentLess(view)
    },
  },
])

// Extensions configuration
const extensions: Extension[] = [
  python(),
  oneDark,
  EditorState.tabSize.of(4),
  autocompletion({
    override: [polarsCompletions],
    defaultKeymap: true,
    closeOnBlur: false,
  }),
  Prec.highest(tabKeymap),
]

const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view
}

function handleCodeChange(newCode: string) {
  polarsCode.value = newCode
  validationError.value = null
  emitUpdate()
}

function insertColumn(name: string) {
  if (view.value) {
    const text = `pl.col("${name}")`
    view.value.dispatch({
      changes: {
        from: view.value.state.selection.main.head,
        to: view.value.state.selection.main.head,
        insert: text,
      },
    })
    // Update the model
    polarsCode.value = view.value.state.doc.toString()
    emitUpdate()
  }
}

function insertSnippet(snippet: string) {
  if (view.value) {
    view.value.dispatch({
      changes: {
        from: view.value.state.selection.main.head,
        to: view.value.state.selection.main.head,
        insert: snippet,
      },
    })
    // Update the model
    polarsCode.value = view.value.state.doc.toString()
    emitUpdate()
  }
}

function emitUpdate() {
  const settings: PolarsCodeSettings = {
    ...props.settings,
    is_setup: polarsCode.value.trim().length > 0,
    polars_code_input: {
      polars_code: polarsCode.value
    }
  }
  emit('update:settings', settings)
}
</script>

<style scoped>
.polars-editor-root {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.column-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  padding: 8px 12px;
  align-items: center;
  background: #21252b;
  border-bottom: 1px solid #181a1f;
}

.chip-label {
  font-size: 11px;
  color: #6272a4;
  margin-right: 4px;
}

.column-chip {
  padding: 3px 8px;
  font-size: 11px;
  background: #282c34;
  color: #50fa7b;
  border-radius: 3px;
  cursor: pointer;
  transition: all 0.15s;
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  border: 1px solid #3e4451;
}

.column-chip:hover {
  background: #3e4451;
  border-color: #50fa7b;
}

.editor-wrapper {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 6px 12px;
  background: #21252b;
  border-bottom: 1px solid #181a1f;
}

.editor-title {
  font-size: 12px;
  font-weight: 600;
  color: #abb2bf;
}

.editor-hint {
  font-size: 10px;
  color: #6272a4;
}

.validation-error {
  padding: 8px 12px;
  color: #ff5555;
  background: rgba(255, 85, 85, 0.1);
  border-top: 1px solid rgba(255, 85, 85, 0.3);
  font-size: 12px;
  font-family: monospace;
}

.help-section {
  padding: 10px 12px;
  background: #21252b;
  border-top: 1px solid #181a1f;
}

.help-title {
  font-size: 10px;
  font-weight: 600;
  color: #6272a4;
  margin-bottom: 8px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.help-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 4px;
}

.help-item {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 6px 8px;
  background: #282c34;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.15s;
  border: 1px solid transparent;
}

.help-item:hover {
  background: #3e4451;
  border-color: #8be9fd;
}

.help-item code {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  font-size: 10px;
  color: #8be9fd;
}

.help-item span {
  font-size: 9px;
  color: #6272a4;
}

/* CodeMirror customization */
:deep(.cm-editor) {
  height: 100%;
  font-size: 13px;
}

:deep(.cm-scroller) {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', monospace;
}

:deep(.cm-gutters) {
  background: #21252b;
  border-right: 1px solid #181a1f;
}

:deep(.cm-activeLineGutter) {
  background: #2c313a;
}

:deep(.cm-activeLine) {
  background: rgba(255, 255, 255, 0.03);
}
</style>
