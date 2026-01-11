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
      <textarea
        ref="editorRef"
        :value="polarsCode"
        @input="updateCode(($event.target as HTMLTextAreaElement).value)"
        class="code-editor"
        spellcheck="false"
        placeholder="# Enter Polars code here...
# Available variables:
#   input_df - the input dataframe
#   pl - polars module
#
# Example:
input_df.filter(pl.col('column') > 0)"
      ></textarea>
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
import { ref, computed } from 'vue'
import { useFlowStore } from '../../stores/flow-store'
import type { PolarsCodeSettings, ColumnSchema } from '../../types'

const props = defineProps<{
  nodeId: number
  settings: PolarsCodeSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: PolarsCodeSettings): void
}>()

const flowStore = useFlowStore()
const editorRef = ref<HTMLTextAreaElement | null>(null)
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

function updateCode(value: string) {
  polarsCode.value = value
  validationError.value = null
  emitUpdate()
}

function insertColumn(name: string) {
  const editor = editorRef.value
  if (editor) {
    const start = editor.selectionStart
    const end = editor.selectionEnd
    const text = `pl.col("${name}")`
    const newCode = polarsCode.value.substring(0, start) + text + polarsCode.value.substring(end)
    polarsCode.value = newCode
    emitUpdate()
    setTimeout(() => {
      editor.focus()
      editor.setSelectionRange(start + text.length, start + text.length)
    }, 0)
  }
}

function insertSnippet(snippet: string) {
  const editor = editorRef.value
  if (editor) {
    const start = editor.selectionStart
    const end = editor.selectionEnd
    const newCode = polarsCode.value.substring(0, start) + snippet + polarsCode.value.substring(end)
    polarsCode.value = newCode
    emitUpdate()
    setTimeout(() => {
      editor.focus()
      editor.setSelectionRange(start + snippet.length, start + snippet.length)
    }, 0)
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

.code-editor {
  width: 100%;
  min-height: 400px;
  height: 100%;
  padding: 12px;
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', monospace;
  font-size: 13px;
  line-height: 1.6;
  background: #282c34;
  color: #abb2bf;
  border: none;
  resize: none;
  tab-size: 4;
  -moz-tab-size: 4;
}

.code-editor:focus {
  outline: none;
}

.code-editor::placeholder {
  color: #5c6370;
}

/* Scrollbar styling */
.code-editor::-webkit-scrollbar {
  width: 10px;
  height: 10px;
}

.code-editor::-webkit-scrollbar-track {
  background: #21252b;
}

.code-editor::-webkit-scrollbar-thumb {
  background: #4b5263;
  border-radius: 5px;
}

.code-editor::-webkit-scrollbar-thumb:hover {
  background: #5c6370;
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
</style>
