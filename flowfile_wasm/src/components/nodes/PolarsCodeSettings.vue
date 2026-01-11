<template>
  <div class="listbox-wrapper">
    <div class="listbox-subtitle">Polars Code</div>

    <div v-if="columns.length > 0" class="column-chips">
      <span class="chip-label">Available columns:</span>
      <span
        v-for="col in columns"
        :key="col.name"
        class="column-chip"
        @click="insertColumn(col.name)"
        :title="'Click to insert pl.col(&quot;' + col.name + '&quot;)'"
      >
        {{ col.name }}
      </span>
    </div>

    <div class="editor-container">
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

    <div class="help-section">
      <div class="help-title">Quick Reference</div>
      <div class="help-examples">
        <div class="help-example">
          <code>input_df</code>
          <span>Return input unchanged</span>
        </div>
        <div class="help-example">
          <code>input_df.filter(pl.col('a') > 0)</code>
          <span>Filter rows</span>
        </div>
        <div class="help-example">
          <code>input_df.select(['a', 'b'])</code>
          <span>Select columns</span>
        </div>
        <div class="help-example">
          <code>input_df.with_columns(pl.col('a') * 2)</code>
          <span>Add/modify column</span>
        </div>
        <div class="help-example">
          <code>input_df.group_by('a').agg(pl.col('b').sum())</code>
          <span>Group and aggregate</span>
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

const defaultCode = `# Example Polars transformation
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
    // Set cursor position after inserted text
    setTimeout(() => {
      editor.focus()
      editor.setSelectionRange(start + text.length, start + text.length)
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
.column-chips {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  padding: 8px 12px;
  align-items: center;
  border-bottom: 1px solid var(--border-light);
}

.chip-label {
  font-size: 11px;
  color: var(--text-secondary);
  margin-right: 4px;
}

.column-chip {
  padding: 2px 6px;
  font-size: 11px;
  background: var(--bg-tertiary);
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: all 0.15s;
  font-family: monospace;
}

.column-chip:hover {
  background: var(--accent-color);
  color: white;
}

.editor-container {
  padding: 8px;
}

.code-editor {
  width: 100%;
  min-height: 200px;
  padding: 12px;
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
  font-size: 13px;
  line-height: 1.5;
  background: #1e1e1e;
  color: #d4d4d4;
  border: 1px solid var(--border-color);
  border-radius: var(--radius-sm);
  resize: vertical;
  tab-size: 4;
}

.code-editor:focus {
  outline: none;
  border-color: var(--accent-color);
}

.code-editor::placeholder {
  color: #6a6a6a;
}

.help-section {
  padding: 12px;
  background: var(--bg-tertiary);
  border-top: 1px solid var(--border-light);
}

.help-title {
  font-size: 11px;
  font-weight: 600;
  color: var(--text-secondary);
  margin-bottom: 8px;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.help-examples {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.help-example {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  font-size: 11px;
}

.help-example code {
  background: var(--bg-secondary);
  padding: 2px 4px;
  border-radius: 2px;
  font-family: monospace;
  font-size: 10px;
  white-space: nowrap;
  flex-shrink: 0;
}

.help-example span {
  color: var(--text-secondary);
}
</style>
