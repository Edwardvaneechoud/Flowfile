<template>
  <div class="formula-editor-root">
    <div class="output-row">
      <div class="output-field">
        <label>Output column</label>
        <input
          v-model="outputName"
          type="text"
          class="text-input"
          placeholder="new_column"
          @input="emitUpdate"
        />
      </div>
      <div class="output-field output-type">
        <label>Data type</label>
        <select v-model="outputType" class="select-sm" @change="emitUpdate">
          <option v-for="t in DATA_TYPES" :key="t" :value="t">{{ t }}</option>
        </select>
      </div>
    </div>

    <div v-if="columns.length > 0" class="column-chips">
      <span class="chip-label">Click to insert:</span>
      <span
        v-for="col in columns"
        :key="col.name"
        class="column-chip"
        @click="insertColumn(col.name)"
        :title="'Insert [' + col.name + ']'"
      >
        {{ col.name }}
      </span>
    </div>

    <div class="editor-wrapper">
      <div class="editor-header">
        <span class="editor-title">Formula</span>
        <span class="editor-hint">e.g. [price] * 1.21</span>
      </div>
      <codemirror
        v-model="formula"
        placeholder="[column_a] + [column_b]"
        :style="{ height: '180px' }"
        :autofocus="false"
        :indent-with-tab="false"
        :tab-size="2"
        :extensions="extensions"
        @ready="handleReady"
        @update:model-value="handleFormulaChange"
      />
    </div>

    <div v-if="packageError" class="validation-error">{{ packageError }}</div>

    <div class="help-section">
      <div class="help-title">Examples</div>
      <div class="help-grid">
        <div class="help-item" @click="insertSnippet('[a] + [b]')">
          <code>[a] + [b]</code>
          <span>Add columns</span>
        </div>
        <div class="help-item" @click="insertSnippet('concat([first], &quot; &quot;, [last])')">
          <code>concat(…)</code>
          <span>Join text</span>
        </div>
        <div class="help-item" @click="insertSnippet('if [x] > 0 then &quot;pos&quot; else &quot;neg&quot; endif')">
          <code>if … then …</code>
          <span>Conditional</span>
        </div>
        <div class="help-item" @click="insertSnippet('round([value], 2)')">
          <code>round(…)</code>
          <span>Round number</span>
        </div>
        <div class="help-item" @click="insertSnippet('uppercase([name])')">
          <code>uppercase(…)</code>
          <span>To uppercase</span>
        </div>
        <div class="help-item" @click="insertSnippet('length([name])')">
          <code>length(…)</code>
          <span>Text length</span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, shallowRef, onMounted } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { python } from '@codemirror/lang-python'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView, keymap } from '@codemirror/view'
import { EditorState, Extension, Prec } from '@codemirror/state'
import { autocompletion, CompletionSource, acceptCompletion } from '@codemirror/autocomplete'
import { indentMore, indentLess } from '@codemirror/commands'
import { useFlowStore } from '../../stores/flow-store'
import { usePyodideStore } from '../../stores/pyodide-store'
import type { NodeFormulaSettings, ColumnSchema } from '../../types'

const FORMULA_PACKAGE = 'polars-expr-transformer==0.5.6'
const DATA_TYPES = ['Auto', 'String', 'Int64', 'Float64', 'Boolean', 'Date', 'Datetime']

const props = defineProps<{
  nodeId: number
  settings: NodeFormulaSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeFormulaSettings): void
}>()

const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()
const view = shallowRef<EditorView | null>(null)
const packageError = ref<string | null>(null)
// Function names pulled from the in-browser package once it loads (dynamic completions).
const functionNames = ref<string[]>([])

const outputName = ref(props.settings.function?.field?.name || 'new_column')
const outputType = ref(props.settings.function?.field?.data_type || 'Auto')
const formula = ref(props.settings.function?.function || '')

const columns = computed<ColumnSchema[]>(() => flowStore.getNodeInputSchema(props.nodeId))

const formulaCompletions: CompletionSource = (context) => {
  const word = context.matchBefore(/[\w[\]]*/)
  if (word?.from === word?.to && !context.explicit) {
    return null
  }
  const options = functionNames.value.map(name => ({
    label: name,
    type: 'function',
    detail: 'function',
  }))
  columns.value.forEach(col => {
    options.push({
      label: `[${col.name}]`,
      type: 'variable',
      detail: `Column: ${col.data_type}`,
    })
  })
  return { from: word?.from ?? context.pos, options }
}

const tabKeymap = keymap.of([
  {
    key: 'Tab',
    run: (v: EditorView): boolean => (acceptCompletion(v) ? true : indentMore(v)),
  },
  { key: 'Shift-Tab', run: (v: EditorView): boolean => indentLess(v) },
])

const extensions: Extension[] = [
  python(),
  oneDark,
  EditorState.tabSize.of(2),
  autocompletion({ override: [formulaCompletions], defaultKeymap: true, closeOnBlur: false }),
  Prec.highest(tabKeymap),
]

const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view
}

function handleFormulaChange(newValue: string) {
  formula.value = newValue
  emitUpdate()
}

function insertText(text: string) {
  if (!view.value) return
  const head = view.value.state.selection.main.head
  view.value.dispatch({ changes: { from: head, to: head, insert: text } })
  formula.value = view.value.state.doc.toString()
  emitUpdate()
}

function insertColumn(name: string) {
  insertText(`[${name}]`)
}

function insertSnippet(snippet: string) {
  insertText(snippet)
}

function emitUpdate() {
  const settings: NodeFormulaSettings = {
    ...props.settings,
    is_setup: outputName.value.trim().length > 0 && formula.value.trim().length > 0,
    function: {
      field: { name: outputName.value.trim() || 'new_column', data_type: outputType.value },
      function: formula.value,
    },
  }
  emit('update:settings', settings)
}

onMounted(async () => {
  // The formula engine (polars-expr-transformer) is micropip-installed on demand;
  // load it as the panel opens so schema + execution are ready when the user types.
  if (!pyodideStore.isReady) return
  try {
    await pyodideStore.ensurePyPackages([FORMULA_PACKAGE])
    const names = await pyodideStore.runPythonWithResult(
      'from polars_expr_transformer.function_overview import get_all_expressions\nget_all_expressions()'
    )
    if (Array.isArray(names)) functionNames.value = names as string[]
  } catch (err) {
    packageError.value = err instanceof Error ? err.message : String(err)
  }
})
</script>

<style scoped>
.formula-editor-root {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.output-row {
  display: flex;
  gap: 10px;
  padding: 10px 12px;
  background: #21252b;
  border-bottom: 1px solid #181a1f;
}

.output-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1;
}

.output-field.output-type {
  flex: 0 0 130px;
}

.output-field label {
  font-size: 10px;
  color: #6272a4;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.text-input,
.select-sm {
  padding: 5px 8px;
  font-size: 12px;
  background: #282c34;
  color: #abb2bf;
  border: 1px solid #3e4451;
  border-radius: 3px;
}

.text-input:focus,
.select-sm:focus {
  outline: none;
  border-color: #8be9fd;
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
</style>
