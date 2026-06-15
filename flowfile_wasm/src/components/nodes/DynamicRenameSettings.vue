<template>
  <div class="settings-pane">
    <p class="settings-note">Bulk-rename columns by applying one rule to a set of columns.</p>

    <div v-if="columns.length === 0" class="no-columns">
      No input columns available. Connect an input node first.
    </div>

    <template v-else>
      <!-- Rename mode -->
      <div class="field">
        <label class="field-label">Rename mode</label>
        <div class="segmented">
          <button
            v-for="m in renameModes"
            :key="m.value"
            type="button"
            class="seg-btn"
            :class="{ active: dr.rename_mode === m.value }"
            @click="setMode(m.value)"
          >
            {{ m.label }}
          </button>
        </div>
      </div>

      <!-- Mode-specific input -->
      <div v-if="dr.rename_mode === 'prefix'" class="field">
        <label class="field-label">Prefix</label>
        <input v-model="dr.prefix" type="text" class="text-input" placeholder="e.g. src_" @input="emitUpdate" />
      </div>
      <div v-else-if="dr.rename_mode === 'suffix'" class="field">
        <label class="field-label">Suffix</label>
        <input v-model="dr.suffix" type="text" class="text-input" placeholder="e.g. _raw" @input="emitUpdate" />
      </div>
      <div v-else-if="dr.rename_mode === 'formula'" class="field">
        <label class="field-label">Formula</label>
        <div class="formula-editor">
          <codemirror
            v-model="dr.formula"
            :style="{ height: '120px' }"
            :autofocus="false"
            :indent-with-tab="true"
            :tab-size="2"
            :extensions="extensions"
            @update:model-value="emitUpdate"
          />
        </div>
        <p class="hint">
          Use <code>[column_name]</code> for the current column name, e.g. <code>uppercase([column_name])</code>.
        </p>
        <div v-if="packageError" class="preview-error">{{ packageError }}</div>
      </div>
      <div v-else class="field">
        <p class="hint">
          The first row becomes the new column names and is then removed from the data. Resolved at run time.
        </p>
      </div>

      <!-- Selection -->
      <div class="field">
        <label class="field-label">Apply to</label>
        <div class="segmented">
          <button
            v-for="m in selectionModes"
            :key="m.value"
            type="button"
            class="seg-btn"
            :class="{ active: dr.selection_mode === m.value }"
            @click="setSelection(m.value)"
          >
            {{ m.label }}
          </button>
        </div>
      </div>

      <div v-if="dr.selection_mode === 'list'" class="field">
        <div class="column-list">
          <label v-for="col in columns" :key="col.name" class="column-item">
            <input
              type="checkbox"
              :checked="dr.selected_columns.includes(col.name)"
              @change="toggleColumn(col.name)"
            />
            <span class="column-name">{{ col.name }}</span>
            <span class="column-type">{{ col.data_type }}</span>
          </label>
        </div>
      </div>
      <div v-else-if="dr.selection_mode === 'data_type'" class="field">
        <select v-model="dr.selected_data_type" class="select" @change="emitUpdate">
          <option :value="null">Select data type...</option>
          <option v-for="g in availableGroups" :key="g" :value="g">{{ g }}</option>
        </select>
      </div>

      <!-- Preview -->
      <div class="field">
        <label class="field-label">Preview</label>
        <div v-if="dr.rename_mode === 'first_row'" class="preview-empty">
          First row values are read at run time. Run the flow to see the resulting columns.
        </div>
        <div v-else-if="previewError" class="preview-error">{{ previewError }}</div>
        <div v-else-if="previewLoading" class="preview-empty">Computing preview…</div>
        <div v-else-if="previewRows.length === 0" class="preview-empty">No columns will be renamed.</div>
        <div v-else class="preview-table">
          <div class="preview-head">
            <span>Original</span>
            <span></span>
            <span>Renamed</span>
          </div>
          <div v-for="row in previewRows" :key="row.oldName" class="preview-row">
            <span class="preview-old" :title="row.oldName">{{ row.oldName }}</span>
            <span class="preview-arrow">→</span>
            <span class="preview-new" :title="row.newName">{{ row.newName }}</span>
          </div>
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { EditorView } from '@codemirror/view'
import type { Extension } from '@codemirror/state'
import { autocompletion, CompletionSource } from '@codemirror/autocomplete'
import { useFlowStore } from '../../stores/flow-store'
import { usePyodideStore } from '../../stores/pyodide-store'
import { resolveDynamicRenameMap } from '../../stores/schema-inference'
import { dataTypeGroup } from '../../utils/dtypeGroup'
import type {
  NodeDynamicRenameSettings,
  DynamicRenameInput,
  ColumnSchema,
  RenameMode,
  ColumnSelectionMode,
  ReadableDataTypeGroup,
} from '../../types'

const FORMULA_PACKAGE = 'polars-expr-transformer==0.5.6'

const props = defineProps<{
  nodeId: number
  settings: NodeDynamicRenameSettings
}>()

const emit = defineEmits<{
  (e: 'update:settings', settings: NodeDynamicRenameSettings): void
}>()

const flowStore = useFlowStore()
const pyodideStore = usePyodideStore()

const columns = computed<ColumnSchema[]>(() => flowStore.getNodeInputSchema(props.nodeId))

function defaultInput(): DynamicRenameInput {
  return {
    rename_mode: 'prefix',
    prefix: '',
    suffix: '',
    formula: '',
    selection_mode: 'all',
    selected_columns: [],
    selected_data_type: null,
  }
}

const dr = ref<DynamicRenameInput>({ ...defaultInput(), ...(props.settings.dynamic_rename_input || {}) })

const renameModes: { value: RenameMode; label: string }[] = [
  { value: 'prefix', label: 'Prefix' },
  { value: 'suffix', label: 'Suffix' },
  { value: 'formula', label: 'Formula' },
  { value: 'first_row', label: 'First row' },
]
const selectionModes: { value: ColumnSelectionMode; label: string }[] = [
  { value: 'all', label: 'All columns' },
  { value: 'list', label: 'Specific columns' },
  { value: 'data_type', label: 'By data type' },
]

// Only the groups actually present in the incoming schema are selectable.
const availableGroups = computed<ReadableDataTypeGroup[]>(() => {
  const present = new Set<ReadableDataTypeGroup>()
  for (const c of columns.value) present.add(dataTypeGroup(c.data_type))
  return [...present]
})

function emitUpdate() {
  emit('update:settings', {
    ...props.settings,
    is_setup: true,
    dynamic_rename_input: { ...dr.value },
  })
}

function setMode(mode: RenameMode) {
  dr.value.rename_mode = mode
  emitUpdate()
}

function setSelection(mode: ColumnSelectionMode) {
  dr.value.selection_mode = mode
  emitUpdate()
}

function toggleColumn(name: string) {
  const selected = new Set(dr.value.selected_columns)
  if (selected.has(name)) selected.delete(name)
  else selected.add(name)
  // Keep selection in schema order for a stable rename map.
  dr.value.selected_columns = columns.value.map(c => c.name).filter(n => selected.has(n))
  emitUpdate()
}

// --- Preview -------------------------------------------------------------
interface PreviewRow {
  oldName: string
  newName: string
}

const previewRows = ref<PreviewRow[]>([])
const previewError = ref<string | null>(null)
const previewLoading = ref(false)
let previewTimer: ReturnType<typeof setTimeout> | null = null

function refreshPreview() {
  previewError.value = null
  const s = dr.value
  if (s.rename_mode === 'first_row') {
    previewRows.value = []
    previewLoading.value = false
    return
  }
  if (s.rename_mode !== 'formula') {
    // prefix / suffix resolve instantly client-side (same resolver as schema inference).
    const { map, duplicates } = resolveDynamicRenameMap(columns.value, s)
    previewRows.value = [...map.entries()].map(([oldName, newName]) => ({ oldName, newName }))
    previewError.value = duplicates.length ? `Duplicate column name(s): ${duplicates.join(', ')}` : null
    previewLoading.value = false
    return
  }
  // formula resolves in the engine (needs the expression package).
  if (!s.formula.trim()) {
    previewRows.value = []
    previewLoading.value = false
    return
  }
  if (!pyodideStore.isReady) {
    previewRows.value = []
    previewLoading.value = false
    return
  }
  previewLoading.value = true
  if (previewTimer) clearTimeout(previewTimer)
  previewTimer = setTimeout(async () => {
    try {
      await pyodideStore.ensurePyPackages([FORMULA_PACKAGE])
      const settingsJson = JSON.stringify(JSON.stringify(s))
      const colsJson = JSON.stringify(
        JSON.stringify(columns.value.map(c => ({ name: c.name, data_type: c.data_type })))
      )
      const res = await pyodideStore.runPythonWithResult(
        `import json\npreview_dynamic_rename(json.loads(${settingsJson}), json.loads(${colsJson}))`
      )
      if (res?.error) {
        previewError.value = res.error
        previewRows.value = []
      } else {
        const map = res?.rename_map || {}
        previewRows.value = Object.keys(map).map(k => ({ oldName: k, newName: map[k] }))
      }
    } catch (err) {
      previewError.value = err instanceof Error ? err.message : String(err)
      previewRows.value = []
    } finally {
      previewLoading.value = false
    }
  }, 300)
}

watch([dr, columns], () => refreshPreview(), { deep: true, immediate: true })

// --- Formula editor ------------------------------------------------------
const functionNames = ref<string[]>([])
const packageError = ref<string | null>(null)

const completions: CompletionSource = context => {
  const word = context.matchBefore(/[\w[]*/)
  if (!word || (word.from === word.to && !context.explicit)) return null
  const options = [
    { label: '[column_name]', type: 'variable', detail: 'current column name' },
    ...functionNames.value.map(n => ({ label: n, type: 'function', apply: `${n}(` })),
  ]
  return { from: word.from, options }
}

const extensions: Extension[] = [
  EditorView.theme({
    '&': { fontSize: '12px', backgroundColor: 'var(--color-background-primary)' },
    '.cm-content': { fontSize: '12px', caretColor: 'var(--color-text-primary)' },
    '.cm-gutters': {
      backgroundColor: 'var(--color-background-secondary)',
      color: 'var(--color-text-muted)',
      border: 'none',
    },
    '.cm-tooltip': {
      backgroundColor: 'var(--color-background-secondary)',
      border: '1px solid var(--color-border-primary)',
      borderRadius: '4px',
      color: 'var(--color-text-primary)',
    },
    '.cm-tooltip.cm-tooltip-autocomplete > ul > li[aria-selected]': {
      backgroundColor: 'var(--color-accent)',
      color: 'var(--color-text-inverse)',
    },
  }),
  autocompletion({ override: [completions], activateOnTyping: true, icons: false }),
]

onMounted(async () => {
  if (!pyodideStore.isReady) return
  try {
    await pyodideStore.ensurePyPackages([FORMULA_PACKAGE])
    const names = await pyodideStore.runPythonWithResult(
      'from polars_expr_transformer.function_overview import get_all_expressions\nget_all_expressions()'
    )
    if (Array.isArray(names)) functionNames.value = (names as string[]).slice().sort()
  } catch (err) {
    packageError.value = err instanceof Error ? err.message : String(err)
  }
})
</script>

<style scoped>
.settings-pane {
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 14px;
  color: var(--color-text-primary);
}

.settings-note {
  font-size: 12px;
  color: var(--color-text-tertiary);
  line-height: 1.5;
  margin: 0;
}

.no-columns {
  font-size: 12px;
  color: var(--color-text-muted);
}

.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.field-label {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.segmented {
  display: flex;
  gap: 0;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
  width: fit-content;
}

.seg-btn {
  padding: 5px 12px;
  font-size: 12px;
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  border: none;
  border-right: 1px solid var(--color-border-primary);
  cursor: pointer;
}

.seg-btn:last-child {
  border-right: none;
}

.seg-btn:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.seg-btn.active {
  background: var(--color-accent);
  color: var(--color-text-inverse);
}

.text-input,
.select {
  padding: 5px 8px;
  font-size: 13px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
}

.text-input:focus,
.select:focus {
  outline: none;
  border-color: var(--color-border-focus);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}

.hint {
  font-size: 11px;
  color: var(--color-text-tertiary);
  line-height: 1.5;
  margin: 0;
}

.hint code {
  font-family: var(--font-family-mono, monospace);
  background: var(--color-background-secondary);
  padding: 1px 4px;
  border-radius: 3px;
}

.formula-editor {
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
}

.column-list {
  display: flex;
  flex-direction: column;
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
}

.column-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 4px 8px;
  font-size: 12px;
  cursor: pointer;
}

.column-item:hover {
  background: var(--color-background-hover);
}

.column-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: var(--font-family-mono, monospace);
}

.column-type {
  font-size: 10px;
  color: var(--color-text-muted);
}

.preview-empty {
  font-size: 12px;
  color: var(--color-text-muted);
  font-style: italic;
}

.preview-error {
  font-size: 12px;
  color: var(--color-danger, #f56c6c);
  background: var(--color-danger-light, rgba(245, 108, 108, 0.1));
  border-radius: 3px;
  padding: 4px 8px;
}

.preview-table {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 220px;
  overflow-y: auto;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  padding: 4px 0;
}

.preview-head,
.preview-row {
  display: grid;
  grid-template-columns: 1fr 20px 1fr;
  align-items: center;
  gap: 4px;
  padding: 2px 8px;
  font-size: 12px;
}

.preview-head {
  font-weight: 600;
  color: var(--color-text-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  padding-bottom: 4px;
}

.preview-old {
  color: var(--color-text-secondary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: var(--font-family-mono, monospace);
}

.preview-new {
  color: var(--color-text-primary);
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: var(--font-family-mono, monospace);
}

.preview-arrow {
  text-align: center;
  color: var(--color-text-muted);
}
</style>
