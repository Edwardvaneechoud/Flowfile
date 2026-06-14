<template>
  <div class="formula-settings">
    <!-- Output field + data type (mirrors the desktop selector-container) -->
    <div class="selector-container">
      <div class="selector-field">
        <label>Output field</label>
        <input
          v-model="outputName"
          type="text"
          class="ff-input"
          list="formula-columns"
          placeholder="Select or create field"
          @input="emitUpdate"
        />
        <datalist id="formula-columns">
          <option v-for="col in columns" :key="col.name" :value="col.name" />
        </datalist>
      </div>
      <div class="selector-field selector-type">
        <label>Data type</label>
        <select v-model="outputType" class="ff-input" @change="emitUpdate">
          <option v-for="t in DATA_TYPES" :key="t" :value="t">{{ t }}</option>
        </select>
      </div>
    </div>

    <!-- Two-pane editor: Fields/Functions sidebar + code editor -->
    <div class="editor-container">
      <div class="sidebar">
        <div class="sidebar-tabs">
          <button
            type="button"
            class="tab"
            :class="{ active: tab === 'fields' }"
            @click="tab = 'fields'"
          >
            <i class="fas fa-columns" /> Fields
          </button>
          <button
            type="button"
            class="tab"
            :class="{ active: tab === 'functions' }"
            @click="tab = 'functions'"
          >
            <i class="fas fa-atom" /> Functions
          </button>
        </div>

        <div class="search-box">
          <i class="fas fa-search search-icon" />
          <input
            v-model="filterText"
            class="search-input"
            type="text"
            :placeholder="tab === 'fields' ? 'Filter fields' : 'Filter functions'"
          />
          <i v-if="filterText" class="fas fa-times clear-icon" @click="filterText = ''" />
        </div>

        <ul class="item-list">
          <li
            v-for="item in filteredItems"
            :key="item.label"
            class="item"
            :title="item.detail || item.label"
            @click="insertItem(item)"
          >
            <span class="item-name">{{ item.label }}</span>
            <span v-if="item.detail" class="item-detail">{{ item.detail }}</span>
          </li>
          <li v-if="filteredItems.length === 0" class="item-empty">
            {{ tab === 'fields' ? 'No input columns' : (functionNames.length ? 'No matches' : 'Loading functions…') }}
          </li>
        </ul>
      </div>

      <div class="editor-pane">
        <codemirror
          v-model="formula"
          :placeholder="editorPlaceholder"
          :style="{ height: '250px' }"
          :autofocus="false"
          :indent-with-tab="true"
          :tab-size="2"
          :extensions="extensions"
          @ready="handleReady"
          @update:model-value="handleFormulaChange"
        />
      </div>
    </div>

    <div v-if="packageError" class="error-banner">{{ packageError }}</div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, shallowRef, onMounted } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { EditorView, Decoration, DecorationSet, ViewPlugin } from '@codemirror/view'
import { EditorState, Extension, RangeSetBuilder } from '@codemirror/state'
import { autocompletion, CompletionSource, CompletionContext } from '@codemirror/autocomplete'
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
const functionNames = ref<string[]>([])

const tab = ref<'fields' | 'functions'>('fields')
const filterText = ref('')

const outputName = ref(props.settings.function?.field?.name ?? '')
const outputType = ref(props.settings.function?.field?.data_type || 'Auto')
const formula = ref(props.settings.function?.function || '')

const columns = computed<ColumnSchema[]>(() => flowStore.getNodeInputSchema(props.nodeId))

// Placeholder built from the node's real input columns.
const editorPlaceholder = computed(() => {
  const cols = columns.value
  if (cols.length >= 2) return `e.g. [${cols[0].name}] + [${cols[1].name}]`
  if (cols.length === 1) return `e.g. [${cols[0].name}] * 2`
  return 'e.g. [price] * 1.21'
})

interface SidebarItem {
  label: string
  detail?: string
  insert: string
}

const filteredItems = computed<SidebarItem[]>(() => {
  const q = filterText.value.toLowerCase()
  if (tab.value === 'fields') {
    return columns.value
      .filter(c => c.name.toLowerCase().includes(q))
      .map(c => ({ label: c.name, detail: c.data_type, insert: `[${c.name}]` }))
  }
  return functionNames.value
    .filter(n => n.toLowerCase().includes(q))
    .map(n => ({ label: n, insert: `${n}()` }))
})

// Syntax highlighting
type HighlightType = 'comment' | 'string' | 'column' | 'function'

const HIGHLIGHT_PRIORITY: Record<HighlightType, number> = { comment: 0, string: 1, column: 2, function: 3 }
const HIGHLIGHT_CLASS: Record<HighlightType, string> = {
  comment: 'cm-ff-comment',
  string: 'cm-ff-string',
  column: 'cm-ff-column',
  function: 'cm-ff-function',
}

function findCommentSpans(text: string): Array<{ start: number; end: number }> {
  const spans: Array<{ start: number; end: number }> = []
  let offset = 0
  for (const line of text.split('\n')) {
    let insideSingle = false
    let insideDouble = false
    const len = line.length
    for (let pos = 0; pos < len; pos++) {
      const ch = line[pos]
      if (ch === "'" && !insideDouble) insideSingle = !insideSingle
      else if (ch === '"' && !insideSingle) insideDouble = !insideDouble
      else if (ch === '/' && pos + 1 < len && line[pos + 1] === '/' && !insideSingle && !insideDouble) {
        spans.push({ start: offset + pos, end: offset + len })
        break
      }
    }
    offset += len + 1
  }
  return spans
}

const highlightPlugin = ViewPlugin.fromClass(
  class {
    decorations: DecorationSet
    constructor(v: EditorView) {
      this.decorations = this.build(v)
    }
    update(u: { docChanged: boolean; viewportChanged: boolean; view: EditorView }) {
      if (u.docChanged || u.viewportChanged) this.decorations = this.build(u.view)
    }
    build(v: EditorView) {
      const builder = new RangeSetBuilder<Decoration>()
      const { doc } = v.state
      const visible = v.visibleRanges
      const commentSpans = findCommentSpans(doc.toString())
      const intersectsVisible = (s: number, e: number) => visible.some(r => s < r.to && e > r.from)
      const overlapsComment = (s: number, e: number) => commentSpans.some(c => s < c.end && e > c.start)

      const spans: Array<{ start: number; end: number; type: HighlightType }> = []
      for (const c of commentSpans) {
        if (c.end > c.start && intersectsVisible(c.start, c.end)) {
          spans.push({ start: c.start, end: c.end, type: 'comment' })
        }
      }

      const regexFunction = /\b([a-zA-Z_]\w*)\(/g
      const regexColumn = /\[[^\]]+\]/g
      const regexString = /(["'])(?:(?=(\\?))\2.)*?\1/g
      const push = (start: number, end: number, type: HighlightType) => {
        if (end > start && !overlapsComment(start, end)) spans.push({ start, end, type })
      }

      for (const { from, to } of visible) {
        const text = doc.sliceString(from, to)
        let m: RegExpExecArray | null
        regexFunction.lastIndex = 0
        while ((m = regexFunction.exec(text)) !== null) push(from + m.index, from + m.index + m[1].length, 'function')
        regexColumn.lastIndex = 0
        while ((m = regexColumn.exec(text)) !== null) push(from + m.index, from + m.index + m[0].length, 'column')
        regexString.lastIndex = 0
        while ((m = regexString.exec(text)) !== null) push(from + m.index, from + m.index + m[0].length, 'string')
      }

      spans.sort((a, b) => a.start - b.start || HIGHLIGHT_PRIORITY[a.type] - HIGHLIGHT_PRIORITY[b.type])
      let lastEnd = -1
      for (const s of spans) {
        if (s.start < lastEnd) continue
        builder.add(s.start, s.end, Decoration.mark({ class: HIGHLIGHT_CLASS[s.type] }))
        lastEnd = s.end
      }
      return builder.finish()
    }
  },
  { decorations: v => v.decorations },
)

// Autocomplete: functions and [columns]
const completions: CompletionSource = (context: CompletionContext) => {
  const functionWord = context.matchBefore(/\w+/)
  const columnWord = context.matchBefore(/\[\w*/)
  if (
    (!functionWord || functionWord.from === functionWord.to) &&
    (!columnWord || columnWord.from === columnWord.to) &&
    !context.explicit
  ) {
    return null
  }

  const options: Array<{ label: string; type: string; detail?: string; apply: (v: EditorView) => void }> = []

  if (functionWord && context.state.sliceDoc(functionWord.from - 1, functionWord.from) !== '[') {
    const cur = functionWord.text.toLowerCase()
    functionNames.value
      .filter(n => n.toLowerCase().startsWith(cur))
      .forEach(n => {
        options.push({
          label: n,
          type: 'function',
          apply: (ev: EditorView) => {
            const insert = `${n}(`
            ev.dispatch({
              changes: { from: functionWord.from, to: functionWord.to, insert },
              selection: { anchor: functionWord.from + insert.length },
            })
          },
        })
      })
  }

  if (columnWord) {
    const inner = columnWord.text.slice(1).toLowerCase()
    columns.value
      .filter(c => c.name.toLowerCase().startsWith(inner))
      .forEach(c => {
        options.push({
          label: c.name,
          type: 'variable',
          detail: c.data_type,
          apply: (ev: EditorView) => {
            ev.dispatch({
              changes: { from: columnWord.from + 1, to: columnWord.to, insert: c.name },
              selection: { anchor: columnWord.from + 1 + c.name.length },
            })
          },
        })
      })
  }

  return { from: functionWord?.from ?? (columnWord ? columnWord.from + 1 : context.pos), options }
}

const extensions: Extension[] = [
  EditorView.theme({
    '&': { fontSize: '12px', backgroundColor: 'var(--color-background-primary)' },
    '.cm-content': { fontSize: '12px', caretColor: 'var(--color-text-primary)' },
    '.cm-gutters': {
      fontSize: '12px',
      backgroundColor: 'var(--color-background-secondary)',
      color: 'var(--color-text-muted)',
      border: 'none',
    },
    '.cm-activeLine': { backgroundColor: 'var(--color-background-hover)' },
    '.cm-activeLineGutter': { backgroundColor: 'var(--color-background-tertiary)' },
    '.cm-ff-function': { color: 'var(--color-code-keyword)', fontWeight: 'bold' },
    '.cm-ff-column': { color: 'var(--color-code-string)' },
    '.cm-ff-string': { color: 'var(--color-code-function)' },
    '.cm-ff-comment': { color: 'var(--color-text-tertiary)', fontStyle: 'italic' },
    // Theme the autocomplete popup (else invisible in dark mode).
    '.cm-tooltip': {
      backgroundColor: 'var(--color-background-secondary)',
      border: '1px solid var(--color-border-primary)',
      borderRadius: '4px',
      color: 'var(--color-text-primary)',
    },
    '.cm-tooltip.cm-tooltip-autocomplete > ul > li': {
      color: 'var(--color-text-primary)',
      padding: '2px 8px',
    },
    '.cm-tooltip.cm-tooltip-autocomplete > ul > li[aria-selected]': {
      backgroundColor: 'var(--color-accent)',
      color: 'var(--color-text-inverse)',
    },
    '.cm-completionDetail': { color: 'var(--color-text-muted)', fontStyle: 'italic' },
  }),
  EditorState.tabSize.of(2),
  autocompletion({ override: [completions], defaultKeymap: true, activateOnTyping: true, icons: false }),
  highlightPlugin,
]

const handleReady = (payload: { view: EditorView }) => {
  view.value = payload.view
}

function handleFormulaChange(value: string) {
  formula.value = value
  emitUpdate()
}

function insertItem(item: SidebarItem) {
  if (!view.value) return
  const head = view.value.state.selection.main.head
  view.value.dispatch({ changes: { from: head, to: head, insert: item.insert } })
  view.value.focus()
  formula.value = view.value.state.doc.toString()
  emitUpdate()
}

function emitUpdate() {
  const settings: NodeFormulaSettings = {
    ...props.settings,
    is_setup: outputName.value.trim().length > 0 && formula.value.trim().length > 0,
    function: {
      field: { name: outputName.value.trim(), data_type: outputType.value },
      function: formula.value,
    },
  }
  emit('update:settings', settings)
}

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
.formula-settings {
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 12px;
  color: var(--color-text-primary);
}

.selector-container {
  display: flex;
  align-items: flex-end;
  gap: 10px;
}

.selector-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1;
}

.selector-field.selector-type {
  flex: 0 0 140px;
}

.selector-field label {
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.ff-input {
  padding: 6px 8px;
  font-size: 13px;
  color: var(--color-text-primary);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  outline: none;
}

.ff-input:focus {
  border-color: var(--color-border-focus);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}

.editor-container {
  display: flex;
  border: 1px solid var(--color-border-primary);
  border-radius: 5px;
  overflow: hidden;
  height: 250px;
  background-color: var(--color-background-primary);
}

.sidebar {
  flex: 0 0 180px;
  display: flex;
  flex-direction: column;
  border-right: 1px solid var(--color-border-primary);
  background-color: var(--color-background-secondary);
  min-width: 0;
}

.sidebar-tabs {
  display: flex;
  border-bottom: 1px solid var(--color-border-primary);
}

.tab {
  flex: 1;
  padding: 6px 4px;
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-secondary);
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 4px;
}

.tab:hover {
  color: var(--color-text-primary);
  background-color: var(--color-background-hover);
}

.tab.active {
  color: var(--color-accent);
  border-bottom-color: var(--color-accent);
}

.search-box {
  position: relative;
  display: flex;
  align-items: center;
  padding: 6px;
}

.search-box .search-icon {
  position: absolute;
  left: 13px;
  font-size: 10px;
  color: var(--color-text-muted);
  pointer-events: none;
}

.search-input {
  width: 100%;
  box-sizing: border-box;
  padding: 4px 22px 4px 22px;
  font-size: 12px;
  color: var(--color-text-primary);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  outline: none;
}

.search-input:focus {
  border-color: var(--color-border-focus);
}

.search-box .clear-icon {
  position: absolute;
  right: 13px;
  font-size: 10px;
  color: var(--color-text-muted);
  cursor: pointer;
}

.search-box .clear-icon:hover {
  color: var(--color-text-secondary);
}

.item-list {
  flex: 1;
  margin: 0;
  padding: 0;
  list-style: none;
  overflow-y: auto;
}

.item {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 6px;
  padding: 4px 10px;
  font-size: 12px;
  cursor: pointer;
  border-left: 2px solid transparent;
}

.item:hover {
  background-color: var(--color-background-hover);
  border-left-color: var(--color-accent);
}

.item-name {
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: var(--font-family-mono, monospace);
}

.item-detail {
  flex-shrink: 0;
  font-size: 10px;
  color: var(--color-text-muted);
}

.item-empty {
  padding: 8px 10px;
  font-size: 11px;
  color: var(--color-text-muted);
  list-style: none;
}

.editor-pane {
  flex: 1;
  min-width: 0;
  overflow: hidden;
  background-color: var(--color-background-primary);
}

.error-banner {
  padding: 8px 10px;
  font-size: 12px;
  color: var(--color-danger);
  background-color: var(--color-danger-light);
  border: 1px solid var(--color-danger);
  border-radius: 4px;
}

:deep(.cm-editor) {
  height: 100%;
}

:deep(.cm-scroller) {
  font-family: var(--font-family-mono, 'Monaco', 'Menlo', monospace);
}
</style>
