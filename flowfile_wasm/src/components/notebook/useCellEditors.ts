/**
 * The notebook cells' editors: one extension list per cell (built once, so an editor is configured once),
 * the keys a cell answers, its completions, the refused line it marks, and which editor gets the caret.
 */

import { nextTick, watch, type Ref } from 'vue'
import { acceptCompletion, autocompletion } from '@codemirror/autocomplete'
import { python } from '@codemirror/lang-python'
import { indentUnit } from '@codemirror/language'
import { EditorState, Prec, type Extension } from '@codemirror/state'
import { EditorView, keymap } from '@codemirror/view'
import { useNotebookStore, type NotebookCell } from '../../stores/notebook-store'
import { notebookCompletionSources, type CellCompletionContext } from './notebookCompletions'
import { notebookEditorLook } from './notebookEditor'
import { setSyncErrorMark, syncErrorLineField } from './syncErrorLine'

const baseExtensions: Extension[] = [
  python(),
  notebookEditorLook,
  EditorView.lineWrapping,
  syncErrorLineField,
  indentUnit.of('    '),
  EditorState.tabSize.of(4)
]
// Tab takes the completion on offer and otherwise moves focus, as the cell's indent-with-tab="false" promises.
const tabKeys = keymap.of([{ key: 'Tab', run: acceptCompletion }])

export function useCellEditors(root: Ref<HTMLElement | null>) {
  const notebook = useNotebookStore()
  const cellExtensions = new Map<string, Extension[]>()
  const views = new Map<string, EditorView>()
  /** The cell just added, to put the caret in as soon as its editor is there. */
  let awaitingFocus: string | null = null

  function addCell(afterCellId: string | null) {
    awaitingFocus = notebook.addCell(afterCellId)
  }

  /** What a cell's completions read: the cells above it as they read now, and the columns the canvas knows. */
  function completionContext(cellId: string): CellCompletionContext {
    const shown = notebook.shownCells
    const index = shown.findIndex(cell => cell.cell_id === cellId)
    return {
      surface: notebook.surface,
      cellId,
      priorCells: shown
        .slice(0, Math.max(index, 0))
        .filter(cell => cell.kind === 'node')
        .map(cell => ({ id: cell.cell_id, code: notebook.cellCode(cell) })),
      frameColumns: name => notebook.frameColumns(name),
      cellColumns: () => notebook.cellColumns(cellId)
    }
  }

  /** Shift-Enter runs and moves on, Mod-Enter runs; a cell that cannot be changed gets the look only. */
  function extensionsFor(cell: NotebookCell): Extension[] {
    if (!notebook.isEditable(cell)) return baseExtensions
    let extensions = cellExtensions.get(cell.cell_id)
    if (!extensions) {
      const cellId = cell.cell_id
      const keys = keymap.of([
        { key: 'Shift-Enter', run: () => (runAndAdvance(cellId), true) },
        { key: 'Mod-Enter', run: () => (void notebook.runCell(cellId), true) }
      ])
      const completions = autocompletion({
        override: notebookCompletionSources(() => completionContext(cellId)),
        defaultKeymap: true,
        icons: false
      })
      extensions = [...baseExtensions, completions, Prec.highest(keys), Prec.high(tabKeys)]
      cellExtensions.set(cellId, extensions)
    }
    return extensions
  }

  /** Run the cell and go on to the next one; from the last cell, go on to a new one. */
  function runAndAdvance(cellId: string) {
    void notebook.runCell(cellId)
    const index = notebook.shownCells.findIndex(cell => cell.cell_id === cellId)
    const next = notebook.shownCells.slice(index + 1).find(cell => notebook.isEditable(cell))
    if (next) views.get(next.cell_id)?.focus()
    else addCell(cellId)
  }

  function markSyncError(cellId: string, view: EditorView) {
    const failure = notebook.syncError
    const mark = failure?.cellId === cellId ? { line: failure.line, message: failure.message } : null
    view.dispatch({ effects: setSyncErrorMark.of(mark) })
  }

  function registerView(cellId: string, view: EditorView) {
    views.set(cellId, view)
    markSyncError(cellId, view)
    if (awaitingFocus === cellId) {
      awaitingFocus = null
      view.focus()
      view.dom.scrollIntoView({ block: 'nearest' })
    }
  }

  watch(
    () => notebook.syncError,
    failure => {
      views.forEach((view, cellId) => markSyncError(cellId, view))
      // A refused cell may be out of sight, above the one whose Run was pressed: bring it into view.
      if (!failure) return
      void nextTick(() => {
        const cell = root.value?.querySelector(`[data-cell-id="${CSS.escape(failure.cellId)}"]`)
        cell?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
      })
    }
  )

  // A cell that is gone takes its editor with it.
  watch(
    () => notebook.shownCells,
    cells => {
      const shown = new Set(cells.map(cell => cell.cell_id))
      for (const cellId of [...views.keys()]) {
        if (!shown.has(cellId)) {
          views.delete(cellId)
          cellExtensions.delete(cellId)
        }
      }
    }
  )

  return { addCell, extensionsFor, registerView }
}
