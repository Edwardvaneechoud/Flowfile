<template>
  <div class="cell-wrapper" :class="cellClasses">
    <!-- Gutter: execution count or cell index -->
    <div class="cell-gutter">[{{ cell.output?.execution_count ?? cellIndex + 1 }}]</div>

    <!-- Main content area -->
    <div class="cell-content">
      <!-- Toolbar (shown on hover/focus) -->
      <div class="cell-toolbar">
        <button :disabled="isExecuting" title="Run cell (Shift+Enter)" @click="emit('run-cell')">
          <i class="fa-solid fa-play"></i>
        </button>
        <button :disabled="cellIndex === 0" title="Move up" @click="emit('move-up')">
          <i class="fa-solid fa-chevron-up"></i>
        </button>
        <button :disabled="isLastCell" title="Move down" @click="emit('move-down')">
          <i class="fa-solid fa-chevron-down"></i>
        </button>
        <button :disabled="cellCount <= 1" title="Delete cell" @click="emit('delete')">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>

      <!-- CodeMirror editor -->
      <div class="cell-editor-wrapper">
        <codemirror
          :model-value="cell.code"
          placeholder="# Enter code..."
          :autofocus="false"
          :indent-with-tab="false"
          :tab-size="4"
          :extensions="cellExtensions"
          @update:model-value="(val: string) => emit('update:code', val)"
        />
      </div>

      <!-- Output area -->
      <CellOutput v-if="cell.output" :output="cell.output" />

      <!-- Executing indicator -->
      <div v-if="isExecuting" class="cell-executing">
        <i class="fas fa-spinner fa-spin"></i> Running...
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";
import type { Extension } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { EditorState, Prec } from "@codemirror/state";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { autocompletion, acceptCompletion } from "@codemirror/autocomplete";
import type { CompletionSource } from "@codemirror/autocomplete";
import { indentMore, indentLess } from "@codemirror/commands";

import type { NotebookCell } from "../../../../../types/node.types";
import CellOutput from "./CellOutput.vue";
import { flowfileCompletionVals } from "./flowfileCompletions";

interface Props {
  cell: NotebookCell;
  cellIndex: number;
  isExecuting: boolean;
  isLastCell: boolean;
  cellCount: number;
}

const props = defineProps<Props>();

const emit = defineEmits<{
  (e: "update:code", code: string): void;
  (e: "run-cell"): void;
  (e: "run-cell-and-advance"): void;
  (e: "move-up"): void;
  (e: "move-down"): void;
  (e: "delete"): void;
}>();

// Cell classes (computed)
const cellClasses = computed(() => ({
  "cell--executing": props.isExecuting,
  "cell--error": props.cell.output?.error,
}));

// ─── CodeMirror Extensions ───────────────────────────────────────────────────

const flowfileCompletions: CompletionSource = (context) => {
  const word = context.matchBefore(/\w*/);
  if (word?.from === word?.to && !context.explicit) return null;
  return { from: word?.from ?? 0, options: flowfileCompletionVals };
};

// Theme for compact cell editors
const cellEditorTheme = EditorView.theme({
  "&": {
    fontSize: "0.8rem",
    maxHeight: "350px",
  },
  ".cm-content": {
    minHeight: "40px",
    padding: "0.4rem 0",
    fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
  },
  ".cm-gutters": {
    fontSize: "0.7rem",
    minWidth: "2.5rem",
  },
  ".cm-scroller": {
    overflow: "auto",
  },
});

// Notebook keymaps: Shift+Enter to run, Ctrl/Cmd+Enter to run and advance
const notebookKeymap = keymap.of([
  {
    key: "Shift-Enter",
    run: (): boolean => {
      emit("run-cell");
      return true; // MUST return true to prevent newline insertion
    },
  },
  {
    key: "Mod-Enter", // Ctrl+Enter on Windows/Linux, Cmd+Enter on Mac
    run: (): boolean => {
      emit("run-cell-and-advance");
      return true;
    },
  },
]);

const tabKeymap = keymap.of([
  {
    key: "Tab",
    run: (view: EditorView): boolean => {
      if (acceptCompletion(view)) return true;
      return indentMore(view);
    },
  },
  {
    key: "Shift-Tab",
    run: (view: EditorView): boolean => {
      return indentLess(view);
    },
  },
]);

const cellExtensions: Extension[] = [
  python(),
  oneDark,
  cellEditorTheme,
  EditorState.tabSize.of(4),
  autocompletion({
    override: [flowfileCompletions],
    defaultKeymap: true,
    closeOnBlur: false,
  }),
  Prec.highest(notebookKeymap),
  Prec.high(tabKeymap),
];
</script>

<style scoped>
.cell-wrapper {
  display: flex;
  border: 1px solid transparent;
  border-left: 3px solid transparent;
  transition: border-color 0.15s;
  margin-bottom: 2px;
}

.cell-wrapper:hover,
.cell-wrapper:focus-within {
  border-color: var(--el-border-color);
  border-left-color: var(--el-color-primary);
}

.cell-wrapper.cell--executing {
  border-left-color: var(--el-color-warning);
}

.cell-wrapper.cell--error {
  border-left-color: var(--el-color-danger);
}

.cell-gutter {
  width: 36px;
  flex-shrink: 0;
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding-top: 0.4rem;
  font-family: "Fira Code", monospace;
  font-size: 0.65rem;
  color: var(--el-text-color-placeholder);
  user-select: none;
}

.cell-content {
  flex: 1;
  min-width: 0;
}

.cell-toolbar {
  display: flex;
  align-items: center;
  gap: 0.2rem;
  padding: 0.1rem 0.25rem;
  opacity: 0;
  transition: opacity 0.15s;
  font-size: 0.7rem;
}

.cell-wrapper:hover .cell-toolbar,
.cell-wrapper:focus-within .cell-toolbar {
  opacity: 1;
}

.cell-toolbar button {
  background: none;
  border: none;
  cursor: pointer;
  padding: 0.15rem 0.3rem;
  border-radius: 2px;
  color: var(--el-text-color-secondary);
  font-size: 0.65rem;
  line-height: 1;
}

.cell-toolbar button:hover:not(:disabled) {
  background: var(--el-fill-color);
  color: var(--el-text-color-primary);
}

.cell-toolbar button:disabled {
  opacity: 0.3;
  cursor: not-allowed;
}

.cell-editor-wrapper {
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 3px;
  overflow: hidden;
}

.cell-executing {
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  color: var(--el-color-warning);
}
</style>
