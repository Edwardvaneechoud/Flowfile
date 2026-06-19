<template>
  <div
    class="nb-cell"
    :class="[`nb-cell--${cell.cellType}`, { running: cell.execState === 'running' }]"
  >
    <!-- Cell toolbar -->
    <div class="nb-cell-bar">
      <el-select
        v-if="allowedTypes.length > 1"
        :model-value="cell.cellType"
        size="small"
        style="width: 110px"
        @change="(v: CellType) => emit('update:type', v)"
      >
        <el-option v-for="t in allowedTypes" :key="t" :label="TYPE_LABELS[t]" :value="t" />
      </el-select>
      <span v-else class="nb-cell-type-badge">{{ TYPE_LABELS[cell.cellType] }}</span>

      <el-button
        size="small"
        type="primary"
        :loading="cell.execState === 'running'"
        @click="emit('run')"
      >
        <i
          v-if="cell.execState !== 'running'"
          class="fa-solid fa-play"
          style="margin-right: 4px"
        ></i>
        {{ cell.cellType === "markdown" ? "Render" : "Run" }}
      </el-button>

      <div class="nb-cell-bar-spacer"></div>

      <el-tooltip content="Move up" :show-after="400">
        <el-button size="small" text :disabled="index === 0" @click="emit('move', -1)">
          <i class="fa-solid fa-arrow-up"></i>
        </el-button>
      </el-tooltip>
      <el-tooltip content="Move down" :show-after="400">
        <el-button size="small" text :disabled="index === cellCount - 1" @click="emit('move', 1)">
          <i class="fa-solid fa-arrow-down"></i>
        </el-button>
      </el-tooltip>
      <el-tooltip content="Delete cell" :show-after="400">
        <el-button size="small" text @click="emit('remove')">
          <i class="fa-solid fa-trash"></i>
        </el-button>
      </el-tooltip>
    </div>

    <!-- Editor -->
    <div class="nb-cell-editor">
      <!-- Markdown preview (double-click to edit). Content is sanitised via
           DOMPurify in sanitiseMarkdown before reaching v-html. -->
      <!-- eslint-disable vue/no-v-html -->
      <div
        v-if="cell.cellType === 'markdown' && !cell.editing"
        class="nb-md-rendered"
        @dblclick="emit('update:editing', true)"
        v-html="cell.renderedHtml || '<em>Empty markdown cell — double-click to edit</em>'"
      ></div>
      <!-- eslint-enable vue/no-v-html -->
      <el-input
        v-else-if="cell.cellType === 'markdown'"
        :model-value="cell.code"
        type="textarea"
        :autosize="{ minRows: 3 }"
        placeholder="# Markdown — Render (Shift+Enter) to preview"
        @update:model-value="(v: string) => emit('update:code', v)"
        @keydown.shift.enter.prevent="emit('run')"
      />
      <!-- Python code -->
      <codemirror
        v-else
        :model-value="cell.code"
        placeholder="# Python — Shift+Enter to run"
        :style="{ minHeight: '60px' }"
        :indent-with-tab="false"
        :tab-size="4"
        :extensions="extensions"
        @update:model-value="(v: string) => emit('update:code', v)"
      />
    </div>

    <!-- Output -->
    <CellOutput v-if="cell.cellType === 'python' && cell.output" :output="cell.output" />
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { Codemirror } from "vue-codemirror";
import { keymap } from "@codemirror/view";
import { Extension, EditorState, Prec } from "@codemirror/state";
import { python, pythonLanguage } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { autocompletion } from "@codemirror/autocomplete";
import CellOutput from "../nodes/node-types/elements/pythonScript/CellOutput.vue";
import { bodyTooltips } from "@/utils/codemirrorTooltips";
import {
  catalogRefChainCompletions,
  createNamedInputCompletions,
  createPolarsExprCompletions,
  createRefVariableCompletions,
  createScopeCompletions,
  createUpstreamColumnCompletions,
  flowfileApiCompletions,
  globalIdentifierCompletions,
  polarsModuleCompletions,
} from "../nodes/node-types/elements/pythonScript/flowfileCompletions";
import type { CellType, NotebookCellModel } from "./types";

const props = defineProps<{
  cell: NotebookCellModel;
  index: number;
  cellCount: number;
  allowedTypes?: CellType[];
  /** Code of cells before this one, for scope/ref completions. */
  priorCellCodes?: string[];
}>();

const emit = defineEmits<{
  (e: "run"): void;
  (e: "update:code", code: string): void;
  (e: "update:type", cellType: CellType): void;
  (e: "update:editing", editing: boolean): void;
  (e: "move", direction: -1 | 1): void;
  (e: "remove"): void;
}>();

const allowedTypes = computed<CellType[]>(() => props.allowedTypes ?? ["python", "markdown"]);

const TYPE_LABELS: Record<CellType, string> = {
  python: "Python",
  markdown: "Markdown",
};

const runKeymap = keymap.of([
  { key: "Shift-Enter", run: () => (emit("run"), true) },
  { key: "Mod-Enter", run: () => (emit("run"), true) },
]);

// Same completion stack as the graph notebook cell. The flow-only sources
// (named inputs, upstream columns) get empty getters since a catalog notebook
// has no flow graph; flowfile_ctx + Polars + prior-cell-scope all work standalone.
// Getters read props live, so the extension array is built once (no churn).
const getPrior = () => props.priorCellCodes ?? [];
const extensions: Extension[] = [
  python(),
  pythonLanguage.data.of({ autocomplete: flowfileApiCompletions }),
  pythonLanguage.data.of({ autocomplete: globalIdentifierCompletions }),
  pythonLanguage.data.of({ autocomplete: catalogRefChainCompletions }),
  pythonLanguage.data.of({ autocomplete: createRefVariableCompletions(getPrior) }),
  pythonLanguage.data.of({ autocomplete: polarsModuleCompletions }),
  pythonLanguage.data.of({ autocomplete: createPolarsExprCompletions(getPrior) }),
  pythonLanguage.data.of({ autocomplete: createNamedInputCompletions(() => []) }),
  pythonLanguage.data.of({ autocomplete: createUpstreamColumnCompletions(() => []) }),
  pythonLanguage.data.of({ autocomplete: createScopeCompletions(getPrior) }),
  oneDark,
  EditorState.tabSize.of(4),
  autocompletion({ defaultKeymap: true, closeOnBlur: false }),
  bodyTooltips(),
  Prec.highest(runKeymap),
];
</script>

<style scoped>
.nb-cell {
  border: 1px solid var(--el-border-color-lighter, #e4e7ed);
  border-radius: 6px;
  margin-bottom: 12px;
  background: var(--el-bg-color, #fff);
  overflow: hidden;
}
.nb-cell.running {
  border-color: var(--el-color-primary, #409eff);
}
.nb-cell-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 8px;
  background: var(--el-fill-color-light, #f5f7fa);
  border-bottom: 1px solid var(--el-border-color-lighter, #e4e7ed);
}
.nb-cell-bar-spacer {
  flex: 1;
}
.nb-cell-type-badge {
  font-size: 12px;
  font-weight: 600;
  color: var(--el-text-color-secondary, #909399);
  width: 110px;
}
.nb-cell-editor {
  padding: 4px;
}
.nb-md-rendered {
  padding: 8px 12px;
  cursor: text;
  line-height: 1.5;
}
</style>
