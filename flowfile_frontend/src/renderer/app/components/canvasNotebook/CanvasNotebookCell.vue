<template>
  <div
    class="cn-cell"
    :class="[`cn-cell--${cell.status}`, `cn-cell--${cell.kind}`]"
    :data-cell-id="cell.cellId"
    :data-status="cell.status"
  >
    <div class="cn-cell-bar">
      <span class="cn-cell-label">{{ label }}</span>
      <span v-if="locked" class="cn-cell-lock" :title="cell.reason ?? undefined">
        <span class="material-icons" aria-hidden="true">lock</span>
        {{ cell.status === "unsupported" ? "Unsupported" : "Placeholder" }}
      </span>
    </div>
    <p v-if="locked && cell.reason" class="cn-cell-reason">{{ cell.reason }}</p>
    <div class="cn-cell-editor">
      <codemirror :model-value="cell.base" :extensions="extensions" :autofocus="false" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { Codemirror } from "vue-codemirror";
import { EditorState } from "@codemirror/state";
import { EditorView } from "@codemirror/view";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import type { DockCell } from "../../stores/canvasNotebook-store";

const props = defineProps<{ cell: DockCell }>();

const readOnlyTheme = EditorView.theme({
  "&": { fontSize: "0.8rem" },
  ".cm-content": {
    padding: "0.4rem 0",
    fontFamily: "'Fira Code', 'Monaco', 'Menlo', monospace",
  },
  ".cm-gutters": { fontSize: "0.7rem", minWidth: "2.5rem" },
});

const extensions = [
  python(),
  oneDark,
  readOnlyTheme,
  EditorState.readOnly.of(true),
  EditorView.editable.of(false),
  EditorView.lineWrapping,
];

const locked = computed(() => props.cell.status !== "code");

const label = computed(() => {
  if (props.cell.kind === "imports") return "Imports";
  if (props.cell.kind === "parameters") return "Parameters";
  const ids = props.cell.nodeIds;
  return ids.length === 1 ? `Node ${ids[0]}` : `Nodes ${ids.join(", ")}`;
});
</script>

<style scoped>
.cn-cell {
  border: 1px solid var(--el-border-color-lighter, #ebeef5);
  border-radius: 6px;
  margin-bottom: 8px;
  background: var(--el-bg-color, #fff);
  overflow: hidden;
}
.cn-cell--placeholder,
.cn-cell--unsupported {
  box-shadow: inset 3px 0 0 var(--el-color-warning, #e6a23c);
}
.cn-cell-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 3px 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary, #909399);
}
.cn-cell-label {
  flex: 1;
  font-weight: 500;
}
.cn-cell-lock {
  display: inline-flex;
  align-items: center;
  gap: 2px;
  color: var(--el-color-warning, #e6a23c);
}
.cn-cell-lock .material-icons {
  font-size: 14px;
}
.cn-cell-reason {
  margin: 0;
  padding: 0 8px 4px;
  font-size: 12px;
  color: var(--el-text-color-regular, #606266);
}
.cn-cell-editor {
  border-top: 1px solid var(--el-border-color-lighter, #ebeef5);
}
</style>
