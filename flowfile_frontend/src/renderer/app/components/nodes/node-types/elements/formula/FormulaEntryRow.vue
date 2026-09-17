<template>
  <div
    class="formula-entry"
    :class="{
      'is-active': active,
      'is-collapsed': isCollapsed,
      'is-dragging': dragging,
      'is-drop-before': dropBefore,
      'is-drop-after': dropAfter,
    }"
    @focusin="emit('focus')"
  >
    <div class="entry-head" @click="onHeadClick">
      <template v-if="reorderable">
        <span class="entry-index">{{ index + 1 }}</span>
        <span
          class="entry-grip"
          title="Drag to reorder"
          draggable="true"
          @dragstart="emit('dragstart', $event)"
          >⠿</span
        >
      </template>

      <input
        :value="entry.field.name"
        class="entry-name"
        type="text"
        :style="{ width: nameWidth }"
        placeholder="column name"
        :list="columnListId"
        v-bind="NO_AUTOFILL"
        @input="emit('update-name', ($event.target as HTMLInputElement).value)"
      />
      <datalist :id="columnListId">
        <option v-for="name in columnNames" :key="name" :value="name" />
      </datalist>

      <span
        v-if="isCollapsed"
        class="entry-expr"
        :class="{ 'is-placeholder': summary.placeholder }"
        :title="summary.placeholder ? undefined : summary.text"
        >{{ summary.text }}</span
      >

      <select
        class="entry-type"
        :class="{ 'is-auto': !badgeType }"
        :style="{ width: typeWidth }"
        :value="entry.field.data_type ?? AUTO_DATA_TYPE"
        :title="`Data type: ${entry.field.data_type ?? AUTO_DATA_TYPE}`"
        @change="emit('update-data-type', ($event.target as HTMLSelectElement).value)"
      >
        <option v-for="dataType in dataTypes" :key="dataType" :value="dataType">
          {{ dataType }}
        </option>
      </select>

      <span
        v-if="issue"
        class="entry-flag"
        :class="issue.kind === 'duplicate' ? 'is-warning' : 'is-error'"
        :title="issue.message"
        >{{ issue.kind === "duplicate" ? "⚠" : "●" }}</span
      >
      <span
        v-else-if="duplicateWarning"
        class="entry-flag is-warning"
        title="Another formula writes to this column too; the last one wins."
        >⚠</span
      >

      <template v-if="reorderable">
        <span class="entry-tools">
          <button
            type="button"
            class="entry-button"
            title="Move up"
            :disabled="index === 0"
            @mousedown.prevent
            @click.stop="emit('move-up')"
          >
            ↑
          </button>
          <button
            type="button"
            class="entry-button"
            title="Move down"
            :disabled="index === count - 1"
            @mousedown.prevent
            @click.stop="emit('move-down')"
          >
            ↓
          </button>
          <button
            type="button"
            class="entry-button entry-remove"
            title="Remove this formula"
            @mousedown.prevent
            @click.stop="emit('remove')"
          >
            ✕
          </button>
        </span>
        <button
          type="button"
          class="entry-button entry-chevron"
          :title="isCollapsed ? 'Expand this formula' : 'Collapse this formula'"
          :aria-expanded="!isCollapsed"
          @mousedown.prevent
          @click.stop="emit('toggle-collapsed')"
        >
          {{ isCollapsed ? "›" : "⌄" }}
        </button>
      </template>
    </div>

    <div v-show="!isCollapsed" class="entry-editor">
      <FunctionEditor
        ref="functionEditor"
        :editor-string="entry.function"
        :columns="columnNames"
        :column-types="columnTypes"
        :parameters="parameters"
        :autofocus="autofocus"
        :height="editorHeight"
        @update-editor-string="emit('update-expression', $event)"
      />
      <div v-if="issue" :class="issue.kind === 'duplicate' ? 'entry-warning' : 'entry-issue'">
        {{ issue.message }}
      </div>
      <div v-else-if="duplicateWarning" class="entry-warning">
        Another formula writes to this column too; the last one wins.
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import FunctionEditor from "../../../../../features/designer/editor/FunctionEditor.vue";
import { NO_AUTOFILL } from "../../../../../utils/noAutofill";
import type { FlowParameter } from "../../../../../types/flow.types";
import type { FormulaChainIssue, FormulaInput } from "../../../../../types/node.types";
import { AUTO_DATA_TYPE, entrySummary, type FormulaColumn } from "./formula";

const props = defineProps<{
  entry: FormulaInput;
  index: number;
  count: number;
  /** The schema this entry sees: input columns plus the outputs before it. */
  columns: FormulaColumn[];
  dataTypes: string[];
  parameters: FlowParameter[];
  issue: FormulaChainIssue | null;
  duplicateWarning: boolean;
  active: boolean;
  dragging: boolean;
  dropBefore: boolean;
  dropAfter: boolean;
  collapsed: boolean;
  autofocus: boolean;
  editorHeight: string;
}>();

const emit = defineEmits<{
  (event: "update-name", payload: string): void;
  (event: "update-data-type", payload: string): void;
  (event: "update-expression", payload: string): void;
  (event: "focus"): void;
  (event: "remove"): void;
  (event: "move-up"): void;
  (event: "move-down"): void;
  (event: "dragstart", payload: DragEvent): void;
  (event: "toggle-collapsed"): void;
  (event: "expand"): void;
}>();

const PLACEHOLDER_CHARS = 11;
const MIN_NAME_CHARS = 8;

const functionEditor = ref<InstanceType<typeof FunctionEditor> | null>(null);

// Reordering and collapsing only exist in list mode; a lone row keeps the bare line.
const reorderable = computed(() => props.count > 1);
const isCollapsed = computed(() => props.collapsed && reorderable.value);

const columnListId = computed(() => `formula-columns-${props.index}`);
const columnNames = computed(() => props.columns.map((column) => column.name));

const columnTypes = computed<Record<string, string>>(() => {
  const map: Record<string, string> = {};
  for (const column of props.columns) {
    if (column.data_type) map[column.name] = column.data_type;
  }
  return map;
});

// Sized to its content: a fixed-width input would eat the row and leave the
// collapsed expression with nothing to render into.
const nameWidth = computed(() => {
  const chars = Math.max(props.entry.field.name.length || PLACEHOLDER_CHARS, MIN_NAME_CHARS);
  return `clamp(90px, calc(${chars}ch + 14px), 200px)`;
});

// A native select is as wide as its widest option; size it to the current one.
const typeWidth = computed(() => {
  const value = props.entry.field.data_type ?? AUTO_DATA_TYPE;
  return `calc(${value.length}ch + 18px)`;
});

const summary = computed(() => entrySummary(props.entry));

const badgeType = computed(() => {
  const dataType = props.entry.field.data_type;
  return !dataType || dataType === AUTO_DATA_TYPE ? "" : dataType;
});

const onHeadClick = () => {
  if (isCollapsed.value) emit("expand");
};

const insertTextAtCursor = (text: string) => functionEditor.value?.insertTextAtCursor(text);

/** CodeMirror measures as zero-sized while hidden, so re-measure after an expand. */
const refreshEditor = () => functionEditor.value?.requestMeasure();

defineExpose({ insertTextAtCursor, refreshEditor });
</script>

<style scoped>
.formula-entry {
  border-top: 1px solid var(--color-border-primary);
  padding: 4px 12px;
}

.formula-entry:first-child {
  border-top: none;
}

.formula-entry.is-active {
  box-shadow: inset 2px 0 0 0 var(--color-accent);
}

.formula-entry.is-dragging {
  opacity: 0.4;
}

.formula-entry.is-drop-before {
  box-shadow: inset 0 2px 0 0 var(--color-accent);
}

.formula-entry.is-drop-after {
  box-shadow: inset 0 -2px 0 0 var(--color-accent);
}

.entry-head {
  display: flex;
  align-items: center;
  gap: 6px;
  height: 38px;
}

.entry-index {
  flex-shrink: 0;
  width: 10px;
  font-size: 0.7rem;
  color: var(--color-text-muted);
  text-align: right;
}

.entry-grip {
  flex-shrink: 0;
  color: var(--color-text-muted);
  cursor: grab;
  opacity: 0;
  transition: opacity var(--transition-fast, 0.15s) ease;
}

.entry-grip:active {
  cursor: grabbing;
}

.entry-name {
  flex: 0 0 auto;
  min-width: 0;
  padding: 2px 0;
  font-size: 13px;
  line-height: 1.5;
  font-weight: 500;
  color: var(--color-text-primary);
  background: transparent;
  border: none;
  border-bottom: 1px solid transparent;
  outline: none;
}

.entry-name::placeholder {
  font-weight: 400;
  color: var(--color-text-muted);
}

.entry-name:hover {
  border-bottom-color: var(--color-border-primary);
}

.entry-name:focus {
  border-bottom-color: var(--color-accent);
}

.entry-expr {
  flex: 1 1 0;
  min-width: 0;
  font-size: 13px;
  line-height: 1.5;
  font-family: var(--font-family-mono, monospace);
  color: var(--color-text-tertiary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.entry-expr.is-placeholder {
  font-style: italic;
  color: var(--color-text-muted);
}

.entry-type {
  flex-shrink: 0;
  padding: 1px 4px;
  font-size: 0.7rem;
  color: var(--color-text-secondary);
  background: var(--color-background-tertiary);
  border: 1px solid transparent;
  border-radius: 3px;
  appearance: none;
  cursor: pointer;
}

.entry-type:hover {
  border-color: var(--color-border-primary);
}

.entry-type.is-auto {
  color: var(--color-text-muted);
  background: transparent;
}

.entry-flag {
  flex-shrink: 0;
  font-size: 0.75rem;
  cursor: help;
}

.entry-flag.is-error {
  color: var(--color-danger);
}

.entry-flag.is-warning {
  color: var(--color-text-secondary);
}

.entry-tools {
  display: flex;
  flex-shrink: 0;
  gap: 2px;
  opacity: 0;
  transition: opacity var(--transition-fast, 0.15s) ease;
}

.formula-entry:hover .entry-tools,
.formula-entry:hover .entry-grip,
.formula-entry.is-active .entry-tools,
.formula-entry.is-active .entry-grip,
.entry-tools:focus-within {
  opacity: 1;
}

/* A collapsed row gives its width to the expression: the idle tools take none,
   and a long name yields rather than squeezing the summary out of the line. */
.formula-entry.is-collapsed .entry-tools {
  display: none;
}

.formula-entry.is-collapsed .entry-name {
  flex: 0 1 auto;
  max-width: 45%;
}

.formula-entry.is-collapsed:hover .entry-tools,
.formula-entry.is-collapsed.is-active .entry-tools {
  display: flex;
}

.entry-button {
  width: 18px;
  height: 18px;
  padding: 0;
  font-size: 0.7rem;
  line-height: 1;
  color: var(--color-text-tertiary);
  background: transparent;
  border: none;
  border-radius: 3px;
  cursor: pointer;
}

.entry-button:disabled {
  opacity: 0.3;
  cursor: default;
}

.entry-button:not(:disabled):hover {
  color: var(--color-text-primary);
  background: var(--color-background-tertiary);
}

.entry-remove:not(:disabled):hover {
  color: var(--color-danger);
}

.entry-chevron {
  flex-shrink: 0;
  font-size: 0.8rem;
}

.entry-editor {
  margin: 6px 0 10px 0;
}

.entry-editor :deep(.function-editor-root) {
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
}

.entry-issue {
  margin-top: 6px;
  font-size: 0.75rem;
  color: var(--color-danger);
}

.entry-warning {
  margin-top: 6px;
  font-size: 0.75rem;
  color: var(--color-text-secondary);
}
</style>
