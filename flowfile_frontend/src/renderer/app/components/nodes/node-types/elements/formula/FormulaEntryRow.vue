<template>
  <div
    ref="root"
    class="formula-entry"
    :class="{
      'is-active': active,
      'has-error': !!diagnostic,
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

      <label class="entry-name">
        <span class="entry-control-label">Output column</span>
        <el-autocomplete
          :model-value="entry.field.name"
          class="entry-name-input"
          placeholder="Output column"
          aria-label="Output column name"
          title="Edit the output column name or choose an existing column"
          :fetch-suggestions="suggestColumns"
          :debounce="0"
          v-bind="NO_AUTOFILL"
          @focus="nameEdited = false"
          @input="updateName"
          @select="emit('update-name', String($event.value))"
        >
          <template #suffix
            ><el-icon class="entry-control-chevron"><ArrowDown /></el-icon
          ></template>
        </el-autocomplete>
      </label>

      <label class="entry-type-field">
        <span class="entry-control-label">Data type</span>
        <el-select
          class="entry-type"
          :suffix-icon="ArrowDown"
          aria-label="Output data type"
          :model-value="entry.field.data_type ?? AUTO_DATA_TYPE"
          :title="`Data type: ${entry.field.data_type ?? AUTO_DATA_TYPE}`"
          @change="emit('update-data-type', $event)"
        >
          <el-option
            v-for="dataType in dataTypes"
            :key="dataType"
            :value="dataType"
            :label="dataType"
          />
        </el-select>
      </label>

      <button
        v-if="diagnostic"
        type="button"
        class="entry-flag is-error"
        :title="diagnostic.message"
        :aria-label="diagnostic.message"
        @mousedown.prevent
        @click.stop="emit('expand')"
      >
        1 error
      </button>

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

    <span
      v-if="isCollapsed"
      class="entry-expr"
      :class="{ 'is-placeholder': summary.placeholder, 'has-error': !!diagnostic }"
      :title="summary.placeholder ? undefined : summary.text"
      >{{ summary.text }}</span
    >

    <div v-show="!isCollapsed" class="entry-editor" :class="{ 'has-error': !!diagnostic }">
      <FunctionEditor
        ref="functionEditor"
        :editor-string="entry.function"
        :columns="columnNames"
        :column-types="columnTypes"
        :parameters="parameters"
        :autofocus="autofocus"
        height="auto"
        min-height="96px"
        max-height="400px"
        @update-editor-string="emit('update-expression', $event)"
      />
      <transition name="entry-diagnostic">
        <div v-if="diagnostic" class="entry-diagnostic is-error" role="status">
          <el-icon class="entry-diagnostic-icon"><WarningFilled /></el-icon>
          <span class="entry-diagnostic-message">{{ diagnostic.message }}</span>
          <button
            v-if="diagnostic.suggestion"
            type="button"
            class="entry-diagnostic-fix"
            :title="`Replace [${diagnostic.suggestion.from}] with [${diagnostic.suggestion.to}]`"
            @mousedown.prevent
            @click="applySuggestion(diagnostic.suggestion)"
          >
            Use [{{ diagnostic.suggestion.to }}]
          </button>
        </div>
      </transition>
      <div v-if="overwrites" class="entry-note">
        Overwrites <code>{{ entry.field.name.trim() }}</code> from formula {{ overwrites }}
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import { ElAutocomplete, ElSelect, ElOption, ElIcon } from "element-plus";
import { ArrowDown, WarningFilled } from "@element-plus/icons-vue";
import FunctionEditor from "../../../../../features/designer/editor/FunctionEditor.vue";
import { NO_AUTOFILL } from "../../../../../utils/noAutofill";
import type { FlowParameter } from "../../../../../types/flow.types";
import type {
  FormulaChainIssue,
  FormulaChainSuggestion,
  FormulaInput,
} from "../../../../../types/node.types";
import {
  AUTO_DATA_TYPE,
  entrySummary,
  replaceColumnReference,
  type FormulaColumn,
} from "./formula";

const props = defineProps<{
  entry: FormulaInput;
  index: number;
  count: number;
  /** The schema this entry sees: input columns plus the outputs before it. */
  columns: FormulaColumn[];
  dataTypes: string[];
  parameters: FlowParameter[];
  issue: FormulaChainIssue | null;
  /** 1-based number of the earlier formula whose output column this row overwrites. */
  overwrites: number | null;
  active: boolean;
  dragging: boolean;
  dropBefore: boolean;
  dropAfter: boolean;
  collapsed: boolean;
  autofocus: boolean;
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

const nameEdited = ref(false);
const updateName = (value: string | number) => {
  nameEdited.value = true;
  emit("update-name", String(value));
};
const suggestColumns = (query: string, done: (items: { value: string }[]) => void) => {
  const search = nameEdited.value ? query.toLowerCase() : "";
  done(
    columnNames.value
      .filter((name) => name.toLowerCase().includes(search))
      .map((value) => ({ value })),
  );
};

const functionEditor = ref<InstanceType<typeof FunctionEditor> | null>(null);

// Reordering and collapsing only exist in list mode; a lone row keeps the bare line.
const reorderable = computed(() => props.count > 1);
const isCollapsed = computed(() => props.collapsed && reorderable.value);

const columnNames = computed(() => props.columns.map((column) => column.name));

const columnTypes = computed<Record<string, string>>(() => {
  const map: Record<string, string> = {};
  for (const column of props.columns) {
    if (column.data_type) map[column.name] = column.data_type;
  }
  return map;
});

const summary = computed(() => entrySummary(props.entry));

// A duplicate output is a deliberate overwrite, noted quietly via `overwrites`, never an error.
const diagnostic = computed(() =>
  props.issue && props.issue.kind !== "duplicate"
    ? { message: props.issue.message, suggestion: props.issue.suggestion ?? null }
    : null,
);

/** Goes through the editor so the fix is one undo step; the string path covers a missing view. */
const applySuggestion = ({ from, to }: FormulaChainSuggestion) => {
  if (functionEditor.value) functionEditor.value.replaceText(`[${from}]`, `[${to}]`);
  else emit("update-expression", replaceColumnReference(props.entry.function, from, to));
};

const root = ref<HTMLElement | null>(null);
const scrollIntoView = () => root.value?.scrollIntoView({ block: "nearest", behavior: "smooth" });

const onHeadClick = () => {
  if (isCollapsed.value) emit("expand");
};

const insertTextAtCursor = (text: string) => functionEditor.value?.insertTextAtCursor(text);

/** CodeMirror measures as zero-sized while hidden, so re-measure after an expand. */
const refreshEditor = () => functionEditor.value?.requestMeasure();

defineExpose({ insertTextAtCursor, refreshEditor, scrollIntoView });
</script>

<style scoped>
.formula-entry {
  display: flex;
  flex-direction: column;
  flex: 1 0 auto;
  border-top: 1px solid var(--color-border-primary);
  padding: 4px 12px;
}

.formula-entry.is-collapsed {
  flex-grow: 0;
}

.formula-entry:first-child {
  border-top: none;
}

.formula-entry.is-active {
  box-shadow: inset 2px 0 0 0 var(--color-accent);
}

.formula-entry.has-error {
  box-shadow: inset 2px 0 0 0 var(--color-danger);
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
  flex-wrap: wrap;
  min-height: 60px;
  padding: 6px 0;
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
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1 1 160px;
  width: 240px;
  max-width: 240px;
  min-width: 120px;
}

.entry-name-input {
  width: 100%;
}

.entry-control-label {
  font-size: 11px;
  line-height: 1.2;
  color: var(--color-text-secondary);
}

.entry-type-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 0 0 112px;
}

.entry-name :deep(.el-input__wrapper),
.entry-type :deep(.el-select__wrapper) {
  box-sizing: border-box;
  height: 28px;
  min-height: 28px;
  padding: 0 8px;
  border: none;
  border-radius: 4px;
  background: var(--color-background-secondary);
  box-shadow: 0 0 0 1px var(--color-border-primary) inset;
  font-family: inherit;
  font-size: 12px;
}

.entry-name :deep(.el-input__inner),
.entry-type :deep(.el-select__selected-item) {
  height: 26px;
  line-height: 26px;
  font-family: inherit;
  font-size: 12px;
  color: var(--color-text-primary);
}

.entry-name :deep(.el-input__wrapper:hover),
.entry-type :deep(.el-select__wrapper:hover) {
  box-shadow: 0 0 0 1px var(--color-text-muted) inset;
}

.entry-name :deep(.el-input__wrapper.is-focus),
.entry-type :deep(.el-select__wrapper.is-focused) {
  box-shadow: 0 0 0 1px var(--color-accent) inset;
}

.entry-control-chevron,
.entry-type :deep(.el-select__caret) {
  font-size: 12px;
  color: var(--color-text-muted);
}

.entry-expr {
  display: block;
  margin: 6px 0 10px;
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

.entry-expr.has-error {
  color: var(--color-danger);
}

.entry-type {
  width: 100%;
}

.entry-flag {
  flex-shrink: 0;
  margin-left: auto;
  padding: 1px 7px;
  font-size: 11px;
  line-height: 1.4;
  font-weight: 500;
  white-space: nowrap;
  border: none;
  border-radius: 10px;
  cursor: pointer;
}

/* The token tints (red-100 / amber-100) wash out on a white drawer; mix a stronger band locally. */
.entry-flag.is-error {
  color: var(--color-danger-dark);
  background: color-mix(in srgb, var(--color-danger) 20%, var(--color-background-primary));
}

.entry-tools {
  margin-left: auto;
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

.entry-flag + .entry-tools {
  margin-left: 0;
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
  display: flex;
  flex-direction: column;
  flex: 1;
  margin: 6px 0 10px 0;
}

.entry-editor :deep(.function-editor-root) {
  flex: 1;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
}

.entry-editor :deep(.cm-editor) {
  flex: 1;
}

/* The strip hangs off the editor's bottom edge, so the editor gives up its lower corners. */
.entry-editor.has-error :deep(.function-editor-root) {
  border-color: var(--color-danger);
  border-bottom-left-radius: 0;
  border-bottom-right-radius: 0;
}

.entry-diagnostic {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: -1px;
  padding: 6px 10px;
  font-size: 12px;
  line-height: 1.4;
  border: 1px solid;
  border-radius: 0 0 4px 4px;
  overflow: hidden;
}

.entry-diagnostic.is-error {
  color: var(--color-danger-dark);
  background: color-mix(in srgb, var(--color-danger) 20%, var(--color-background-primary));
  border-color: var(--color-danger);
}

.entry-diagnostic-icon {
  flex-shrink: 0;
  font-size: 14px;
}

.entry-diagnostic.is-error .entry-diagnostic-icon {
  color: var(--color-danger);
}

.entry-note {
  margin-top: 6px;
  font-size: 11px;
  line-height: 1.4;
  color: var(--color-text-muted);
}

.entry-note code {
  font-family: var(--font-family-mono, monospace);
  color: var(--color-text-tertiary);
}

.entry-diagnostic-message {
  min-width: 0;
  flex: 1;
  overflow-wrap: anywhere;
}

.entry-diagnostic-fix {
  flex-shrink: 0;
  padding: 1px 8px;
  font-size: 11px;
  font-weight: 500;
  font-family: var(--font-family-mono, monospace);
  white-space: nowrap;
  color: var(--color-danger-dark);
  background: var(--color-background-primary);
  border: 1px solid var(--color-danger);
  border-radius: 10px;
  cursor: pointer;
}

.entry-diagnostic-fix:hover {
  color: var(--color-background-primary);
  background: var(--color-danger);
}

.entry-diagnostic-enter-active,
.entry-diagnostic-leave-active {
  max-height: 120px;
  transition:
    max-height var(--transition-normal, 200ms) var(--transition-timing, ease),
    opacity var(--transition-normal, 200ms) var(--transition-timing, ease),
    padding var(--transition-normal, 200ms) var(--transition-timing, ease);
}

.entry-diagnostic-enter-from,
.entry-diagnostic-leave-to {
  max-height: 0;
  padding-top: 0;
  padding-bottom: 0;
  opacity: 0;
}
</style>
