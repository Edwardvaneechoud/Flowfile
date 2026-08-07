<template>
  <el-dialog
    :model-value="true"
    fullscreen
    append-to-body
    class="labeling-dialog"
    :show-close="false"
    :close-on-click-modal="false"
    :close-on-press-escape="false"
  >
    <template #header>
      <div class="labeling-header">
        <div class="labeling-title">
          <i class="fa-solid fa-tags"></i>
          <span>Label rows</span>
          <span v-if="phase === 'labeling'" class="progress-chip">
            {{ progress.labelled }} / {{ progress.total }} labelled
          </span>
        </div>
        <el-button size="small" @click="emit('close')">
          {{ phase === "labeling" ? "Done" : "Cancel" }}
        </el-button>
      </div>
    </template>

    <!-- Setup: pick target column + classes -->
    <div v-if="phase === 'setup'" class="labeling-setup">
      <h3>Where do the labels go?</h3>
      <el-radio-group v-model="targetMode">
        <el-radio value="new">Create a new label column</el-radio>
        <el-radio value="existing" :disabled="existingTargets.length === 0">
          Use an existing column
        </el-radio>
      </el-radio-group>
      <el-input
        v-if="targetMode === 'new'"
        v-model="newColumnName"
        placeholder="label"
        class="target-input"
      >
        <template #prepend>Column name</template>
      </el-input>
      <el-select
        v-else
        v-model="existingTarget"
        filterable
        placeholder="Pick a column"
        class="target-input"
        @change="prefillClasses"
      >
        <el-option v-for="col in existingTargets" :key="col" :label="col" :value="col" />
      </el-select>

      <h3>Label classes</h3>
      <p class="setup-hint">
        Number keys 1–{{ Math.min(classes.length || 1, 9) }} assign a class and advance to the
        next row. Space skips, ⌫ undoes, arrow keys navigate, Esc finishes.
      </p>
      <div class="class-chips">
        <el-tag
          v-for="(cls, i) in classes"
          :key="cls"
          closable
          class="class-chip"
          @close="classes.splice(i, 1)"
        >
          <b v-if="i < 9">{{ i + 1 }}</b> {{ cls }}
        </el-tag>
        <el-input
          v-model="newClass"
          placeholder="Add a class + Enter"
          class="class-input"
          @keydown.enter="addClass"
        />
      </div>
      <p class="setup-hint">
        Tip: include an explicit “can't tell” class — recording unlabelable rows is a real
        signal, forcing a choice just manufactures noise.
      </p>
      <el-button
        type="primary"
        class="start-btn"
        :disabled="!canStart"
        @click="startLabeling"
      >
        Start labelling
      </el-button>
    </div>

    <!-- Labelling: one row at a time -->
    <div v-else-if="currentRow" class="labeling-main">
      <div class="labeling-card">
        <div class="row-position">
          Row {{ pointer + 1 }} of {{ order.length }}
          <span v-if="currentLabel" class="current-label">
            labelled: <b>{{ currentLabel }}</b>
          </span>
        </div>
        <div class="row-fields">
          <div v-for="field in displayFields" :key="field.name" class="row-field">
            <span class="field-name">{{ field.name }}</span>
            <span class="field-value" :class="{ 'long-text': field.long }">{{ field.value }}</span>
          </div>
        </div>
      </div>

      <div class="class-buttons">
        <button
          v-for="cls in labelClasses"
          :key="cls.value"
          type="button"
          class="class-btn"
          :class="{ active: currentLabel === cls.value }"
          @mousedown.prevent
          @click="assign(cls.value)"
        >
          <span v-if="cls.shortcut !== null" class="shortcut-key">{{ cls.shortcut }}</span>
          {{ cls.value }}
        </button>
      </div>

      <div class="labeling-controls">
        <el-button size="small" :disabled="pointer === 0" @click="goto(pointer - 1)">
          ← Previous
        </el-button>
        <el-button size="small" :disabled="undoStack.length === 0" @click="undo">
          ⌫ Undo
        </el-button>
        <el-button size="small" @click="advance">Skip (space) →</el-button>
      </div>

      <div class="per-class-counts">
        <span v-for="(count, cls) in progress.perClass" :key="cls" class="count-chip">
          {{ cls }}: {{ count }}
        </span>
      </div>
    </div>

    <!-- Completion -->
    <div v-else class="labeling-done">
      <i class="fa-solid fa-circle-check"></i>
      <h3>All rows visited</h3>
      <p>{{ progress.labelled }} of {{ progress.total }} rows labelled.</p>
      <p class="setup-hint">Close this view and hit “Save changes” to write the labels.</p>
      <div class="done-actions">
        <el-button @click="restart">Review from start</el-button>
        <el-button type="primary" @click="emit('close')">Done</el-button>
      </div>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import type { NewColumnSpec } from "../../types";
import {
  applyCellEdit,
  buildLabelClasses,
  distinctColumnValues,
  labelingOrder,
  labelingProgress,
  type EditSession,
} from "./tableEditing";

const props = defineProps<{ session: EditSession }>();

const emit = defineEmits<{
  (e: "close"): void;
  (e: "add-column", spec: NewColumnSpec): void;
}>();

const phase = ref<"setup" | "labeling">("setup");
const targetMode = ref<"new" | "existing">("new");
const newColumnName = ref("label");
const existingTarget = ref("");
const classes = ref<string[]>([]);
const newClass = ref("");
const targetColumn = ref("");
const order = ref<number[]>([]);
const pointer = ref(0);
const undoStack = ref<Array<{ orderIndex: number; previous: unknown }>>([]);

// Key columns are excluded: writing labels into a merge key would silently turn
// updates into inserts (the grid locks key cells for the same reason).
const existingTargets = computed(() =>
  props.session.columns
    .filter((c) => c.editable && !props.session.keyColumns.includes(c.name))
    .map((c) => c.name),
);

const canStart = computed(() => {
  if (classes.value.length === 0) return false;
  if (targetMode.value === "existing") return !!existingTarget.value;
  const name = newColumnName.value.trim();
  if (!name || /[\s,;{}()=]/.test(name)) return false;
  return !props.session.columns.some((c) => c.name === name);
});

const labelClasses = computed(() => buildLabelClasses(classes.value));

const progress = computed(() => labelingProgress(props.session.rows, targetColumn.value));

const currentRow = computed(() =>
  pointer.value < order.value.length ? props.session.rows[order.value[pointer.value]] : null,
);

const currentLabel = computed(() => {
  const value = currentRow.value?.cells[targetColumn.value];
  return value === null || value === undefined || value === "" ? null : String(value);
});

const displayFields = computed(() => {
  const row = currentRow.value;
  if (!row) return [];
  return props.session.columns
    .filter((c) => c.name !== targetColumn.value)
    .map((c) => {
      const raw = row.cells[c.name];
      const value = raw === null || raw === undefined ? "—" : String(raw);
      return { name: c.name, value, long: value.length > 120 };
    });
});

function addClass() {
  const value = newClass.value.trim();
  if (value && !classes.value.includes(value)) classes.value.push(value);
  newClass.value = "";
}

function prefillClasses(column: string) {
  if (classes.value.length === 0) {
    classes.value = distinctColumnValues(props.session.rows, column, 9);
  }
}

function startLabeling() {
  if (targetMode.value === "new") {
    const name = newColumnName.value.trim();
    emit("add-column", { name, dtype: "String" });
    targetColumn.value = name;
  } else {
    targetColumn.value = existingTarget.value;
  }
  order.value = labelingOrder(props.session.rows, targetColumn.value);
  pointer.value = 0;
  undoStack.value = [];
  phase.value = "labeling";
}

function assign(classValue: string) {
  const row = currentRow.value;
  if (!row) return;
  undoStack.value.push({
    orderIndex: pointer.value,
    previous: row.cells[targetColumn.value] ?? null,
  });
  applyCellEdit(row, targetColumn.value, classValue);
  advance();
}

function advance() {
  if (pointer.value < order.value.length) pointer.value += 1;
}

function goto(index: number) {
  pointer.value = Math.max(0, Math.min(index, order.value.length));
}

function undo() {
  const last = undoStack.value.pop();
  if (!last) return;
  const row = props.session.rows[order.value[last.orderIndex]];
  if (row) applyCellEdit(row, targetColumn.value, last.previous);
  pointer.value = last.orderIndex;
}

function restart() {
  pointer.value = 0;
}

function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName.toLowerCase();
  return tag === "input" || tag === "textarea" || target.isContentEditable;
}

function onKeydown(event: KeyboardEvent) {
  if (phase.value !== "labeling" || isTypingTarget(event.target)) {
    if (event.key === "Escape") emit("close");
    return;
  }
  if (event.key === "Escape") {
    emit("close");
    return;
  }
  if (/^[1-9]$/.test(event.key)) {
    const index = Number(event.key) - 1;
    if (index < classes.value.length) {
      event.preventDefault();
      assign(classes.value[index]);
    }
    return;
  }
  if (event.key === " " || event.code === "Space") {
    event.preventDefault();
    advance();
    return;
  }
  switch (event.key) {
    case "ArrowRight":
      advance();
      break;
    case "ArrowLeft":
      goto(pointer.value - 1);
      break;
    case "Backspace":
    case "z":
      event.preventDefault();
      undo();
      break;
  }
}

onMounted(() => window.addEventListener("keydown", onKeydown));
onBeforeUnmount(() => window.removeEventListener("keydown", onKeydown));
</script>

<style scoped>
.labeling-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
}

.labeling-title {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  font-size: var(--font-size-lg, 16px);
  font-weight: 600;
  color: var(--color-text-primary);
}

.progress-chip {
  font-size: var(--font-size-xs, 11px);
  font-weight: 400;
  padding: 2px 8px;
  border-radius: 10px;
  background: var(--color-background-soft, #f0f2f5);
  color: var(--color-text-secondary);
}

.labeling-setup {
  max-width: 560px;
  margin: 0 auto;
  padding: var(--spacing-6, 32px) var(--spacing-4, 16px);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3, 12px);
}

.target-input {
  width: 100%;
}

.setup-hint {
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm, 13px);
  margin: 0;
}

.class-chips {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2, 8px);
  align-items: center;
}

.class-chip b {
  margin-right: 4px;
}

.class-input {
  width: 180px;
}

.start-btn {
  align-self: flex-start;
}

.labeling-main {
  max-width: 860px;
  margin: 0 auto;
  padding: var(--spacing-4, 16px);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4, 16px);
}

.labeling-card {
  border: 1px solid var(--color-border, #dcdfe6);
  border-radius: 8px;
  padding: var(--spacing-4, 16px);
  background: var(--color-background, #fff);
}

.row-position {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
  margin-bottom: var(--spacing-3, 12px);
  display: flex;
  gap: var(--spacing-3, 12px);
}

.current-label {
  color: var(--color-success, #67c23a);
}

.row-fields {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2, 8px);
  max-height: 46vh;
  overflow-y: auto;
}

.row-field {
  display: flex;
  gap: var(--spacing-3, 12px);
  align-items: baseline;
}

.field-name {
  flex: 0 0 160px;
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
  text-align: right;
}

.field-value {
  font-size: var(--font-size-md, 14px);
  color: var(--color-text-primary);
  overflow-wrap: anywhere;
}

.field-value.long-text {
  font-size: var(--font-size-lg, 16px);
  line-height: 1.55;
}

.class-buttons {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2, 8px);
}

.class-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  padding: 10px 16px;
  font-size: var(--font-size-md, 14px);
  border: 1px solid var(--color-border, #dcdfe6);
  border-radius: 8px;
  background: var(--color-background, #fff);
  color: var(--color-text-primary);
  cursor: pointer;
}

.class-btn:hover {
  border-color: var(--color-primary, #409eff);
}

.class-btn.active {
  border-color: var(--color-success, #67c23a);
  background: rgba(103, 194, 58, 0.08);
}

.shortcut-key {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border-radius: 4px;
  border: 1px solid var(--color-border, #dcdfe6);
  background: var(--color-background-soft, #f5f7fa);
  font-size: var(--font-size-xs, 11px);
  font-weight: 700;
}

.labeling-controls {
  display: flex;
  gap: var(--spacing-2, 8px);
}

.per-class-counts {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-2, 8px);
}

.count-chip {
  font-size: var(--font-size-xs, 11px);
  padding: 2px 8px;
  border-radius: 10px;
  background: var(--color-background-soft, #f0f2f5);
  color: var(--color-text-secondary);
}

.labeling-done {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: var(--spacing-3, 12px);
  padding: var(--spacing-8, 48px);
}

.labeling-done i {
  font-size: 40px;
  color: var(--color-success, #67c23a);
}

.done-actions {
  display: flex;
  gap: var(--spacing-2, 8px);
}
</style>
