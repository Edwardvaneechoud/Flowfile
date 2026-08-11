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
          <span v-if="phase === 'labeling' && refreshNotice" class="refresh-badge">
            suggestions updated
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
        Number keys 1–{{ Math.min(classes.length || 1, 9) }} assign a class and advance to the next
        row. Space skips, ⌫ undoes, arrow keys navigate, Esc finishes.
      </p>
      <div class="class-chips">
        <el-tag
          v-for="(cls, i) in classes"
          :key="cls"
          closable
          class="class-chip"
          @close="removeClass(i)"
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
      <p v-if="classNote" class="setup-note">{{ classNote }}</p>
      <p class="setup-hint">
        Tip: include an explicit “can't tell” class — recording unlabelable rows is a real signal,
        forcing a choice just manufactures noise.
      </p>

      <h3>Suggestions from a prediction table (optional)</h3>
      <p class="setup-hint">
        Join a table of model predictions on this session's key column(s) to pre-fill a suggested
        class per row. Enter accepts the suggestion. Nothing here blocks labelling.
      </p>
      <div class="suggestion-row">
        <span class="suggestion-label">Prediction table</span>
        <el-select
          v-model="predictionTableId"
          filterable
          clearable
          placeholder="No suggestions"
          class="suggestion-select"
          :loading="predLoading"
          @change="onPredictionTablePick"
        >
          <el-option
            v-for="option in predictionTables"
            :key="option.id"
            :label="option.qualified_name ?? option.full_table_name ?? option.name"
            :value="option.id"
          />
        </el-select>
      </div>
      <p v-if="suggestionNote" class="setup-note">{{ suggestionNote }}</p>
      <p v-if="serverNote" class="setup-note">{{ serverNote }}</p>
      <div v-if="suggestionError" class="suggestion-warning">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <span>{{ suggestionError }}</span>
        <el-button size="small" @click="retryPredictionLoad">Retry</el-button>
      </div>
      <p v-else-if="joinError" class="suggestion-warning">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <span>{{ joinError }}</span>
      </p>
      <template v-else-if="predColumns.length > 0">
        <div class="suggestion-row">
          <span class="suggestion-label">Class column</span>
          <el-select
            v-model="classColumn"
            filterable
            clearable
            placeholder="Pick a column"
            class="suggestion-select"
          >
            <el-option v-for="col in classColumnOptions" :key="col" :label="col" :value="col" />
          </el-select>
        </div>
        <div class="suggestion-row">
          <span class="suggestion-label">Probability column</span>
          <el-select
            v-model="probabilityColumn"
            filterable
            clearable
            placeholder="None"
            class="suggestion-select"
          >
            <el-option
              v-for="col in probabilityColumnOptions"
              :key="col"
              :label="col"
              :value="col"
            />
          </el-select>
        </div>
        <div v-if="discoveredClasses.length > 0" class="discovered-classes">
          <span class="setup-note">
            Classes found in “{{ classColumn }}”: {{ discoveredClasses.join(", ") }} ({{
              discoveredClasses.length
            }})
          </span>
          <button
            v-if="!classesMatchDiscovered"
            type="button"
            class="text-btn"
            @mousedown.prevent
            @click="adoptDiscoveredClasses"
          >
            Use these as my classes
          </button>
        </div>
        <el-checkbox v-if="probabilityColumn" v-model="leastConfidentFirst">
          Least confident first
        </el-checkbox>
      </template>

      <div class="start-row">
        <el-button type="primary" :disabled="!canStart || startPending" @click="startLabeling">
          Start labelling
        </el-button>
        <span v-if="suggestionsResolving" class="setup-note">checking for predictions…</span>
      </div>
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
        <div class="fields-toolbar">
          <button type="button" class="text-btn" @mousedown.prevent @click="collapseAll">
            Collapse all
          </button>
          <span class="text-btn-sep">·</span>
          <button type="button" class="text-btn" @mousedown.prevent @click="expandAll">
            Expand all
          </button>
        </div>
        <div class="row-fields">
          <template v-for="field in displayFields" :key="field.name">
            <button
              v-if="field.collapsed"
              type="button"
              class="collapsed-field"
              @mousedown.prevent
              @click="toggleColumn(field.name)"
            >
              <i class="fa-solid fa-chevron-right"></i>
              <span>{{ field.name }}</span>
            </button>
            <div v-else class="row-field">
              <button
                type="button"
                class="field-toggle"
                @mousedown.prevent
                @click="toggleColumn(field.name)"
              >
                <i class="fa-solid fa-chevron-down"></i>
              </button>
              <span class="field-name">{{ field.name }}</span>
              <span class="field-value" :class="{ 'long-text': field.long }">
                {{ field.value }}
              </span>
            </div>
          </template>
        </div>
        <div v-if="currentSuggestion" class="suggestion-line">
          <i class="fa-solid fa-wand-magic-sparkles"></i>
          <template v-if="suggestedClassKnown">
            <span>
              model: <b>{{ currentSuggestion.suggested }}</b>
              <template v-if="confidenceText"> ({{ confidenceText }})</template>
            </span>
            <span class="enter-hint">Enter accepts</span>
          </template>
          <template v-else>
            <span>model:</span>
            <span class="unknown-chip">{{ currentSuggestion.suggested }}</span>
            <span v-if="confidenceText" class="enter-hint">({{ confidenceText }})</span>
            <button type="button" class="mini-btn" @mousedown.prevent @click="addSuggestedClass">
              + Add class
            </button>
            <span class="enter-hint">not in your class list — Enter does nothing yet</span>
          </template>
        </div>
      </div>

      <div class="class-buttons">
        <button
          v-for="cls in labelClasses"
          :key="cls.value"
          type="button"
          class="class-btn"
          :class="{ active: currentLabel === cls.value, suggested: suggestedValue === cls.value }"
          @mousedown.prevent
          @click="assign(cls.value)"
        >
          <span v-if="cls.shortcut !== null" class="shortcut-key">{{ cls.shortcut }}</span>
          {{ cls.value }}
          <span v-if="probabilityFor(cls.value)" class="class-prob">
            {{ probabilityFor(cls.value) }}
          </span>
          <span v-if="suggestedValue === cls.value" class="suggested-chip">
            suggested <kbd>⏎</kbd>
          </span>
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
        <span v-if="suggestionActive" class="count-chip">
          {{ coverage.covered }} / {{ coverage.total }} with a suggestion
        </span>
        <span v-if="suggestionActive && disagreement > 0" class="count-chip">
          {{ disagreement }} disagree with model
        </span>
        <span v-if="unknownClassRows > 0" class="count-chip">
          {{ unknownClassRows }} row(s) ignored — class not in your list
        </span>
        <span v-if="outOfRangeRows > 0" class="count-chip">
          {{ outOfRangeRows }} row(s) ignored — value outside 0–1
        </span>
        <span v-if="duplicateKeys > 0" class="count-chip">
          {{ duplicateKeys }} repeated class row(s) ignored
        </span>
      </div>

      <div v-if="suggestionNote || pollPaused" class="suggestion-status">
        <span v-if="suggestionNote" class="setup-note">{{ suggestionNote }}</span>
        <template v-if="pollPaused">
          <span class="setup-note">Suggestion refresh paused after repeated failures.</span>
          <el-button size="small" @click="resumePolling">Retry</el-button>
        </template>
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
import { computed, onBeforeUnmount, onMounted, ref, shallowRef, watch } from "vue";
import { ElMessage } from "element-plus";
import type { CatalogTable, NewColumnSpec, SqlQueryResult } from "../../types";
import { CatalogApi } from "../../api/catalog.api";
import {
  applyCellEdit,
  buildJoinKeySpec,
  buildLabelClasses,
  buildSuggestionMap,
  distinctColumnValues,
  distinctPredictionClasses,
  formatConfidence,
  isContinuousDtype,
  isEditableDtype,
  isNumericDtype,
  labelingOrder,
  labelingProgress,
  predictionProjectionColumns,
  rowSuggestion,
  suggestionBlockReason,
  suggestionCoverage,
  suggestionDisagreement,
  type EditRow,
  type EditSession,
  type JoinKeySpec,
  type SuggestionMap,
} from "./tableEditing";
import {
  MAX_LABEL_CLASSES,
  readCollapsedColumns,
  readLabelSettings,
  writeCollapsedColumns,
  writeLabelSettings,
  type StoredLabelSettings,
  type StoredSuggestionSettings,
} from "./labelingStorage";

const POLL_INTERVAL_MS = 10_000;
const REMOTE_POLL_INTERVAL_MS = 30_000;
const MAX_POLL_FAILURES = 3;
// The join needs every class row for a key, and read order is physical, not sorted — so the
// prediction table is read whole, projected to the join columns through the SQL endpoint. That
// path only reaches local Delta tables; anything else falls back to the row-capped preview and
// refuses to join when the cap truncates it.
const MAX_PREDICTION_ROWS = 500_000;
const PREDICTION_PREVIEW_LIMIT = 10_000;

const props = defineProps<{ session: EditSession; table: CatalogTable; rowLimit: number }>();

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
const classNote = ref("");
const targetColumn = ref("");
const order = ref<number[]>([]);
const pointer = ref(0);
// Keyed by the row's stable uid, not its position: the visit order is re-derived
// on restart, so an order index would resolve to a different row after a reshuffle.
const undoStack = ref<Array<{ uid: number; previous: unknown }>>([]);
const collapsedColumns = ref<Set<string>>(new Set());

const predictionTables = ref<CatalogTable[]>([]);
const predictionTableId = ref<number | null>(null);
const predictionTable = ref<CatalogTable | null>(null);
const serverPredictionTableId = ref<number | null>(null);
const serverPredictionTableName = ref<string | null>(null);
const predColumns = ref<string[]>([]);
const predDtypes = ref<string[]>([]);
const predRowCount = ref(0);
const predTotalRows = ref(0);
const predTruncated = ref(false);
const predRowsLoaded = ref(false);
/** Rows came from the row-capped preview because SQL could not reach the table. */
const predRowsViaPreview = ref(false);
const discoveredClasses = ref<string[]>([]);
const predVersion = ref<number | null>(null);
const predLoading = ref(false);
const classColumn = ref("");
const probabilityColumn = ref<string | null>(null);
const leastConfidentFirst = ref(false);
const suggestionError = ref("");
const suggestionNote = ref("");
const serverNote = ref("");
const suggestionsDisabled = ref(false);
const refreshNotice = ref(false);
const pollPaused = ref(false);
const hydrating = ref(true);
const startPending = ref(false);

const joinSpec = shallowRef<JoinKeySpec[] | null>(null);
const suggestionMap = shallowRef<SuggestionMap | null>(null);

// Prediction rows are only ever read to rebuild the map — kept out of reactivity.
let predRows: unknown[][] = [];
// The column order of predRows, which is the projection's, not the table's.
let rowColumns: string[] = [];
let pollTimer: number | null = null;
let pollInFlight = false;
let pollFailures = 0;
let loadSeq = 0;
/** Sequences the association PUTs; a slower earlier one must not win the race. */
let persistSeq = 0;
let predFetches = 0;
/** In-flight row fetch, so Start can await one already running instead of firing a second. */
let predRowsPending: Promise<boolean> | null = null;
/** Survives a transient preview failure so persistSettings can't erase the block. */
let rememberedSuggestion: StoredSuggestionSettings | null = null;

// Key columns are excluded: writing labels into a merge key would silently turn
// updates into inserts (the grid locks key cells for the same reason).
const existingTargets = computed(() =>
  props.session.columns
    .filter((c) => c.editable && !props.session.keyColumns.includes(c.name))
    .map((c) => c.name),
);

const sessionColumnNames = computed(() => new Set(props.session.columns.map((c) => c.name)));

const canStart = computed(() => {
  if (classes.value.length === 0) return false;
  if (targetMode.value === "existing") return !!existingTarget.value;
  const name = newColumnName.value.trim();
  if (!name || /[\s,;{}()=]/.test(name)) return false;
  return !props.session.columns.some((c) => c.name === name);
});

// Start stays blocked only while a suggestion source is still resolving; both
// inputs are guaranteed to settle, so this can never block labelling for good.
// Never gates Start — a hung preview must not be able to block labelling.
const suggestionsResolving = computed(() => hydrating.value || predLoading.value);

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
      return {
        name: c.name,
        value,
        long: value.length > 120,
        collapsed: collapsedColumns.value.has(c.name),
      };
    });
});

// A class label is a scalar and never continuous — that rules out both the
// probability columns and nested dtypes like a List(Float32) embedding.
const classColumnOptions = computed(() =>
  predColumns.value.filter((c, i) => {
    const dtype = predDtypes.value[i] ?? "";
    return (
      !props.session.keyColumns.includes(c) && isEditableDtype(dtype) && !isContinuousDtype(dtype)
    );
  }),
);

const probabilityColumnOptions = computed(() =>
  predColumns.value.filter(
    (c, i) => !props.session.keyColumns.includes(c) && isNumericDtype(predDtypes.value[i] ?? ""),
  ),
);

const missingJoinColumns = computed(() =>
  props.session.keyColumns.filter((c) => !predColumns.value.includes(c)),
);

const joinError = computed(() => {
  if (predColumns.value.length === 0) return "";
  if (props.session.keyColumns.length === 0) {
    return "This session has no key columns, so suggestions cannot be joined.";
  }
  if (predTruncated.value) {
    const cause = predRowsViaPreview.value
      ? "this table is not a local Delta table, so it can only be read through the row-capped preview"
      : `this view reads at most ${MAX_PREDICTION_ROWS.toLocaleString()} prediction rows`;
    return `Only ${predRowCount.value.toLocaleString()} of ${predTotalRows.value.toLocaleString()} prediction rows could be read: ${cause}. Rows arrive in storage order, so a partial read would hide classes and bias every suggestion — suggestions stay off. Reduce the prediction table to the rows you are labelling.`;
  }
  if (missingJoinColumns.value.length === 0) return "";
  return `The prediction table has no ${missingJoinColumns.value.join(", ")} column, so it cannot be joined to this session. Suggestions stay off.`;
});

const suggestionActive = computed(
  () => !suggestionsDisabled.value && joinSpec.value !== null && suggestionMap.value !== null,
);

const currentSuggestion = computed(() => {
  const row = currentRow.value;
  const spec = joinSpec.value;
  const map = suggestionMap.value;
  if (!row || !spec || !map || suggestionsDisabled.value) return null;
  return rowSuggestion(row, spec, map.byKey);
});

const suggestedClassKnown = computed(
  () => !!currentSuggestion.value && classes.value.includes(currentSuggestion.value.suggested),
);

const suggestedValue = computed(() =>
  suggestedClassKnown.value ? currentSuggestion.value?.suggested : undefined,
);

const confidenceText = computed(() =>
  formatConfidence(currentSuggestion.value?.confidence ?? null),
);

const coverage = computed(() => {
  const spec = joinSpec.value;
  const map = suggestionMap.value;
  if (!spec || !map) return { covered: 0, total: 0 };
  return suggestionCoverage(props.session.rows, spec, map.byKey);
});

const disagreement = computed(() => {
  const spec = joinSpec.value;
  const map = suggestionMap.value;
  if (!spec || !map) return 0;
  return suggestionDisagreement(props.session.rows, targetColumn.value, spec, map.byKey);
});

const duplicateKeys = computed(() =>
  suggestionActive.value ? (suggestionMap.value?.duplicateKeys ?? 0) : 0,
);

const outOfRangeRows = computed(() =>
  suggestionActive.value ? (suggestionMap.value?.outOfRangeRows ?? 0) : 0,
);

const unknownClassRows = computed(() =>
  suggestionActive.value ? (suggestionMap.value?.unknownClassRows ?? 0) : 0,
);

function addClass() {
  const value = newClass.value.trim();
  if (value && !classes.value.includes(value)) pushClass(value);
  newClass.value = "";
}

/** Refuses past the cap the storage layer enforces, rather than dropping it silently on persist. */
function pushClass(value: string): boolean {
  if (classes.value.length >= MAX_LABEL_CLASSES) {
    classNote.value = `At most ${MAX_LABEL_CLASSES} classes.`;
    return false;
  }
  classes.value.push(value);
  classNote.value = "";
  return true;
}

const classesMatchDiscovered = computed(
  () =>
    discoveredClasses.value.length === classes.value.length &&
    discoveredClasses.value.every((c) => classes.value.includes(c)),
);

/** Replace the class list with what the model actually emits, so the two can never drift. */
function adoptDiscoveredClasses() {
  classes.value = discoveredClasses.value.slice(0, MAX_LABEL_CLASSES);
  classNote.value = "";
  persistSettings();
}

function removeClass(index: number) {
  classes.value.splice(index, 1);
}

function prefillClasses(column: string) {
  if (classes.value.length === 0) {
    classes.value = distinctColumnValues(props.session.rows, column, 9);
  }
}

function probabilityFor(className: string): string {
  const probabilities = currentSuggestion.value?.probabilities;
  if (!probabilities || !(className in probabilities)) return "";
  return formatConfidence(probabilities[className]);
}

function persistCollapsed() {
  const kept = [...collapsedColumns.value].filter((name) => sessionColumnNames.value.has(name));
  writeCollapsedColumns(props.table.id, kept);
}

function toggleColumn(name: string) {
  const next = new Set(collapsedColumns.value);
  if (next.has(name)) next.delete(name);
  else next.add(name);
  collapsedColumns.value = next;
  persistCollapsed();
}

function collapseAll() {
  collapsedColumns.value = new Set(displayFields.value.map((field) => field.name));
  persistCollapsed();
}

function expandAll() {
  collapsedColumns.value = new Set();
  persistCollapsed();
}

/**
 * The block to persist for the currently selected prediction table. An empty
 * selection caused by an unreadable preview falls back to the remembered block:
 * the columns are only blank because we never got a schema to validate them
 * against, and overwriting would lose the user's choices for good.
 */
function currentSuggestionSettings(): StoredSuggestionSettings | null {
  const id = predictionTableId.value;
  if (id === null) return null;
  if (!classColumn.value) {
    const previewFailed = !!suggestionError.value || predColumns.value.length === 0;
    if (!previewFailed || rememberedSuggestion?.predictionTableId !== id) return null;
    return rememberedSuggestion;
  }
  return {
    predictionTableId: id,
    predictionTableName: predictionTable.value?.name ?? "",
    classColumn: classColumn.value,
    probabilityColumn: probabilityColumn.value,
    leastConfidentFirst: leastConfidentFirst.value,
  };
}

function persistSettings() {
  const suggestion = currentSuggestionSettings();
  if (suggestion) rememberedSuggestion = suggestion;
  writeLabelSettings(props.table.id, {
    targetMode: targetMode.value,
    newColumnName: newColumnName.value.trim(),
    existingTarget: existingTarget.value,
    classes: [...classes.value],
    suggestion,
  });
}

/**
 * Restore the remembered setup. A remembered *new* column that meanwhile exists
 * in the schema is promoted to an existing target — that is the
 * label → save → reopen loop, and re-creating the column would fail.
 */
function applyStoredSettings(stored: StoredLabelSettings | null) {
  if (!stored) return;
  if (stored.classes.length > 0) classes.value = [...stored.classes];
  if (stored.newColumnName) newColumnName.value = stored.newColumnName;
  const available = new Set(existingTargets.value);
  if (stored.targetMode === "new" && available.has(stored.newColumnName)) {
    targetMode.value = "existing";
    existingTarget.value = stored.newColumnName;
    return;
  }
  if (stored.targetMode === "existing" && available.has(stored.existingTarget)) {
    targetMode.value = "existing";
    existingTarget.value = stored.existingTarget;
  }
}

function pruneSuggestionSelections() {
  const available = new Set(predColumns.value);
  if (classColumn.value && !available.has(classColumn.value)) classColumn.value = "";
  if (probabilityColumn.value && !available.has(probabilityColumn.value))
    probabilityColumn.value = null;
  if (!probabilityColumn.value) leastConfidentFirst.value = false;
}

/** Called explicitly after a row fetch too: predRows and its column list are non-reactive. */
function recomputeDiscoveredClasses() {
  discoveredClasses.value = classColumn.value
    ? distinctPredictionClasses(rowColumns, predRows, classColumn.value)
    : [];
}

function clearPredictionRows() {
  predRows = [];
  rowColumns = [];
  predRowCount.value = 0;
  predTruncated.value = false;
  predRowsLoaded.value = false;
  predRowsViaPreview.value = false;
}

function clearPredictionData() {
  predColumns.value = [];
  predDtypes.value = [];
  predTotalRows.value = 0;
  predVersion.value = null;
  clearPredictionRows();
}

function beginPredFetch() {
  predFetches += 1;
  predLoading.value = true;
}

/** Paired with beginPredFetch in a finally — a failed or orphaned fetch must never
 * latch the "checking for predictions…" hint on. */
function endPredFetch() {
  predFetches = Math.max(0, predFetches - 1);
  if (predFetches === 0) predLoading.value = false;
}

/**
 * Phase 1: read the schema only. A one-row preview is enough to drive the column
 * pickers and carries the real row count, and it keeps embedding columns out of
 * setup — the preview serializer reprs any non-scalar cell, so one List(Float32)
 * vector is kilobytes of JSON a row.
 */
async function loadPredictionTable(id: number) {
  // Stale responses must not pair one table's columns with another's id — the
  // poll would then compare Delta versions across two unrelated tables.
  const seq = ++loadSeq;
  beginPredFetch();
  suggestionError.value = "";
  try {
    const [history, preview] = await Promise.allSettled([
      CatalogApi.getTableHistory(id, 1),
      CatalogApi.getTablePreview(id, 1),
    ]);
    if (seq !== loadSeq) return;
    if (preview.status === "rejected") {
      clearPredictionData();
      suggestionError.value = "Could not read the prediction table.";
      return;
    }
    predColumns.value = preview.value.columns;
    predDtypes.value = preview.value.dtypes;
    predTotalRows.value = preview.value.total_rows;
    clearPredictionRows();
    // A missing version anchor only costs us change detection; the first poll re-anchors.
    predVersion.value = history.status === "fulfilled" ? history.value.current_version : null;
  } finally {
    endPredFetch();
  }
}

function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/** The join only needs the key columns plus the picked ones, so project to those. */
function predictionRowsQuery(): string | null {
  const table = predictionTable.value;
  const target = table?.qualified_name ?? table?.full_table_name ?? table?.name;
  if (!target) return null;
  const columns = predictionProjectionColumns(
    props.session.keyColumns,
    classColumn.value,
    probabilityColumn.value,
  );
  if (!columns) return null;
  return `SELECT ${columns.map(quoteIdent).join(", ")} FROM ${quoteIdent(target)}`;
}

/** Generous headroom over the row count we saw, so rows written since still fit. */
function predictionRowsLimit(): number {
  return Math.min(Math.max(predTotalRows.value * 2, 50_000), MAX_PREDICTION_ROWS);
}

/** `null` on any failure — the endpoint reports its refusals as `error` on a 200. */
async function runPredictionQuery(query: string, maxRows: number): Promise<SqlQueryResult | null> {
  try {
    const result = await CatalogApi.executeSqlQuery(query, maxRows);
    return result.error ? null : result;
  } catch {
    return null;
  }
}

function applyPredictionRows(
  columns: string[],
  rows: unknown[][],
  totalRows: number,
  truncated: boolean,
  viaPreview: boolean,
) {
  rowColumns = columns;
  predRows = rows;
  predRowCount.value = rows.length;
  predTotalRows.value = totalRows;
  predTruncated.value = truncated;
  predRowsViaPreview.value = viaPreview;
  predRowsLoaded.value = true;
  suggestionError.value = "";
  recomputeDiscoveredClasses();
}

/**
 * Phase 2: read the join columns for every row. SQL takes no row cap worth worrying
 * about; a table it cannot reach (object storage, legacy Parquet) falls back to the
 * row-capped preview, which joinError then refuses to join.
 */
async function fetchPredictionRows(): Promise<boolean> {
  const id = predictionTableId.value;
  const query = predictionRowsQuery();
  if (id === null || query === null) return false;
  const seq = ++loadSeq;
  beginPredFetch();
  try {
    const result = await runPredictionQuery(query, predictionRowsLimit());
    if (seq !== loadSeq) return false;
    if (result) {
      applyPredictionRows(result.columns, result.rows, result.total_rows, result.truncated, false);
      return true;
    }
    const preview = await CatalogApi.getTablePreview(id, PREDICTION_PREVIEW_LIMIT).catch(
      () => null,
    );
    if (seq !== loadSeq) return false;
    if (!preview) {
      // Whatever rows we already had are the last known good ones — keep them.
      suggestionError.value = "Could not read the prediction table.";
      return false;
    }
    // The full schema arrives with the fallback, so refresh the pickers off it too.
    predColumns.value = preview.columns;
    predDtypes.value = preview.dtypes;
    applyPredictionRows(
      preview.columns,
      preview.rows,
      preview.total_rows,
      preview.total_rows > preview.rows.length,
      true,
    );
    return true;
  } finally {
    endPredFetch();
  }
}

/** Resolves `true` only when rows were actually applied. */
async function loadPredictionRows(): Promise<boolean> {
  const run = fetchPredictionRows();
  predRowsPending = run;
  try {
    return await run;
  } finally {
    if (predRowsPending === run) predRowsPending = null;
  }
}

function needsPredictionRows(): boolean {
  return predictionTableId.value !== null && !!classColumn.value && !predRowsLoaded.value;
}

async function selectPredictionTable(id: number, stored?: StoredSuggestionSettings | null) {
  predictionTableId.value = id;
  predictionTable.value = predictionTables.value.find((t) => t.id === id) ?? null;
  suggestionsDisabled.value = false;
  suggestionNote.value = "";
  await loadPredictionTable(id);
  if (predictionTableId.value !== id) return;
  if (stored && stored.predictionTableId === id) {
    classColumn.value = stored.classColumn;
    probabilityColumn.value = stored.probabilityColumn;
    leastConfidentFirst.value = stored.leastConfidentFirst;
  }
  pruneSuggestionSelections();
  // The column watcher can't cover this: restored (or carried-over) picks may be
  // identical to the previous table's, so nothing changes for it to react to.
  if (needsPredictionRows()) void loadPredictionRows();
}

function clearPrediction() {
  loadSeq += 1; // orphan any in-flight load so it cannot repopulate the cleared slice
  predictionTableId.value = null;
  predictionTable.value = null;
  classColumn.value = "";
  probabilityColumn.value = null;
  suggestionError.value = "";
  suggestionNote.value = "";
  suggestionsDisabled.value = false;
  clearPredictionData();
  joinSpec.value = null;
  suggestionMap.value = null;
}

/** `null` is the explicit clear: the backend tri-state drops the association. */
async function persistPredictionTable(id: number | null) {
  const seq = ++persistSeq;
  try {
    const updated = await CatalogApi.updateTable(props.table.id, { prediction_table_id: id });
    if (seq !== persistSeq) return;
    serverPredictionTableId.value = updated.prediction_table_id;
    serverPredictionTableName.value = updated.prediction_table_name;
    serverNote.value = "";
  } catch {
    if (seq !== persistSeq) return;
    serverNote.value =
      id === null
        ? "Could not forget this prediction table on the server — it stays off for this session."
        : "Could not remember this prediction table on the server — it still applies to this session.";
  }
}

async function onPredictionTablePick(value: unknown) {
  const id = typeof value === "number" ? value : null;
  // Persist the pick itself, before loading: a clear issued mid-load would
  // otherwise leave the abandoned table associated on the server.
  if (id !== serverPredictionTableId.value) void persistPredictionTable(id);
  if (id === null) {
    clearPrediction();
    return;
  }
  await selectPredictionTable(id);
}

function retryPredictionLoad() {
  const id = predictionTableId.value;
  if (id === null) return;
  void loadPredictionTable(id).then(() => {
    pruneSuggestionSelections();
    if (needsPredictionRows()) void loadPredictionRows();
  });
}

/** Best-effort name match for a sibling predictions table; failures are silent. */
async function guessPredictionTable(): Promise<CatalogTable | null> {
  for (const suffix of ["_predictions", "_scored"]) {
    try {
      const found = await CatalogApi.resolveTableByName(
        `${props.table.name}${suffix}`,
        props.table.namespace_id,
      );
      if (found && found.id !== props.table.id) return found;
    } catch {
      return null;
    }
  }
  return null;
}

/** An unreadable table list can't disprove anything, so it defers to a load attempt. */
function stillInCatalog(id: number): boolean {
  return predictionTables.value.length === 0 || predictionTables.value.some((t) => t.id === id);
}

/** A null name with a live id means the table exists but the user cannot read it. */
function noteMissingPredictionTable(name: string | null) {
  suggestionNote.value = name
    ? `Previous prediction table “${name}” is no longer available`
    : "The prediction table set for this table is not available to you";
}

async function initSuggestionSources(stored: StoredLabelSettings | null) {
  try {
    try {
      const tables = await CatalogApi.getTables();
      predictionTables.value = tables.filter((t) => t.id !== props.table.id);
      // The association is server-owned and shared, so trust the list over a prop
      // the parent may have fetched before another session (or a delete) changed it.
      const self = tables.find((t) => t.id === props.table.id);
      if (self) {
        serverPredictionTableId.value = self.prediction_table_id;
        serverPredictionTableName.value = self.prediction_table_name;
      }
    } catch {
      predictionTables.value = [];
    }

    // A table the user picked while we hydrated wins over anything remembered.
    if (predictionTableId.value !== null) return;

    const serverId = serverPredictionTableId.value;
    if (serverId !== null) {
      if (stillInCatalog(serverId)) {
        await selectPredictionTable(serverId, stored?.suggestion ?? null);
      } else {
        noteMissingPredictionTable(serverPredictionTableName.value);
      }
      return;
    }
    if (stored?.suggestion) {
      if (stillInCatalog(stored.suggestion.predictionTableId)) {
        await selectPredictionTable(stored.suggestion.predictionTableId, stored.suggestion);
      } else {
        noteMissingPredictionTable(stored.suggestion.predictionTableName);
      }
      return;
    }
    const guess = await guessPredictionTable();
    if (guess && predictionTableId.value === null) {
      if (!predictionTables.value.some((t) => t.id === guess.id))
        predictionTables.value.push(guess);
      await selectPredictionTable(guess.id);
    }
  } finally {
    hydrating.value = false;
  }
}

function rebuildSuggestionMap() {
  const spec = joinSpec.value;
  if (!spec || suggestionsDisabled.value || !classColumn.value) return;
  suggestionMap.value = buildSuggestionMap(
    rowColumns,
    predRows,
    spec,
    classColumn.value,
    probabilityColumn.value ?? "",
    classes.value,
  );
}

function buildSuggestionState() {
  joinSpec.value = null;
  suggestionMap.value = null;
  suggestionsDisabled.value = false;
  // A plain class table (no probability column) is a supported shape.
  if (predictionTableId.value === null || !classColumn.value) return;
  if (!predRowsLoaded.value || predTruncated.value) return;
  // The fetched projection, not the table schema, is what the map indexes into.
  if (!rowColumns.includes(classColumn.value)) return;
  const spec = buildJoinKeySpec(props.session.keyColumns, props.session.columns, rowColumns);
  if (!spec) return;
  joinSpec.value = spec;
  rebuildSuggestionMap();
}

function orderConfidence(): ((row: EditRow) => number | null) | undefined {
  if (!leastConfidentFirst.value || !probabilityColumn.value) return undefined;
  const spec = joinSpec.value;
  const map = suggestionMap.value;
  if (!spec || !map) return undefined;
  return (row) => rowSuggestion(row, spec, map.byKey)?.confidence ?? null;
}

async function startLabeling() {
  if (startPending.value) return;
  startPending.value = true;
  try {
    if (targetMode.value === "new") {
      const name = newColumnName.value.trim();
      emit("add-column", { name, dtype: "String" });
      targetColumn.value = name;
    } else {
      targetColumn.value = existingTarget.value;
    }
    // The visit order depends on the suggestion map, so the rows have to be in
    // before it is derived. Every fetch path settles, so this can't hang Start.
    if (needsPredictionRows()) await (predRowsPending ?? loadPredictionRows());
    buildSuggestionState();
    persistSettings();
    order.value = labelingOrder(props.session.rows, targetColumn.value, orderConfidence());
    pointer.value = 0;
    undoStack.value = [];
    phase.value = "labeling";
  } finally {
    startPending.value = false;
  }
}

function assign(classValue: string) {
  const row = currentRow.value;
  if (!row) return;
  undoStack.value.push({ uid: row.uid, previous: row.cells[targetColumn.value] ?? null });
  applyCellEdit(row, targetColumn.value, classValue);
  advance();
}

function acceptSuggestion() {
  if (!suggestedClassKnown.value) return;
  const suggested = currentSuggestion.value?.suggested;
  if (suggested) assign(suggested);
}

function addSuggestedClass() {
  const suggested = currentSuggestion.value?.suggested;
  if (!suggested || classes.value.includes(suggested)) return;
  if (!pushClass(suggested)) return;
  rebuildSuggestionMap();
  persistSettings();
}

function advance() {
  refreshNotice.value = false;
  if (pointer.value < order.value.length) pointer.value += 1;
}

function goto(index: number) {
  pointer.value = Math.max(0, Math.min(index, order.value.length));
}

function undo() {
  const last = undoStack.value[undoStack.value.length - 1];
  if (!last) return;
  const rowIndex = props.session.rows.findIndex((r) => r.uid === last.uid);
  // Keep the entry if its row left the buffer, so undo never silently burns a step.
  if (rowIndex === -1) return;
  undoStack.value.pop();
  applyCellEdit(props.session.rows[rowIndex], targetColumn.value, last.previous);
  const orderIndex = order.value.indexOf(rowIndex);
  if (orderIndex !== -1) pointer.value = orderIndex;
}

function restart() {
  order.value = labelingOrder(props.session.rows, targetColumn.value, orderConfidence());
  pointer.value = 0;
  undoStack.value = [];
}

function stopPolling() {
  if (pollTimer !== null) {
    window.clearInterval(pollTimer);
    pollTimer = null;
  }
}

function startPolling() {
  stopPolling();
  if (pollPaused.value) return;
  const interval = predictionTable.value?.is_remote_storage
    ? REMOTE_POLL_INTERVAL_MS
    : POLL_INTERVAL_MS;
  pollTimer = window.setInterval(() => void pollTick(), interval);
}

function resumePolling() {
  pollFailures = 0;
  pollPaused.value = false;
  startPolling();
}

/** Re-read the prediction slice after a new Delta version; the visit order is
 * deliberately left alone so the row under the cursor never moves. */
async function refreshSuggestions(id: number, version: number) {
  const loaded = await loadPredictionRows();
  // A table switch mid-refresh already orphaned the fetch; don't re-anchor its version.
  if (predictionTableId.value !== id) return;
  // A failed re-read keeps the current map and the old version anchor, and counts as a
  // poll failure so the pause-after-three ladder still applies.
  if (!loaded) throw new Error("Could not re-read the prediction table");
  predVersion.value = version;

  const hadConfidence = probabilityColumn.value;
  pruneSuggestionSelections();
  const lostConfidence = !!hadConfidence && !probabilityColumn.value;

  const columns = rowColumns;
  const spec = buildJoinKeySpec(props.session.keyColumns, props.session.columns, columns);
  const blocked = suggestionBlockReason({
    truncated: predTruncated.value,
    joinSpec: spec,
    columns,
    classColumn: classColumn.value,
  });
  if (blocked) {
    suggestionsDisabled.value = true;
    suggestionNote.value = blocked;
    stopPolling();
    return;
  }
  joinSpec.value = spec;
  rebuildSuggestionMap();
  if (lostConfidence) {
    suggestionNote.value = `The “${hadConfidence}” column is gone from the prediction table, so confidence is no longer shown.`;
  }
  refreshNotice.value = true;
  ElMessage({ message: "Suggestions updated", type: "info", grouping: true });
}

async function pollTick() {
  const id = predictionTableId.value;
  if (id === null || phase.value !== "labeling" || !suggestionActive.value) return;
  if (pollInFlight || document.hidden) return;
  pollInFlight = true;
  try {
    const history = await CatalogApi.getTableHistory(id, 1);
    pollFailures = 0;
    if (predVersion.value === null) predVersion.value = history.current_version;
    else if (history.current_version > predVersion.value) {
      await refreshSuggestions(id, history.current_version);
    }
  } catch {
    pollFailures += 1;
    if (pollFailures >= MAX_POLL_FAILURES) {
      pollPaused.value = true;
      stopPolling();
    }
  } finally {
    pollInFlight = false;
  }
}

function isTypingTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName.toLowerCase();
  return tag === "input" || tag === "textarea" || target.isContentEditable;
}

/** Enter and Space activate a focused control; the shortcuts must yield to it. */
function isActivatableTarget(target: EventTarget | null): boolean {
  return target instanceof HTMLElement && target.closest('button, [role="button"]') !== null;
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
  if (event.key === "Enter") {
    if (isActivatableTarget(event.target)) return;
    event.preventDefault();
    acceptSuggestion();
    return;
  }
  if (event.key === " " || event.code === "Space") {
    if (isActivatableTarget(event.target)) return;
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

watch([classColumn, predColumns], recomputeDiscoveredClasses);

// Rows are only worth reading once both picks are in — the join needs the pair, and
// the projection is built from them.
watch([classColumn, probabilityColumn], () => {
  if (hydrating.value) return;
  if (!classColumn.value || !probabilityColumn.value) return;
  void loadPredictionRows();
});

watch(probabilityColumn, (value) => {
  if (!value) leastConfidentFirst.value = false;
});

watch(leastConfidentFirst, () => {
  if (!hydrating.value) persistSettings();
});

// A prediction source that arrives after Start still gets picked up; the visit
// order is deliberately left alone so no row moves mid-pass.
watch([predictionTableId, classColumn, probabilityColumn, predRowsLoaded], () => {
  if (phase.value !== "labeling" || suggestionsDisabled.value || suggestionActive.value) return;
  buildSuggestionState();
});

watch(
  () => phase.value === "labeling" && suggestionActive.value,
  (active) => {
    if (active) startPolling();
    else stopPolling();
  },
);

onMounted(() => {
  window.addEventListener("keydown", onKeydown);
  collapsedColumns.value = new Set(
    readCollapsedColumns(props.table.id).filter((name) => sessionColumnNames.value.has(name)),
  );
  serverPredictionTableId.value = props.table.prediction_table_id;
  serverPredictionTableName.value = props.table.prediction_table_name;
  const stored = readLabelSettings(props.table.id);
  rememberedSuggestion = stored?.suggestion ?? null;
  applyStoredSettings(stored);
  void initSuggestionSources(stored);
});

onBeforeUnmount(() => {
  window.removeEventListener("keydown", onKeydown);
  stopPolling();
});
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
  background: var(--color-background-soft);
  color: var(--color-text-secondary);
}

.refresh-badge {
  font-size: var(--font-size-xs, 11px);
  font-weight: 400;
  padding: 2px 8px;
  border-radius: 10px;
  border: 1px solid var(--color-accent-dark);
  color: var(--color-accent-dark);
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

.setup-note {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs, 11px);
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

.suggestion-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
}

.suggestion-label {
  flex: 0 0 180px;
  font-size: var(--font-size-sm, 13px);
  color: var(--color-text-secondary);
}

.suggestion-select {
  flex: 1 1 auto;
  min-width: 0;
}

.suggestion-warning {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  margin: 0;
  font-size: var(--font-size-sm, 13px);
  color: var(--color-warning-dark);
}

.start-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
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
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  padding: var(--spacing-4, 16px);
  background: var(--color-background-primary);
}

.row-position {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
  margin-bottom: var(--spacing-3, 12px);
  display: flex;
  gap: var(--spacing-3, 12px);
}

.current-label {
  color: var(--color-success-dark);
}

.fields-toolbar {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  margin-bottom: var(--spacing-2, 8px);
}

.text-btn {
  border: none;
  background: none;
  padding: 0;
  cursor: pointer;
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.text-btn:hover {
  color: var(--color-accent-dark);
}

.text-btn-sep {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
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

.field-toggle {
  flex: 0 0 14px;
  border: none;
  background: none;
  padding: 0;
  cursor: pointer;
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.field-toggle:hover {
  color: var(--color-accent-dark);
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

.collapsed-field {
  display: flex;
  align-items: center;
  gap: var(--spacing-2, 8px);
  width: 100%;
  border: none;
  background: none;
  padding: 0 0 0 2px;
  cursor: pointer;
  text-align: left;
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.collapsed-field:hover {
  color: var(--color-accent-dark);
}

.suggestion-line {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2, 8px);
  margin-top: var(--spacing-3, 12px);
  padding-top: var(--spacing-2, 8px);
  border-top: 1px solid var(--color-border-primary);
  font-size: var(--font-size-sm, 13px);
  color: var(--color-accent-dark);
}

.enter-hint {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.unknown-chip {
  font-size: var(--font-size-xs, 11px);
  padding: 2px 8px;
  border-radius: 10px;
  background: var(--color-background-soft);
  color: var(--color-text-secondary);
}

.mini-btn {
  border: 1px solid var(--color-accent-dark);
  border-radius: 4px;
  background: none;
  padding: 1px 6px;
  cursor: pointer;
  font-size: var(--font-size-xs, 11px);
  color: var(--color-accent-dark);
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
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  cursor: pointer;
}

.class-btn:hover {
  border-color: var(--color-accent);
}

.class-btn.active {
  border-color: var(--color-success);
  background: var(--color-success-light);
}

/* Pre-selected weight: the model's pick should be obvious without reading the line above. */
.class-btn.suggested {
  border-color: var(--color-accent);
  border-width: 2px;
  padding: 9px 15px;
  background: var(--color-accent-subtle);
  font-weight: 600;
  box-shadow: 0 0 0 3px var(--color-focus-ring-accent);
}

.class-btn.suggested .suggested-chip kbd {
  font-family: inherit;
  font-size: var(--font-size-xs, 11px);
  padding: 0 3px;
  border-radius: 3px;
  border: 1px solid var(--color-accent);
}

.discovered-classes {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  gap: var(--spacing-2, 8px);
}

.class-btn.suggested.active {
  border-color: var(--color-success);
  background: var(--color-success-light);
  box-shadow: none;
}

.class-prob {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.suggested-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: var(--font-size-xs, 11px);
  padding: 1px 6px;
  border-radius: 8px;
  background: var(--color-accent-subtle);
  color: var(--color-accent-dark);
}

.shortcut-key {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border-radius: 4px;
  border: 1px solid var(--color-border-primary);
  background: var(--color-background-soft);
  color: var(--color-text-primary);
  font-size: var(--font-size-xs, 11px);
  font-weight: 700;
}

/* Tint rather than fill: a solid accent/success keycap only reads at one end of the
   theme range, and the digit has to stay legible on both. */
.class-btn.suggested .shortcut-key {
  border-color: var(--color-accent);
  background: var(--color-accent-subtle);
  color: var(--color-accent-dark);
}

.class-btn.active .shortcut-key {
  border-color: var(--color-success);
  background: var(--color-success-light);
  color: var(--color-success-dark);
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

.suggestion-status {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2, 8px);
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
  color: var(--color-success-dark);
}

.done-actions {
  display: flex;
  gap: var(--spacing-2, 8px);
}
</style>
