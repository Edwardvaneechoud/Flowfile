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
          <span class="suggestion-label">{{
            predRepeatsKeys ? "Class column" : "Suggested class column"
          }}</span>
          <el-select
            v-model="suggestionColumn"
            filterable
            clearable
            placeholder="Pick a column"
            class="suggestion-select"
          >
            <el-option v-for="col in suggestionColumns" :key="col" :label="col" :value="col" />
          </el-select>
        </div>
        <div class="suggestion-row">
          <span class="suggestion-label">{{
            predRepeatsKeys ? "Probability column" : "Confidence column"
          }}</span>
          <el-select
            v-model="confidenceColumn"
            filterable
            clearable
            placeholder="None"
            class="suggestion-select"
          >
            <el-option v-for="col in confidenceColumns" :key="col" :label="col" :value="col" />
          </el-select>
        </div>
        <p v-if="predRepeatsKeys" class="setup-hint">
          This table has one row per class. Pick the probability column so the highest-scoring class
          wins — without it the first row per key is used.
        </p>
        <el-checkbox v-model="useProbabilityColumns" :disabled="!canUseProbabilities">
          Show per-class probabilities
        </el-checkbox>
        <el-checkbox v-model="leastConfidentFirst" :disabled="!confidenceColumn">
          Least confident first
        </el-checkbox>
      </template>

      <div class="start-row">
        <el-button type="primary" :disabled="!canStart" @click="startLabeling">
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
          <span v-if="suggestedValue === cls.value" class="suggested-chip">suggested</span>
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
        <span v-if="duplicateKeys > 0" class="count-chip">
          {{ duplicateKeys }} repeated class row(s) ignored
        </span>
      </div>

      <div v-if="predSliceNote || suggestionNote || pollPaused" class="suggestion-status">
        <span v-if="predSliceNote" class="setup-note">{{ predSliceNote }}</span>
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
import type { CatalogTable, NewColumnSpec } from "../../types";
import { CatalogApi } from "../../api/catalog.api";
import {
  applyCellEdit,
  buildJoinKeySpec,
  buildLabelClasses,
  buildSuggestionMap,
  detectProbabilityColumns,
  distinctColumnValues,
  formatConfidence,
  hasRepeatedKeys,
  isContinuousDtype,
  isNumericDtype,
  labelingOrder,
  labelingProgress,
  rowSuggestion,
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
const predVersion = ref<number | null>(null);
const predRepeatsKeys = ref(false);
const predLoading = ref(false);
const suggestionColumn = ref("");
const confidenceColumn = ref<string | null>(null);
const useProbabilityColumns = ref(false);
const leastConfidentFirst = ref(false);
const suggestionError = ref("");
const suggestionNote = ref("");
const serverNote = ref("");
const suggestionsDisabled = ref(false);
const refreshNotice = ref(false);
const pollPaused = ref(false);
const hydrating = ref(true);

const joinSpec = shallowRef<JoinKeySpec[] | null>(null);
const suggestionMap = shallowRef<SuggestionMap | null>(null);

// Prediction rows are only ever read to rebuild the map — kept out of reactivity.
let predRows: unknown[][] = [];
let pollTimer: number | null = null;
let pollInFlight = false;
let pollFailures = 0;
let loadSeq = 0;
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

const suggestionColumns = computed(() =>
  predColumns.value.filter(
    (c, i) =>
      !props.session.keyColumns.includes(c) && !isContinuousDtype(predDtypes.value[i] ?? ""),
  ),
);

const confidenceColumns = computed(() =>
  predColumns.value.filter((c, i) => isNumericDtype(predDtypes.value[i] ?? "")),
);

const probabilityMatches = computed(() =>
  detectProbabilityColumns(predColumns.value, classes.value),
);

const canUseProbabilities = computed(
  () =>
    Object.keys(probabilityMatches.value).length > 0 ||
    (predRepeatsKeys.value && !!confidenceColumn.value),
);

const missingJoinColumns = computed(() =>
  props.session.keyColumns.filter((c) => !predColumns.value.includes(c)),
);

const joinError = computed(() => {
  if (predColumns.value.length === 0) return "";
  if (props.session.keyColumns.length === 0) {
    return "This session has no key columns, so suggestions cannot be joined.";
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

const predSliceNote = computed(() => {
  if (!suggestionActive.value || predTotalRows.value <= predRowCount.value) return "";
  return `Suggestions cover the first ${predRowCount.value.toLocaleString()} of ${predTotalRows.value.toLocaleString()} prediction rows.`;
});

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
  if (!suggestionColumn.value) {
    const previewFailed = !!suggestionError.value || predColumns.value.length === 0;
    if (!previewFailed || rememberedSuggestion?.predictionTableId !== id) return null;
    return rememberedSuggestion;
  }
  return {
    predictionTableId: id,
    predictionTableName: predictionTable.value?.name ?? "",
    suggestionColumn: suggestionColumn.value,
    confidenceColumn: confidenceColumn.value,
    useProbabilityColumns: useProbabilityColumns.value,
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
  if (suggestionColumn.value && !available.has(suggestionColumn.value)) suggestionColumn.value = "";
  if (confidenceColumn.value && !available.has(confidenceColumn.value))
    confidenceColumn.value = null;
  if (!confidenceColumn.value) leastConfidentFirst.value = false;
  if (!canUseProbabilities.value) useProbabilityColumns.value = false;
}

function clearPredictionData() {
  predRepeatsKeys.value = false;
  predColumns.value = [];
  predDtypes.value = [];
  predRows = [];
  predRowCount.value = 0;
  predTotalRows.value = 0;
  predVersion.value = null;
}

async function loadPredictionTable(id: number) {
  // Stale responses must not pair one table's columns with another's id — the
  // poll would then compare Delta versions across two unrelated tables.
  const seq = ++loadSeq;
  predLoading.value = true;
  suggestionError.value = "";
  const [history, preview] = await Promise.allSettled([
    CatalogApi.getTableHistory(id, 1),
    CatalogApi.getTablePreview(id, props.rowLimit),
  ]);
  predLoading.value = false;
  if (seq !== loadSeq) return;
  if (preview.status === "rejected") {
    clearPredictionData();
    suggestionError.value = "Could not read the prediction table.";
    return;
  }
  predColumns.value = preview.value.columns;
  predDtypes.value = preview.value.dtypes;
  predRows = preview.value.rows;
  predRowCount.value = preview.value.rows.length;
  predTotalRows.value = preview.value.total_rows;
  // A missing version anchor only costs us change detection; the first poll re-anchors.
  predVersion.value = history.status === "fulfilled" ? history.value.current_version : null;
  predRepeatsKeys.value = detectRepeatedKeys(preview.value.columns);
}

/** One row per class (long) vs one row per record (wide) — drives the probability affordance. */
function detectRepeatedKeys(columns: string[]): boolean {
  const spec = buildJoinKeySpec(props.session.keyColumns, props.session.columns, columns);
  return spec !== null && hasRepeatedKeys(columns, predRows, spec);
}

async function selectPredictionTable(id: number, stored?: StoredSuggestionSettings | null) {
  predictionTableId.value = id;
  predictionTable.value = predictionTables.value.find((t) => t.id === id) ?? null;
  suggestionsDisabled.value = false;
  suggestionNote.value = "";
  await loadPredictionTable(id);
  if (predictionTableId.value !== id) return;
  if (stored && stored.predictionTableId === id) {
    suggestionColumn.value = stored.suggestionColumn;
    confidenceColumn.value = stored.confidenceColumn;
    useProbabilityColumns.value = stored.useProbabilityColumns;
    leastConfidentFirst.value = stored.leastConfidentFirst;
  }
  pruneSuggestionSelections();
}

function clearPrediction() {
  loadSeq += 1; // orphan any in-flight load so it cannot repopulate the cleared slice
  predictionTableId.value = null;
  predictionTable.value = null;
  suggestionColumn.value = "";
  confidenceColumn.value = null;
  suggestionError.value = "";
  suggestionNote.value = "";
  suggestionsDisabled.value = false;
  clearPredictionData();
  joinSpec.value = null;
  suggestionMap.value = null;
}

/** `null` is the explicit clear: the backend tri-state drops the association. */
async function persistPredictionTable(id: number | null) {
  try {
    const updated = await CatalogApi.updateTable(props.table.id, { prediction_table_id: id });
    serverPredictionTableId.value = updated.prediction_table_id;
    serverPredictionTableName.value = updated.prediction_table_name;
    serverNote.value = "";
  } catch {
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
  void loadPredictionTable(id).then(pruneSuggestionSelections);
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
  if (!spec || suggestionsDisabled.value || !suggestionColumn.value) return;
  suggestionMap.value = buildSuggestionMap(
    predColumns.value,
    predRows,
    spec,
    suggestionColumn.value,
    confidenceColumn.value,
    useProbabilityColumns.value ? probabilityMatches.value : null,
  );
}

function buildSuggestionState() {
  joinSpec.value = null;
  suggestionMap.value = null;
  suggestionsDisabled.value = false;
  if (predictionTableId.value === null || !suggestionColumn.value) return;
  if (predColumns.value.length === 0) return;
  const spec = buildJoinKeySpec(props.session.keyColumns, props.session.columns, predColumns.value);
  if (!spec) return;
  joinSpec.value = spec;
  rebuildSuggestionMap();
}

function orderConfidence(): ((row: EditRow) => number | null) | undefined {
  if (!leastConfidentFirst.value || !confidenceColumn.value) return undefined;
  const spec = joinSpec.value;
  const map = suggestionMap.value;
  if (!spec || !map) return undefined;
  return (row) => rowSuggestion(row, spec, map.byKey)?.confidence ?? null;
}

function startLabeling() {
  if (targetMode.value === "new") {
    const name = newColumnName.value.trim();
    emit("add-column", { name, dtype: "String" });
    targetColumn.value = name;
  } else {
    targetColumn.value = existingTarget.value;
  }
  buildSuggestionState();
  persistSettings();
  order.value = labelingOrder(props.session.rows, targetColumn.value, orderConfidence());
  pointer.value = 0;
  undoStack.value = [];
  phase.value = "labeling";
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
  const preview = await CatalogApi.getTablePreview(id, props.rowLimit);
  predColumns.value = preview.columns;
  predDtypes.value = preview.dtypes;
  predRows = preview.rows;
  predRowCount.value = preview.rows.length;
  predTotalRows.value = preview.total_rows;
  predVersion.value = version;
  predRepeatsKeys.value = detectRepeatedKeys(preview.columns);

  const hadConfidence = confidenceColumn.value;
  pruneSuggestionSelections();
  const lostConfidence = !!hadConfidence && !confidenceColumn.value;

  const spec = buildJoinKeySpec(props.session.keyColumns, props.session.columns, preview.columns);
  if (!spec || !preview.columns.includes(suggestionColumn.value)) {
    suggestionsDisabled.value = true;
    suggestionNote.value =
      "The prediction table's schema changed, so suggestions are off for this pass.";
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

watch(confidenceColumn, (value) => {
  if (!value) leastConfidentFirst.value = false;
});

watch(canUseProbabilities, (usable) => {
  if (!usable) useProbabilityColumns.value = false;
});

watch(leastConfidentFirst, () => {
  if (!hydrating.value) persistSettings();
});

// A prediction source that arrives after Start still gets picked up; the visit
// order is deliberately left alone so no row moves mid-pass.
watch([predictionTableId, suggestionColumn, predColumns], () => {
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
  background: var(--color-background-soft, #f0f2f5);
  color: var(--color-text-secondary);
}

.refresh-badge {
  font-size: var(--font-size-xs, 11px);
  font-weight: 400;
  padding: 2px 8px;
  border-radius: 10px;
  border: 1px solid var(--color-primary, #409eff);
  color: var(--color-primary, #409eff);
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
  color: var(--color-warning, #e6a23c);
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
  color: var(--color-primary, #409eff);
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
  color: var(--color-primary, #409eff);
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
  color: var(--color-primary, #409eff);
}

.suggestion-line {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2, 8px);
  margin-top: var(--spacing-3, 12px);
  padding-top: var(--spacing-2, 8px);
  border-top: 1px solid var(--color-border, #dcdfe6);
  font-size: var(--font-size-sm, 13px);
  color: var(--color-primary, #409eff);
}

.enter-hint {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.unknown-chip {
  font-size: var(--font-size-xs, 11px);
  padding: 2px 8px;
  border-radius: 10px;
  background: var(--color-background-soft, #f0f2f5);
  color: var(--color-text-secondary);
}

.mini-btn {
  border: 1px solid var(--color-primary, #409eff);
  border-radius: 4px;
  background: none;
  padding: 1px 6px;
  cursor: pointer;
  font-size: var(--font-size-xs, 11px);
  color: var(--color-primary, #409eff);
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

.class-btn.suggested {
  border-color: var(--color-primary, #409eff);
  background: rgba(64, 158, 255, 0.08);
}

.class-btn.suggested.active {
  border-color: var(--color-success, #67c23a);
  background: rgba(103, 194, 58, 0.08);
}

.class-prob {
  font-size: var(--font-size-xs, 11px);
  color: var(--color-text-secondary);
}

.suggested-chip {
  font-size: var(--font-size-xs, 11px);
  padding: 1px 6px;
  border-radius: 8px;
  background: rgba(64, 158, 255, 0.14);
  color: var(--color-primary, #409eff);
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
  color: var(--color-success, #67c23a);
}

.done-actions {
  display: flex;
  gap: var(--spacing-2, 8px);
}
</style>
