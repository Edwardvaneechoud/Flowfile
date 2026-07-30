<template>
  <div v-if="isLoaded && nodeFuzzyJoin" class="fuzzy-join-container">
    <generic-node-settings
      v-model="nodeFuzzyJoin"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <!-- Segmented control rather than a second tab bar: the drawer already has
           one directly above this, and two competing tab rows read as a mistake. -->
      <div class="fuzzy-tabs" role="tablist">
        <button
          v-for="tab in tabs"
          :key="tab.id"
          class="fuzzy-tab"
          :class="{ 'is-active': activeTab === tab.id }"
          type="button"
          role="tab"
          :aria-selected="activeTab === tab.id"
          @click="activeTab = tab.id"
        >
          {{ tab.label }}
          <span
            v-if="tab.id === 'match' && hasInvalidFields"
            class="fuzzy-tab-alert"
            title="A match rule still needs attention"
            aria-label="A match rule still needs attention"
            >●</span
          >
        </button>
      </div>

      <section v-if="activeTab === 'match'" class="tab-panel">
        <p class="tab-hint">
          Two rows are paired when their columns are similar enough to clear the threshold. A row
          has to satisfy every rule below to match.
        </p>

        <div v-if="nodeFuzzyJoin?.join_input" class="rule-list">
          <article
            v-for="(fuzzyMap, index) in nodeFuzzyJoin.join_input.join_mapping"
            :key="index"
            class="rule-card"
            :class="{ 'is-invalid': missingColumns(fuzzyMap).length > 0 }"
          >
            <header class="rule-header">
              <span class="rule-title">Match rule {{ index + 1 }}</span>

              <template v-if="missingColumns(fuzzyMap).length">
                <unavailable-field
                  tooltip-text="This rule points at a column that no longer exists upstream"
                />
                <span class="rule-alert">
                  {{ missingColumns(fuzzyMap).join(" and ") }} no longer exists upstream
                </span>
              </template>
              <span v-else-if="isRuleIncomplete(fuzzyMap)" class="rule-hint">
                Pick a column on both sides
              </span>

              <button
                v-if="canRemoveRule"
                class="btn btn-sm btn-icon btn-ghost rule-remove"
                type="button"
                title="Remove this rule"
                aria-label="Remove this rule"
                @click="removeJoinCondition(index)"
              >
                ✕
              </button>
            </header>

            <div class="field-grid">
              <div class="field">
                <span class="form-label">Left column</span>
                <column-selector
                  v-model="fuzzyMap.left_col"
                  :value="fuzzyMap.left_col"
                  :column-options="result?.main_input?.columns"
                  @update:value="(value: string) => handleChange(value, index, 'left')"
                />
              </div>

              <div class="field">
                <span class="form-label">Right column</span>
                <column-selector
                  v-model="fuzzyMap.right_col"
                  :value="fuzzyMap.right_col"
                  :column-options="result?.right_input?.columns"
                  @update:value="(value: string) => handleChange(value, index, 'right')"
                />
              </div>
            </div>

            <div class="field-grid">
              <div class="field">
                <label :for="`threshold-score-${index}`" class="form-label">
                  Similarity threshold
                </label>
                <div class="threshold-control">
                  <input
                    :id="`threshold-score-${index}`"
                    v-model.number="fuzzyMap.threshold_score"
                    type="range"
                    min="0"
                    max="100"
                    step="1"
                    class="range-slider"
                    :style="{ '--range-fill': rangeFill(fuzzyMap) }"
                  />
                  <div class="threshold-value">
                    <input
                      v-model.number="fuzzyMap.threshold_score"
                      type="number"
                      min="0"
                      max="100"
                      class="threshold-input"
                      :aria-label="`Similarity threshold for match rule ${index + 1}`"
                      @change="clampThreshold(fuzzyMap)"
                    />
                    <span class="threshold-unit">%</span>
                  </div>
                </div>
              </div>

              <div class="field">
                <label :for="`fuzzy-type-${index}`" class="form-label">Match algorithm</label>
                <div class="native-select">
                  <select
                    :id="`fuzzy-type-${index}`"
                    v-model="fuzzyMap.fuzzy_type"
                    class="native-select__input"
                  >
                    <option
                      v-for="option in fuzzyMatchOptions"
                      :key="option.value"
                      :value="option.value"
                    >
                      {{ option.label }}
                    </option>
                  </select>
                </div>
              </div>
            </div>
          </article>

          <button class="btn btn-sm add-rule" type="button" @click="addJoinCondition()">
            + Add match rule
          </button>
        </div>
      </section>

      <section v-else class="tab-panel">
        <p class="tab-hint">
          Pick the columns each input contributes to the result. Match keys stay available whether
          or not you keep them.
        </p>

        <select-dynamic
          v-if="nodeFuzzyJoin?.join_input"
          :select-inputs="nodeFuzzyJoin?.join_input.left_select.renames"
          :show-keep-option="true"
          :show-headers="true"
          :show-new-columns="false"
          :show-title="true"
          title="Left data"
          @update-select-inputs="
            (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, true)
          "
        />

        <select-dynamic
          v-if="nodeFuzzyJoin?.join_input"
          :select-inputs="nodeFuzzyJoin?.join_input.right_select.renames"
          :show-keep-option="true"
          :show-headers="true"
          :show-new-columns="false"
          :show-title="true"
          title="Right data"
          @update-select-inputs="
            (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, false)
          "
        />
      </section>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { NodeJoin, FuzzyJoinSettings, SelectInput, FuzzyMap } from "../../../baseNode/nodeInput";
import ColumnSelector from "../../../baseNode/page_objects/dropDown.vue";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import unavailableField from "../../../baseNode/selectComponents/UnavailableFields.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { CodeLoader } from "vue-content-loader";

const tabs = [
  { id: "match", label: "Match rules" },
  { id: "fields", label: "Output columns" },
] as const;

const activeTab = ref<"match" | "fields">("match");

const containsVal = (arr: string[], val: string) => {
  return arr.includes(val);
};

const result = ref<NodeData | null>(null);
const nodeStore = useNodeStore();
const isLoaded = ref(false);
const nodeFuzzyJoin = ref<NodeJoin | null>(null);

// Missing join fields light the canvas dot via the backend settings validation;
// the in-drawer field highlighting (hasInvalidFields) stays client-side.
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeFuzzyJoin,
});

const createSelectInput = (field: string): SelectInput => {
  return {
    old_name: field,
    new_name: field,
    position: 0,
    keep: true,
    is_altered: false,
    data_type_change: false,
    is_available: true,
    original_position: 0,
  };
};
const updateSelectInputsHandler = (updatedInputs: SelectInput[], isLeft: boolean) => {
  if (isLeft && nodeFuzzyJoin.value) {
    nodeFuzzyJoin.value.join_input.left_select.renames = updatedInputs;
  } else if (nodeFuzzyJoin.value) {
    nodeFuzzyJoin.value.join_input.right_select.renames = updatedInputs;
  }
};

const fuzzyMatchOptions = [
  { value: "levenshtein", label: "Levenshtein" },
  { value: "jaro", label: "Jaro" },
  { value: "jaro_winkler", label: "Jaro Winkler" },
  { value: "hamming", label: "Hamming" },
  { value: "damerau_levenshtein", label: "Damerau Levenshtein" },
  { value: "indel", label: "Indel" },
];

/** A configured column that has since disappeared upstream — the actionable error. */
const missingColumns = (fuzzyMap: FuzzyMap): string[] => {
  const missing: string[] = [];
  if (fuzzyMap.left_col && !containsVal(result.value?.main_input?.columns ?? [], fuzzyMap.left_col))
    missing.push(`"${fuzzyMap.left_col}"`);
  if (
    fuzzyMap.right_col &&
    !containsVal(result.value?.right_input?.columns ?? [], fuzzyMap.right_col)
  )
    missing.push(`"${fuzzyMap.right_col}"`);
  return missing;
};

/** A rule the user hasn't finished yet — a nudge, not an error. */
const isRuleIncomplete = (fuzzyMap: FuzzyMap) => !fuzzyMap.left_col || !fuzzyMap.right_col;

const hasInvalidFields = computed(() => {
  if (!nodeFuzzyJoin.value?.join_input || !result.value) {
    return false;
  }
  return nodeFuzzyJoin.value.join_input.join_mapping.some(
    (fuzzyMap) => isRuleIncomplete(fuzzyMap) || missingColumns(fuzzyMap).length > 0,
  );
});

const canRemoveRule = computed(
  () => (nodeFuzzyJoin.value?.join_input?.join_mapping.length ?? 0) > 1,
);

const rangeFill = (fuzzyMap: FuzzyMap) => {
  const score = Number(fuzzyMap.threshold_score);
  return `${Number.isFinite(score) ? Math.min(100, Math.max(0, score)) : 0}%`;
};

/** The number field lets you type anything; the slider only speaks 0-100. */
const clampThreshold = (fuzzyMap: FuzzyMap) => {
  const score = Number(fuzzyMap.threshold_score);
  fuzzyMap.threshold_score = Number.isFinite(score) ? Math.min(100, Math.max(0, score)) : 75;
};

const getEmptySetup = (left_fields: string[], right_fields: string[]): FuzzyJoinSettings => {
  return {
    join_mapping: [
      {
        left_col: "",
        right_col: "",
        threshold_score: 75,
        fuzzy_type: "levenshtein",
        valid: true,
      },
    ],
    left_select: {
      renames: left_fields.map(createSelectInput),
    },
    right_select: {
      renames: right_fields.map(createSelectInput),
    },
    aggregate_output: false,
  };
};

const loadNodeData = async (nodeId: number) => {
  result.value = await nodeStore.getNodeData(nodeId, false);
  nodeFuzzyJoin.value = result.value?.setting_input;
  if (nodeFuzzyJoin.value) {
    if (!nodeFuzzyJoin.value.is_setup && result.value?.main_input) {
      if (result.value.main_input.columns && result.value.right_input?.columns) {
        nodeFuzzyJoin.value.join_input = getEmptySetup(
          result.value.main_input.columns,
          result.value.right_input.columns,
        );
      }
    }
    isLoaded.value = true;
  }
};

const addJoinCondition = () => {
  const newCondition: FuzzyMap = {
    left_col: "",
    right_col: "",
    threshold_score: 75,
    fuzzy_type: "levenshtein",
    valid: true,
  };
  nodeFuzzyJoin.value?.join_input?.join_mapping.push(newCondition);
};
const removeJoinCondition = (index: number) => {
  nodeFuzzyJoin.value?.join_input?.join_mapping.splice(index, 1);
};

const handleChange = (newValue: string, index: number, side: string) => {
  if (side === "left") {
    if (nodeFuzzyJoin.value?.join_input) {
      nodeFuzzyJoin.value.join_input.join_mapping[index].left_col = newValue;
    }
  } else {
    if (nodeFuzzyJoin.value?.join_input) {
      nodeFuzzyJoin.value.join_input.join_mapping[index].right_col = newValue;
    }
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
  hasInvalidFields,
});
</script>

<style scoped>
.fuzzy-join-container {
  font-family: var(--font-family-base);
  color: var(--color-text-primary);
  max-width: 100%;
}

/* ===== Sub-navigation ===== */
.fuzzy-tabs {
  display: inline-flex;
  /* The tab pane is a column flexbox, so without this the track stretches edge
     to edge and reads as a second tab bar instead of a segmented control. */
  align-self: flex-start;
  gap: var(--spacing-0-5);
  margin: var(--spacing-2) var(--spacing-1) var(--spacing-1);
  padding: var(--spacing-0-5);
  border-radius: var(--border-radius-lg);
  background-color: var(--color-background-tertiary);
}

.fuzzy-tab {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: var(--spacing-1-5) var(--spacing-3);
  border: none;
  border-radius: var(--border-radius-md);
  background-color: transparent;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  transition: all var(--transition-fast) var(--transition-timing);
}

.fuzzy-tab:hover:not(.is-active) {
  color: var(--color-text-primary);
}

.fuzzy-tab.is-active {
  background-color: var(--color-background-primary);
  color: var(--color-accent);
  box-shadow: var(--shadow-xs);
}

.fuzzy-tab-alert {
  color: var(--color-warning);
  font-size: var(--font-size-2xs);
  line-height: 1;
}

/* ===== Panels ===== */
.tab-panel {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-1);
}

.tab-hint {
  margin: 0 var(--spacing-2);
  font-size: var(--font-size-xs);
  line-height: var(--line-height-relaxed);
  color: var(--color-text-tertiary);
}

/* ===== Match rules ===== */
.rule-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.rule-card {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background-color: var(--color-background-primary);
  box-shadow: var(--shadow-xs);
  transition: border-color var(--transition-fast) var(--transition-timing);
}

.rule-card:hover {
  border-color: var(--color-border-secondary);
}

.rule-card.is-invalid {
  border-color: var(--color-danger);
  background-color: var(--color-danger-light);
}

.rule-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-height: 28px;
}

.rule-title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.rule-alert {
  font-size: var(--font-size-xs);
  color: var(--color-danger);
}

.rule-hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

/* Always last in the header, whatever precedes it. */
.rule-remove {
  margin-left: auto;
  box-shadow: none;
}

.rule-remove:hover:not(:disabled) {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

/* auto-fit rather than a viewport media query: the drawer is a resizable panel,
   so the fields have to fold on the container's width, not the window's. */
.field-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: var(--spacing-3);
}

.field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1-5);
  min-width: 0;
}

.add-rule {
  width: 100%;
  border: 1px dashed var(--color-border-secondary);
  background-color: transparent;
  color: var(--color-text-secondary);
  box-shadow: none;
}

.add-rule:hover {
  border-color: var(--color-accent);
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
}

/* ===== Threshold ===== */
.threshold-control {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}

.range-slider {
  flex: 1;
  min-width: 0;
  height: 4px;
  margin: 0;
  padding: 0;
  border-radius: var(--border-radius-full);
  /* Fill left of the thumb so the value is readable without chasing the number. */
  background: linear-gradient(
    to right,
    var(--color-accent) 0 var(--range-fill, 0%),
    var(--color-border-primary) var(--range-fill, 0%) 100%
  );
  appearance: none;
  -webkit-appearance: none;
  outline: none;
  cursor: pointer;
}

.range-slider::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 14px;
  height: 14px;
  border: 2px solid var(--color-accent);
  border-radius: 50%;
  background-color: var(--color-background-primary);
  box-shadow: var(--shadow-xs);
  cursor: grab;
}

.range-slider::-moz-range-track {
  background: transparent;
}

.range-slider::-moz-range-thumb {
  width: 14px;
  height: 14px;
  border: 2px solid var(--color-accent);
  border-radius: 50%;
  background-color: var(--color-background-primary);
  box-shadow: var(--shadow-xs);
  cursor: grab;
}

.range-slider:focus-visible {
  box-shadow: 0 0 0 3px var(--color-focus-ring-accent);
}

.threshold-value {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  flex-shrink: 0;
}

.threshold-input {
  width: 52px;
  padding: var(--spacing-1) var(--spacing-1-5);
  border: 1px solid var(--input-border);
  border-radius: var(--border-radius-sm);
  background-color: var(--input-bg);
  color: var(--color-text-primary);
  font-family: inherit;
  font-size: var(--font-size-sm);
  text-align: right;
  outline: none;
  transition: border-color var(--transition-fast) var(--transition-timing);
}

.threshold-input:focus {
  border-color: var(--input-border-focus);
  box-shadow: 0 0 0 2px var(--color-focus-ring-accent);
}

.threshold-unit {
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

/* ===== Algorithm select =====
   Sized to match the column dropdowns next to it so the two rows line up. */
.native-select {
  position: relative;
  display: flex;
  align-items: center;
}

.native-select::after {
  content: "";
  position: absolute;
  right: 12px;
  width: 7px;
  height: 7px;
  border-right: 1.5px solid var(--color-text-tertiary);
  border-bottom: 1.5px solid var(--color-text-tertiary);
  transform: translateY(-2px) rotate(45deg);
  pointer-events: none;
}

.native-select__input {
  width: 100%;
  box-sizing: border-box;
  padding: 8px 30px 8px 12px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
  font-family: inherit;
  font-size: var(--font-size-base);
  line-height: 1.4;
  box-shadow: var(--shadow-xs);
  appearance: none;
  -webkit-appearance: none;
  outline: none;
  cursor: pointer;
  transition: border-color var(--transition-fast) var(--transition-timing);
}

.native-select__input:focus {
  border-color: var(--input-border-focus);
  box-shadow: 0 0 0 3px var(--color-focus-ring-accent);
}
</style>
