<template>
  <div v-if="isLoaded && nodeFuzzyJoin" class="fuzzy-join-container">
    <generic-node-settings v-model="nodeFuzzyJoin">
      <!-- Tabs Navigation -->
      <div class="tabs-navigation">
        <button
          class="tab-button"
          :class="{ active: activeTab === 'match' }"
          @click="activeTab = 'match'"
        >
          Match Settings
        </button>
        <button
          class="tab-button"
          :class="{ active: activeTab === 'fields' }"
          @click="activeTab = 'fields'"
        >
          Select Fields
        </button>
      </div>

      <!-- Match Settings Tab -->
      <div v-if="activeTab === 'match'" class="tab-content">
        <div class="settings-card">
          <div class="card-header">
            <h3 class="section-title">Fuzzy match settings</h3>
          </div>
          <div class="card-content">
            <div v-if="nodeFuzzyJoin?.join_input" class="join-settings">
              <div
                v-for="(fuzzyMap, index) in nodeFuzzyJoin?.join_input.join_mapping"
                :key="index"
                class="setting-panel"
              >
                <div class="setting-header">
                  <h4 class="setting-title">
                    <unavailableField
                      v-if="
                        !(
                          containsVal(result?.main_input?.columns ?? [], fuzzyMap.left_col) &&
                          containsVal(result?.right_input?.columns ?? [], fuzzyMap.right_col)
                        )
                      "
                      tooltip-text="Join is not valid"
                      class="unavailable-field"
                    />
                    Setting {{ index + 1 }}
                  </h4>
                  <button
                    v-if="nodeFuzzyJoin?.join_input.join_mapping.length > 1"
                    class="remove-button"
                    type="button"
                    aria-label="Remove setting"
                    @click="removeJoinCondition(index)"
                  >
                    Remove setting
                  </button>
                </div>

                <div class="columns-grid">
                  <div class="column-field">
                    <label>Left column</label>
                    <column-selector
                      v-model="fuzzyMap.left_col"
                      :value="fuzzyMap.left_col"
                      :column-options="result?.main_input?.columns"
                      @update:value="(value: string) => handleChange(value, index, 'left')"
                    />
                  </div>

                  <div class="column-field">
                    <label>Right column</label>
                    <column-selector
                      v-model="fuzzyMap.right_col"
                      :value="fuzzyMap.right_col"
                      :column-options="result?.right_input?.columns"
                      @update:value="(value: string) => handleChange(value, index, 'right')"
                    />
                  </div>
                </div>

                <div class="settings-grid">
                  <div class="threshold-field">
                    <label for="threshold-score">Threshold score</label>
                    <div class="range-container">
                      <input
                        :id="`threshold-score-${index}`"
                        v-model="fuzzyMap.threshold_score"
                        type="range"
                        min="0"
                        max="100"
                        step="1"
                        class="range-slider"
                      />
                      <div class="range-value">{{ fuzzyMap.threshold_score }}%</div>
                    </div>
                  </div>

                  <div class="select-field">
                    <label for="fuzzy-type">Match algorithm</label>
                    <div class="select-wrapper">
                      <select
                        :id="`fuzzy-type-${index}`"
                        v-model="fuzzyMap.fuzzy_type"
                        class="select-input"
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
              </div>

              <button class="add-button" type="button" @click="addJoinCondition()">
                Add setting
              </button>
            </div>
          </div>
        </div>
      </div>

      <!-- Select Fields Tab -->
      <div v-if="activeTab === 'fields'" class="tab-content">
        <div class="settings-card">
          <div class="card-header">
            <h3 class="section-title">Select fields to include</h3>
          </div>

          <div class="card-content">
            <select-dynamic
              v-if="nodeFuzzyJoin?.join_input"
              :select-inputs="nodeFuzzyJoin?.join_input.right_select.renames"
              :show-keep-option="true"
              :show-headers="true"
              :show-new-columns="false"
              :show-title="true"
              :show-data="true"
              title="Right data"
              class="select-section"
              @update-select-inputs="
                (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, false)
              "
            />

            <select-dynamic
              v-if="nodeFuzzyJoin?.join_input"
              :select-inputs="nodeFuzzyJoin?.join_input.left_select.renames"
              :show-keep-option="true"
              :show-title="true"
              :show-headers="true"
              :show-new-columns="false"
              :show-data="true"
              title="Left data"
              class="select-section"
              @update-select-inputs="
                (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, true)
              "
            />
          </div>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, onMounted, nextTick, computed } from "vue";
import { useNodeStore } from "../../../../../stores/column-store";
import { useNodeSettings } from "../../../../../composables";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { NodeJoin, FuzzyJoinSettings, SelectInput, FuzzyMap } from "../../../baseNode/nodeInput";
import ColumnSelector from "../../../baseNode/page_objects/dropDown.vue";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import unavailableField from "../../../baseNode/selectComponents/UnavailableFields.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { CodeLoader } from "vue-content-loader";

const activeTab = ref("match"); // Default to match settings tab

const containsVal = (arr: string[], val: string) => {
  return arr.includes(val);
};

const result = ref<NodeData | null>(null);
const nodeStore = useNodeStore();
const isLoaded = ref(false);
const nodeFuzzyJoin = ref<NodeJoin | null>(null);

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

const hasInvalidFields = computed(() => {
  if (!nodeFuzzyJoin.value?.join_input || !result.value) {
    return false;
  }
  return nodeFuzzyJoin.value.join_input.join_mapping.some((fuzzyMap) => {
    const leftValid = containsVal(result.value?.main_input?.columns ?? [], fuzzyMap.left_col);
    const rightValid = containsVal(result.value?.right_input?.columns ?? [], fuzzyMap.right_col);
    return !(leftValid && rightValid);
  });
});

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
  if (!nodeFuzzyJoin.value?.is_setup && result.value?.main_input) {
    if (nodeFuzzyJoin.value) {
      if (result.value?.main_input.columns && result.value?.right_input?.columns) {
        nodeFuzzyJoin.value.join_input = getEmptySetup(
          result.value.main_input.columns,
          result.value.right_input.columns,
        );
        isLoaded.value = true;
      }
    }
  } else {
    isLoaded.value = true;
  }
  isLoaded.value = true;
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

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeData: nodeFuzzyJoin,
  beforeSave: () => {
    if (nodeFuzzyJoin.value) {
      nodeFuzzyJoin.value.is_setup = true;
    }
  },
  afterSave: () => {
    if (hasInvalidFields.value && nodeFuzzyJoin.value) {
      nodeStore.setNodeValidation(nodeFuzzyJoin.value.node_id, {
        isValid: false,
        error: "Join fields are not valid",
      });
    } else if (nodeFuzzyJoin.value) {
      nodeStore.setNodeValidation(nodeFuzzyJoin.value.node_id, {
        isValid: true,
        error: "",
      });
    }
  },
});

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
  hasInvalidFields,
});

onMounted(async () => {
  await nextTick();
});
</script>

<style scoped>
/* Modern styling for an AI app */
.fuzzy-join-container {
  font-family: var(--font-family-base);
  color: var(--color-text-primary);
  max-width: 100%;
}

/* Tabs Navigation */
.tabs-navigation {
  display: flex;
  border-bottom: 1px solid var(--color-border-light);
  margin-bottom: 1.25rem;
  background-color: var(--color-background-secondary);
  border-radius: 8px 8px 0 0;
  overflow: hidden;
}

.tab-button {
  background: none;
  border: none;
  padding: 0.75rem 1.25rem;
  font-size: 0.9rem;
  font-weight: 500;
  color: #718096;
  cursor: pointer;
  transition: all 0.2s ease;
  border-bottom: 2px solid transparent;
  outline: none;
  flex: 1;
  text-align: center;
}

.tab-button:hover {
  color: var(--color-text-primary);
}

.tab-button.active {
  border-bottom-color: var(--color-accent-subtle);
  color: var(--color-accent);
  background-color: var(--color-accent-subtle);
}

.tab-content {
  min-height: 300px;
}

/* Card styling */
.settings-card {
  background-color: var(--color-background-primary);
  border-radius: 8px;
  box-shadow: 0 1px 3px rgba(125, 107, 107, 0.05);
  margin-bottom: 1.5rem;
  border: 1px solid var(--color-border-primary);
  overflow: hidden;
}

.card-header {
  padding: 1rem 1.25rem;
  border-bottom: 1px solid var(--color-border-light);
  background-color: var(--color-background-secondary);
}

.section-title {
  margin: 0;
  font-size: 1rem;
  font-weight: 600;
  color: var(--color-text-primary);
}

.card-content {
  padding: 1rem 1.25rem;
}

/* Join Settings */
.join-settings {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.setting-panel {
  background-color: var(--color-background-secondary);
  border-radius: 6px;
  padding: 1rem;
  border: 1px solid var(--color-border-secondary);
  transition: all 0.2s ease;
  color: var(--color-text-secondary);
}

.setting-panel:hover {
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.04);
  border-color: var(--color-border-light);
}

.setting-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
}

.setting-title {
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--color-text-primary);
  margin: 0;
  display: flex;
  align-items: center;
}

.unavailable-field {
  margin-right: 0.5rem;
}

/* Column Selection Grid */
.columns-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
  margin-bottom: 1rem;
}

.column-field {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.column-field label {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--color-text-secondary);
}

/* Settings Grid */
.settings-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
}

.threshold-field,
.select-field {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.threshold-field label,
.select-field label {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--color-text-secondary);
}

/* Range Input */
.range-container {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.range-slider {
  flex: 1;
  width: 100%;
  height: 6px;
  border-radius: 3px;
  background: var(--color-background-secondary);
  outline: none;
}

.range-slider::-webkit-slider-thumb {
  -webkit-appearance: none;
  appearance: none;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: var(--color-accent);
  cursor: pointer;
  border: 2px solid var(--color-accent);
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
}

.range-slider::-moz-range-thumb {
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: var(--color-accent);
  cursor: pointer;
  border: 2px solid white;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
}

.range-value {
  min-width: 3rem;
  text-align: right;
  font-size: 0.875rem;
  font-weight: 500;
  color: #4a5568;
}

/* Select Input */
.select-wrapper {
  position: relative;
}

.select-input {
  width: 100%;
  padding: 0.5rem 0.75rem;
  font-size: 0.875rem;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  background-color: var(--color-background-primary);
  color: #4a5568;
  appearance: none;
  outline: none;
  transition: all 0.2s ease;
}

.select-input:focus {
  border-color: #3182ce;
  box-shadow: 0 0 0 2px rgba(49, 130, 206, 0.15);
}

/* Buttons */
.add-button,
.remove-button {
  padding: 0.5rem 0.75rem;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  background-color: #f7fafc;
  color: #4a5568;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
}

.add-button {
  background-color: #ebf8ff;
  color: #3182ce;
  margin-top: 0.5rem;
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: center;
}

.add-button:hover {
  background-color: #bee3f8;
}

.remove-button {
  background-color: transparent;
  border: 1px solid #e2e8f0;
}

.remove-button:hover {
  background-color: #fee2e2;
  color: #e53e3e;
  border-color: #fbd5d5;
}

.select-section {
  margin-top: 1rem;
}

.select-section + .select-section {
  margin-top: 2rem;
  padding-top: 1.5rem;
  border-top: 1px solid #edf2f7;
}

/* Responsive adjustments */
@media (max-width: 768px) {
  .columns-grid,
  .settings-grid {
    grid-template-columns: 1fr;
  }
}
</style>
