<template>
  <div v-if="dataLoaded && nodeJoin" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeJoin"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-subtitle">Join columns</div>
      <div class="join-content">
        <div class="join-type-selector">
          <label class="join-type-label">Join Type:</label>
          <drop-down
            v-if="nodeJoin"
            v-model="nodeJoin.join_input.how"
            :column-options="joinTypes"
            placeholder="Select join type"
            :allow-other="false"
            @error="handleJoinTypeError"
          />
        </div>
        <div class="join-mapping-section">
          <div class="suggest-keys-row">
            <button class="suggest-keys-button" :disabled="aiSuggesting" @click="suggestJoinKeys">
              <span v-if="aiSuggesting">Asking AI…</span>
              <span v-else>✨ Suggest keys</span>
            </button>
            <span v-if="aiSuggestNotice" class="suggest-keys-notice">
              {{ aiSuggestNotice }}
            </span>
          </div>
          <div class="table-wrapper">
            <div class="selectors-header">
              <div class="selectors-title">L</div>
              <div class="selectors-title">R</div>
              <div class="selectors-title"></div>
            </div>
            <div class="selectors-container">
              <div
                v-for="(selector, index) in nodeJoin?.join_input.join_mapping"
                :key="index"
                class="selectors-row"
              >
                <div class="selector-wrapper">
                  <drop-down
                    v-model="selector.left_col"
                    :value="selector.left_col"
                    :column-options="result?.main_input?.columns"
                    @update:value="(value: string) => handleChange(value, index, 'left')"
                  />
                </div>
                <div class="selector-wrapper">
                  <drop-down
                    v-model="selector.right_col"
                    :value="selector.right_col"
                    :column-options="result?.right_input?.columns"
                    @update:value="(value: string) => handleChange(value, index, 'right')"
                  />
                </div>
                <div class="action-buttons">
                  <button class="action-button remove-button" @click="removeJoinCondition(index)">
                    -
                  </button>
                  <button
                    v-if="index === (nodeJoin?.join_input.join_mapping?.length ?? 0) - 1"
                    class="action-button add-button"
                    @click="addJoinCondition"
                  >
                    +
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
      <select-dynamic
        v-if="showColumnSelection"
        :select-inputs="nodeJoin?.join_input.left_select.renames"
        :show-keep-option="true"
        :show-title="true"
        :show-headers="true"
        :show-data="true"
        title="Left data"
        @update-select-inputs="
          (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, true)
        "
      />
      <select-dynamic
        v-if="showColumnSelection"
        :select-inputs="nodeJoin?.join_input.right_select.renames"
        :show-keep-option="true"
        :show-headers="true"
        :show-title="true"
        :show-data="true"
        title="Right data"
        @update-select-inputs="
          (updatedInputs: any) => updateSelectInputsHandler(updatedInputs, false)
        "
      />
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { useAiAutocompleteStore } from "../../../../../stores/ai-autocomplete-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { SelectInput } from "../../../baseNode/nodeInput";
import { NodeJoin } from "./joinInterfaces";
import DropDown from "../../../baseNode/page_objects/dropDown.vue";
import selectDynamic from "../../../baseNode/selectComponents/selectDynamic.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

type JoinType = "inner" | "left" | "right" | "full" | "semi" | "anti" | "cross";

const joinTypes: JoinType[] = ["inner", "left", "right", "full", "semi", "anti", "cross"];

const JOIN_TYPES_WITHOUT_COLUMN_SELECTION: JoinType[] = ["anti", "semi"];

const handleJoinTypeError = (error: string) => {
  console.error("Join type error:", error);
};

const result = ref<NodeData | null>(null);
const nodeStore = useNodeStore();
const aiAutocompleteStore = useAiAutocompleteStore();
const dataLoaded = ref(false);
const nodeJoin = ref<NodeJoin | null>(null);
const aiSuggesting = ref(false);
const aiSuggestNotice = ref<string>("");

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeJoin,
});

const updateSelectInputsHandler = (updatedInputs: SelectInput[], isLeft: boolean) => {
  if (isLeft && nodeJoin.value) {
    nodeJoin.value.join_input.left_select.renames = updatedInputs;
  } else if (nodeJoin.value) {
    nodeJoin.value.join_input.right_select.renames = updatedInputs;
  }
};

const loadNodeData = async (nodeId: number) => {
  result.value = await nodeStore.getNodeData(nodeId, false);
  nodeJoin.value = result.value?.setting_input;
  if (result.value) {
    dataLoaded.value = true;
  }
};

const addJoinCondition = () => {
  if (nodeJoin.value) {
    nodeJoin.value.join_input.join_mapping.push({
      left_col: "",
      right_col: "",
    });
  }
};

const showColumnSelection = computed(() => {
  const joinType = nodeJoin.value?.join_input.how;
  return joinType && !JOIN_TYPES_WITHOUT_COLUMN_SELECTION.includes(joinType);
});

const removeJoinCondition = (index: number) => {
  if (nodeJoin.value && index >= 0) {
    nodeJoin.value.join_input.join_mapping.splice(index, 1);
  }
};

const handleChange = (newValue: string, index: number, side: string) => {
  if (side === "left") {
    if (nodeJoin.value) {
      nodeJoin.value.join_input.join_mapping[index].left_col = newValue;
    }
  } else {
    if (nodeJoin.value) {
      nodeJoin.value.join_input.join_mapping[index].right_col = newValue;
    }
  }
};

// — ask the AI for likely (left_col, right_col) pairs based on the
// upstream column overlap. Empty rows in `join_mapping` are populated;
// rows the user has already filled are NEVER overwritten.
const suggestJoinKeys = async () => {
  aiSuggestNotice.value = "";
  if (!nodeJoin.value || !result.value) return;
  const flowId = nodeStore.flow_id;
  const leftNodeId = result.value.main_input?.node_id;
  const rightNodeId = result.value.right_input?.node_id;
  if (flowId == null || leftNodeId == null || rightNodeId == null) {
    aiSuggestNotice.value = "Connect both inputs first.";
    return;
  }

  aiSuggesting.value = true;
  try {
    const response = await aiAutocompleteStore.getJoinKeySuggestions({
      flowId: Number(flowId),
      leftNodeId,
      rightNodeId,
      how: nodeJoin.value.join_input.how,
    });
    if (response === null) {
      aiSuggestNotice.value = aiAutocompleteStore.aiDisabled
        ? "AI is disabled — enable it in settings."
        : "Couldn't reach the AI service.";
      return;
    }
    if (response.degraded) {
      aiSuggestNotice.value =
        response.reason && response.reason.length > 0
          ? `AI degraded: ${response.reason}`
          : "AI couldn't ground against the upstream schemas — run the upstream nodes first.";
      return;
    }
    const pairs = response.keyPairs;
    if (pairs.length === 0) {
      aiSuggestNotice.value = "No plausible key pairs found.";
      return;
    }

    const mapping = nodeJoin.value.join_input.join_mapping;
    let filled = 0;
    for (const pair of pairs) {
      // Find the next empty row, or append a new one.
      const emptyIdx = mapping.findIndex(
        (m) => (!m.left_col || m.left_col === "") && (!m.right_col || m.right_col === ""),
      );
      if (emptyIdx >= 0) {
        mapping[emptyIdx].left_col = pair.leftCol;
        mapping[emptyIdx].right_col = pair.rightCol;
      } else {
        mapping.push({ left_col: pair.leftCol, right_col: pair.rightCol });
      }
      filled += 1;
    }
    aiSuggestNotice.value = `Filled ${filled} pair${filled === 1 ? "" : "s"}.`;
  } finally {
    aiSuggesting.value = false;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
/* Join Type Selector */
.join-type-selector {
  display: flex;
  align-items: center;
  margin: 12px;
  gap: 10px;
}

.join-type-label {
  font-size: 12px;
  color: var(--color-text-primary);
  font-weight: 500;
  min-width: 70px;
}

/* — Suggest keys button row */
.suggest-keys-row {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 5px 5px 0 5px;
}

.suggest-keys-button {
  background-color: var(--color-background-muted);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  padding: 4px 10px;
  font-size: 12px;
  cursor: pointer;
  color: var(--color-text-primary);
}

.suggest-keys-button:disabled {
  cursor: progress;
  opacity: 0.7;
}

.suggest-keys-button:hover:not(:disabled) {
  background-color: var(--color-background-secondary);
}

.suggest-keys-notice {
  font-size: 11px;
  color: var(--color-text-secondary);
}

/* Join Mapping Section */
.table-wrapper {
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  overflow: hidden;
  margin: 5px;
}

.selectors-header {
  display: flex;
  justify-content: space-between;
  padding: 8px 16px;
  background-color: var(--color-background-muted);
  border-bottom: 1px solid var(--color-border-primary);
}

.selectors-title {
  flex: 1;
  text-align: center;
  font-size: 12px;
  color: var(--color-text-secondary);
  font-weight: 500;
}

.selectors-container {
  padding: 12px;
  box-sizing: border-box;
  width: 100%;
  display: flex;
  justify-content: space-between;
  flex-direction: column;
}

.selectors-row {
  display: flex;
  gap: 12px;
  margin-bottom: 8px;
  width: 100%;
  justify-content: space-between;
  align-items: center;
}

.selectors-row:last-child {
  margin-bottom: 0;
}

.selector-wrapper {
  flex: 1;
  min-width: 0;
}

/* Action Buttons */
.action-buttons {
  display: flex;
  gap: 4px;
  width: 56px;
  justify-content: flex-end;
  align-items: center;
}

.action-button,
.add-join-button,
.remove-join-button {
  cursor: pointer;
  width: 24px;
  height: 24px;
  border-radius: 4px;
  border: 1px solid var(--color-border-primary);
  background-color: var(--color-background-primary);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  transition: all 0.2s ease;
}

.add-join-button,
.add-button {
  color: var(--color-success);
  border-color: var(--color-success);
}

.add-join-button:hover,
.add-button:hover {
  background-color: var(--color-success);
  color: var(--color-text-inverse);
}

.remove-join-button,
.remove-button {
  color: var(--color-danger);
  border-color: var(--color-danger);
}

.remove-join-button:hover,
.remove-button:hover {
  background-color: var(--color-danger);
  color: var(--color-text-inverse);
}

/* Custom scrollbar */
.selectors-container::-webkit-scrollbar {
  width: 8px;
}

.selectors-container::-webkit-scrollbar-track {
  background: transparent;
}

.selectors-container::-webkit-scrollbar-thumb {
  background-color: var(--color-border-primary);
  border-radius: 4px;
}

.selectors-container::-webkit-scrollbar-thumb:hover {
  background-color: var(--color-border-secondary);
}
</style>
