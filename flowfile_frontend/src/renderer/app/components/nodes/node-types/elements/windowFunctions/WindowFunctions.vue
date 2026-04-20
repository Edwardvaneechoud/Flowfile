<template>
  <div v-if="dataLoaded && nodeWindow" class="window-functions">
    <generic-node-settings
      v-model="nodeWindow"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="section-header">
          <span class="listbox-subtitle">Partition by</span>
          <span class="optional-badge">optional</span>
          <el-tooltip
            placement="top"
            content="Resets each calculation per group. Example: pick 'customer_id' so a cumulative sum restarts for each customer. Leave empty to compute over the whole table."
          >
            <span class="help-icon">?</span>
          </el-tooltip>
        </div>
        <el-select
          v-model="windowInput.partition_by"
          multiple
          filterable
          clearable
          collapse-tags
          collapse-tags-tooltip
          placeholder="Select columns to partition by"
          size="small"
          class="full-width"
        >
          <el-option
            v-for="col in availableColumns"
            :key="col.name"
            :label="`${col.name} (${col.data_type})`"
            :value="col.name"
          />
        </el-select>
      </div>

      <div class="listbox-wrapper">
        <div class="section-header">
          <span class="listbox-subtitle">Order by</span>
          <el-tooltip
            placement="top"
            content="Defines the row order inside each partition. Required for rolling and tile functions (how else would we know which rows come first?). For cumulative functions it's optional but strongly recommended."
          >
            <span class="help-icon">?</span>
          </el-tooltip>
        </div>
        <div v-if="windowInput.order_by.length" class="table-wrapper">
          <table class="styled-table window-table">
            <thead>
              <tr>
                <th style="width: 55%">Column</th>
                <th style="width: 30%">Direction</th>
                <th class="action-col"></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(item, index) in windowInput.order_by" :key="`order-${index}`">
                <td>
                  <el-select v-model="item.column" size="small" filterable class="full-width">
                    <el-option
                      v-for="col in availableColumns"
                      :key="col.name"
                      :label="col.name"
                      :value="col.name"
                    />
                  </el-select>
                </td>
                <td>
                  <el-select v-model="item.how" size="small" class="full-width">
                    <el-option label="Ascending" value="asc" />
                    <el-option label="Descending" value="desc" />
                  </el-select>
                </td>
                <td class="action-col">
                  <el-button link size="small" @click="removeOrderBy(index)">Remove</el-button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <el-button size="small" class="add-btn" @click="addOrderBy">
          + Add order-by column
        </el-button>
      </div>

      <div class="listbox-wrapper">
        <div class="section-header">
          <span class="listbox-subtitle">Window functions</span>
          <el-tooltip
            placement="top"
            content="Each row adds one new column to the output. Pick a function (e.g. Rolling mean), a source column, a name for the new column, and any extra params that function needs."
          >
            <span class="help-icon">?</span>
          </el-tooltip>
        </div>
        <div v-if="windowInput.window_functions.length" class="table-wrapper">
          <table class="styled-table window-table">
            <thead>
              <tr>
                <th style="width: 22%">Function</th>
                <th style="width: 22%">Source column</th>
                <th style="width: 28%">Output name</th>
                <th style="width: 20%">
                  <span>Parameter</span>
                  <el-tooltip
                    placement="top"
                    content="Rolling: how many rows to include in the window (e.g. 3 = current row + 2 previous). Tile: how many equal-sized groups to split into. Rank: tie-breaking strategy."
                  >
                    <span class="help-icon">?</span>
                  </el-tooltip>
                </th>
                <th class="action-col"></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(item, index) in windowInput.window_functions" :key="`op-${index}`">
                <td>
                  <el-select
                    v-model="item.function"
                    size="small"
                    class="full-width"
                    @change="onFunctionChange(item, index)"
                  >
                    <el-option-group label="Rolling">
                      <el-option label="Rolling sum" value="rolling_sum" />
                      <el-option label="Rolling mean" value="rolling_mean" />
                      <el-option label="Rolling min" value="rolling_min" />
                      <el-option label="Rolling max" value="rolling_max" />
                      <el-option label="Rolling std" value="rolling_std" />
                    </el-option-group>
                    <el-option-group label="Cumulative">
                      <el-option label="Cumulative sum" value="cum_sum" />
                      <el-option label="Cumulative count" value="cum_count" />
                      <el-option label="Cumulative min" value="cum_min" />
                      <el-option label="Cumulative max" value="cum_max" />
                    </el-option-group>
                    <el-option-group label="Ranking">
                      <el-option label="Rank" value="rank" />
                      <el-option label="Tile (equal groups)" value="tile" />
                    </el-option-group>
                  </el-select>
                </td>
                <td>
                  <el-select
                    v-if="item.function !== 'tile'"
                    v-model="item.column"
                    size="small"
                    filterable
                    placeholder="Select column"
                    class="full-width"
                    @change="onColumnChange(item, index)"
                  >
                    <el-option
                      v-for="col in availableColumns"
                      :key="col.name"
                      :label="col.name"
                      :value="col.name"
                    />
                  </el-select>
                  <span v-else class="muted not-applicable">n/a</span>
                </td>
                <td>
                  <el-input
                    v-model="item.new_column_name"
                    size="small"
                    placeholder="Output column name"
                    class="full-width"
                    @input="onNameEdited(index)"
                  />
                </td>
                <td>
                  <div v-if="isRolling(item.function)" class="param-cell">
                    <el-input-number
                      v-model="item.window_size"
                      :min="1"
                      size="small"
                      controls-position="right"
                      class="param-input"
                    />
                    <span class="param-label">window size (rows)</span>
                  </div>
                  <div v-else-if="item.function === 'tile'" class="param-cell">
                    <el-input-number
                      v-model="item.number_of_groups"
                      :min="2"
                      size="small"
                      controls-position="right"
                      class="param-input"
                    />
                    <span class="param-label">number of groups</span>
                  </div>
                  <div v-else-if="item.function === 'rank'" class="param-cell">
                    <el-select
                      v-model="item.rank_method"
                      size="small"
                      class="param-input"
                    >
                      <el-option label="Ordinal" value="ordinal" />
                      <el-option label="Dense" value="dense" />
                      <el-option label="Min" value="min" />
                      <el-option label="Max" value="max" />
                      <el-option label="Average" value="average" />
                    </el-select>
                    <span class="param-label">tie-breaking method</span>
                  </div>
                  <span v-else class="muted">No parameters</span>
                </td>
                <td class="action-col">
                  <el-button link size="small" @click="removeOp(index)">Remove</el-button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <el-button size="small" class="add-btn" @click="addOp">+ Add window function</el-button>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, watch } from "vue";
import { CodeLoader } from "vue-content-loader";
import {
  NodeWindowFunctions,
  WindowFunctionInput,
  WindowFunctionName,
  WindowFunctionsInput,
} from "../../../baseNode/nodeInput";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeWindow = ref<NodeWindowFunctions | null>(null);
const nodeData = ref<NodeData | null>(null);
const dataLoaded = ref(false);

const defaultWindowInput = (): WindowFunctionsInput => ({
  partition_by: [],
  order_by: [],
  window_functions: [],
});

const windowInput = ref<WindowFunctionsInput>(defaultWindowInput());

// Parallel array: whether each op row's output name is still auto-generated.
// Kept outside the persisted schema so it never reaches the backend.
const autoName = ref<boolean[]>([]);

const ROLLING_FUNCTIONS: ReadonlySet<WindowFunctionName> = new Set([
  "rolling_sum",
  "rolling_mean",
  "rolling_min",
  "rolling_max",
  "rolling_std",
]);

const isRolling = (f: WindowFunctionName | undefined) =>
  !!f && ROLLING_FUNCTIONS.has(f);

const availableColumns = computed(() => nodeData.value?.main_input?.table_schema ?? []);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeWindow,
  onAfterSave: async () => {
    validateNode();
  },
  getValidationFunc: () => (nodeWindow.value?.window_input ? validateNode : undefined),
});

const autoNameFor = (item: WindowFunctionInput): string => {
  if (item.function === "tile") return `tile_${item.number_of_groups ?? "n"}`;
  const base = item.column || "col";
  return `${base}_${item.function}`;
};

const applyAutoName = (index: number) => {
  const item = windowInput.value.window_functions[index];
  if (!item) return;
  if (autoName.value[index]) {
    item.new_column_name = autoNameFor(item);
  }
};

const addOrderBy = () => {
  const firstCol = availableColumns.value[0]?.name ?? "";
  windowInput.value.order_by.push({ column: firstCol, how: "asc" });
};

const removeOrderBy = (index: number) => {
  windowInput.value.order_by.splice(index, 1);
};

const addOp = () => {
  const firstCol = availableColumns.value[0]?.name ?? "";
  const item: WindowFunctionInput = {
    column: firstCol,
    function: "rolling_mean",
    new_column_name: "",
    window_size: 3,
    min_periods: null,
    number_of_groups: null,
    rank_method: "ordinal",
  };
  item.new_column_name = autoNameFor(item);
  windowInput.value.window_functions.push(item);
  autoName.value.push(true);
};

const removeOp = (index: number) => {
  windowInput.value.window_functions.splice(index, 1);
  autoName.value.splice(index, 1);
};

const onFunctionChange = (item: WindowFunctionInput, index: number) => {
  if (isRolling(item.function) && (!item.window_size || item.window_size < 1)) {
    item.window_size = 3;
  }
  if (item.function === "tile" && (!item.number_of_groups || item.number_of_groups < 2)) {
    item.number_of_groups = 4;
  }
  if (item.function === "rank" && !item.rank_method) {
    item.rank_method = "ordinal";
  }
  applyAutoName(index);
};

const onColumnChange = (_item: WindowFunctionInput, index: number) => {
  applyAutoName(index);
};

const onNameEdited = (index: number) => {
  const item = windowInput.value.window_functions[index];
  if (!item) return;
  // If the user cleared the field or typed something different from the auto
  // name, treat the name as user-controlled from now on. Re-typing the exact
  // auto-generated string re-enables auto-update — matching expectations.
  autoName.value[index] = item.new_column_name === "" || item.new_column_name === autoNameFor(item);
};

const validateNode = () => {
  if (!nodeWindow.value) return;
  const nodeId = nodeWindow.value.node_id;
  const input = windowInput.value;
  if (input.window_functions.length === 0) {
    nodeStore.setNodeValidation(nodeId, {
      isValid: false,
      error: "Add at least one window function.",
    });
    return;
  }
  const needsOrder = input.window_functions.some(
    (w) => isRolling(w.function) || w.function === "tile",
  );
  if (needsOrder && input.order_by.length === 0) {
    nodeStore.setNodeValidation(nodeId, {
      isValid: false,
      error: "Rolling and tile functions require an order-by column.",
    });
    return;
  }
  const names = new Set<string>();
  for (const w of input.window_functions) {
    if (!w.new_column_name) {
      nodeStore.setNodeValidation(nodeId, {
        isValid: false,
        error: "Every window function needs an output column name.",
      });
      return;
    }
    if (names.has(w.new_column_name)) {
      nodeStore.setNodeValidation(nodeId, {
        isValid: false,
        error: `Duplicate output name: ${w.new_column_name}`,
      });
      return;
    }
    names.add(w.new_column_name);
    if (isRolling(w.function) && (!w.window_size || w.window_size < 1)) {
      nodeStore.setNodeValidation(nodeId, {
        isValid: false,
        error: `Rolling functions need a window size (${w.new_column_name}).`,
      });
      return;
    }
    if (w.function === "tile" && (!w.number_of_groups || w.number_of_groups < 2)) {
      nodeStore.setNodeValidation(nodeId, {
        isValid: false,
        error: `Tile needs at least 2 groups (${w.new_column_name}).`,
      });
      return;
    }
  }
  nodeStore.setNodeValidation(nodeId, { isValid: true, error: "" });
};

watch(windowInput, validateNode, { deep: true });

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeWindow.value = nodeData.value?.setting_input;
  if (nodeWindow.value) {
    if (!nodeWindow.value.window_input) {
      nodeWindow.value.window_input = defaultWindowInput();
    }
    windowInput.value = nodeWindow.value.window_input;
    // Loading an existing node: assume the user's saved names are intentional,
    // so disable auto-update for every row.
    autoName.value = windowInput.value.window_functions.map(() => false);
  }
  dataLoaded.value = true;
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.full-width {
  width: 100%;
}
.section-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-1, 6px);
  margin-bottom: 4px;
}
.optional-badge {
  font-size: 10px;
  color: var(--color-text-secondary, #888);
  background: var(--color-background-soft, #f2f2f2);
  padding: 1px 6px;
  border-radius: 8px;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.help-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: var(--color-background-soft, #f0f0f0);
  color: var(--color-text-secondary, #777);
  font-size: 11px;
  font-weight: 600;
  cursor: help;
}
.table-wrapper {
  max-height: 360px;
  box-shadow: var(--shadow-sm);
  border-radius: var(--border-radius-lg);
  overflow: auto;
  margin: var(--spacing-1) 0;
}
.window-table {
  width: 100%;
  table-layout: fixed;
}
.window-table th,
.window-table td {
  padding: 6px 8px;
  vertical-align: middle;
}
.window-table td {
  overflow: visible;
}
.action-col {
  width: 72px;
  text-align: right;
}
.param-cell {
  display: flex;
  flex-direction: column;
  gap: 2px;
}
.param-input {
  width: 100%;
}
.param-label {
  font-size: 10px;
  color: var(--color-text-secondary, #888);
  line-height: 1.1;
}
.add-btn {
  margin: var(--spacing-1) 0 var(--spacing-2);
}
.muted {
  color: var(--color-text-secondary, #999);
  font-size: 12px;
}
.not-applicable {
  font-style: italic;
}
</style>
