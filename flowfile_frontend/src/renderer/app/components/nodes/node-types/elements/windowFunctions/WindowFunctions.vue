<template>
  <div v-if="dataLoaded && nodeWindow" class="window-functions">
    <generic-node-settings
      v-model="nodeWindow"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Partition by (optional)</div>
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
        <div class="listbox-subtitle">Order by</div>
        <div class="table-wrapper">
          <table class="styled-table">
            <thead>
              <tr>
                <th>Column</th>
                <th>Direction</th>
                <th class="action-col"></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(item, index) in windowInput.order_by" :key="`order-${index}`">
                <td>
                  <el-select v-model="item.column" size="small" filterable>
                    <el-option
                      v-for="col in availableColumns"
                      :key="col.name"
                      :label="col.name"
                      :value="col.name"
                    />
                  </el-select>
                </td>
                <td>
                  <el-select v-model="item.how" size="small">
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
        <el-button size="small" class="add-btn" @click="addOrderBy">+ Add order-by column</el-button>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Window functions</div>
        <div class="table-wrapper">
          <table class="styled-table">
            <thead>
              <tr>
                <th>Function</th>
                <th>Column</th>
                <th>Output name</th>
                <th>Params</th>
                <th class="action-col"></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(item, index) in windowInput.window_functions" :key="`op-${index}`">
                <td>
                  <el-select
                    v-model="item.function"
                    size="small"
                    @change="onFunctionChange(item)"
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
                  >
                    <el-option
                      v-for="col in availableColumns"
                      :key="col.name"
                      :label="col.name"
                      :value="col.name"
                    />
                  </el-select>
                  <span v-else class="muted">—</span>
                </td>
                <td>
                  <el-input v-model="item.new_column_name" size="small" />
                </td>
                <td>
                  <el-input-number
                    v-if="isRolling(item.function)"
                    v-model="item.window_size"
                    :min="1"
                    size="small"
                    placeholder="Window"
                    controls-position="right"
                    class="param-input"
                  />
                  <el-input-number
                    v-else-if="item.function === 'tile'"
                    v-model="item.number_of_groups"
                    :min="1"
                    size="small"
                    placeholder="Groups"
                    controls-position="right"
                    class="param-input"
                  />
                  <el-select
                    v-else-if="item.function === 'rank'"
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
                  <span v-else class="muted">—</span>
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

const addOrderBy = () => {
  const firstCol = availableColumns.value[0]?.name ?? "";
  windowInput.value.order_by.push({ column: firstCol, how: "asc" });
};

const removeOrderBy = (index: number) => {
  windowInput.value.order_by.splice(index, 1);
};

const addOp = () => {
  const firstCol = availableColumns.value[0]?.name ?? "";
  windowInput.value.window_functions.push({
    column: firstCol,
    function: "rolling_mean",
    new_column_name: firstCol ? `${firstCol}_rolling_mean` : "new_col",
    window_size: 3,
    min_periods: null,
    number_of_groups: null,
    rank_method: "ordinal",
  });
};

const removeOp = (index: number) => {
  windowInput.value.window_functions.splice(index, 1);
};

const onFunctionChange = (item: WindowFunctionInput) => {
  if (isRolling(item.function)) {
    if (!item.window_size || item.window_size < 1) item.window_size = 3;
  }
  if (item.function === "tile") {
    if (!item.number_of_groups || item.number_of_groups < 1) item.number_of_groups = 4;
  }
  if (item.function === "rank" && !item.rank_method) {
    item.rank_method = "ordinal";
  }
  const base = item.column ?? "tile";
  if (!item.new_column_name || item.new_column_name.startsWith(`${base}_`)) {
    item.new_column_name = `${base}_${item.function}`;
  }
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
    if (w.function === "tile" && (!w.number_of_groups || w.number_of_groups < 1)) {
      nodeStore.setNodeValidation(nodeId, {
        isValid: false,
        error: `Tile needs a number of groups (${w.new_column_name}).`,
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
.table-wrapper {
  max-height: 300px;
  box-shadow: var(--shadow-sm);
  border-radius: var(--border-radius-lg);
  overflow: auto;
  margin: var(--spacing-1);
}
.styled-table th,
.styled-table td {
  padding: 4px 6px;
  vertical-align: middle;
}
.action-col {
  width: 72px;
  text-align: right;
}
.param-input {
  width: 120px;
}
.add-btn {
  margin: var(--spacing-1) 0 var(--spacing-2);
}
.muted {
  color: var(--color-text-secondary, #999);
}
</style>
