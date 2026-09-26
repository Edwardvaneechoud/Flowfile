<template>
  <div v-if="dataLoaded && nodeExplodeHierarchy" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeExplodeHierarchy"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Hierarchy</div>
        <div class="field">
          <label class="field-label" for="explode-hierarchy-parent">Parent column</label>
          <el-select
            id="explode-hierarchy-parent"
            v-model="settings.parent_column"
            filterable
            placeholder="Select a column"
            size="small"
            class="full-width"
          >
            <el-option
              v-for="col in incomingColumns"
              :key="col.name"
              :label="col.name"
              :value="col.name"
            >
              <span>{{ col.name }}</span>
              <span class="column-type">{{ col.data_type }}</span>
            </el-option>
          </el-select>
          <div class="hint">The assembly or account that contains the child.</div>
        </div>

        <div class="field">
          <label class="field-label" for="explode-hierarchy-child">Child column</label>
          <el-select
            id="explode-hierarchy-child"
            v-model="settings.child_column"
            filterable
            placeholder="Select a column"
            size="small"
            class="full-width"
          >
            <el-option
              v-for="col in incomingColumns"
              :key="col.name"
              :label="col.name"
              :value="col.name"
            >
              <span>{{ col.name }}</span>
              <span class="column-type">{{ col.data_type }}</span>
            </el-option>
          </el-select>
          <div class="hint">The component or sub-account it contains.</div>
          <div v-if="sameParentAndChild" class="field-warning">
            Parent and child must be different columns.
          </div>
        </div>

        <div class="field">
          <label class="field-label" for="explode-hierarchy-quantity">
            Quantity column <span class="field-optional">optional</span>
          </label>
          <el-select
            id="explode-hierarchy-quantity"
            v-model="settings.quantity_column"
            filterable
            clearable
            placeholder="Each edge counts 1"
            size="small"
            class="full-width"
          >
            <el-option
              v-for="col in incomingColumns"
              :key="col.name"
              :label="col.name"
              :value="col.name"
            >
              <span>{{ col.name }}</span>
              <span class="column-type">{{ col.data_type }}</span>
            </el-option>
          </el-select>
          <div class="hint">
            How many of the child one parent holds. Quantities multiply down a route and add up
            across routes.
          </div>
        </div>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Output</div>
        <div class="field">
          <el-radio-group v-model="settings.output_detail" size="small">
            <el-radio-button
              v-for="option in HIERARCHY_OUTPUT_OPTIONS"
              :key="option.value"
              :value="option.value"
            >
              {{ option.label }}
            </el-radio-button>
          </el-radio-group>
          <div class="hint output-description">{{ selectedOutput.description }}</div>
          <ul class="output-columns" aria-label="Output columns">
            <li v-for="col in outputColumns" :key="col.name" class="output-column">
              <code>{{ col.name }}</code>
              <span class="output-column-description">{{ col.description }}</span>
            </li>
          </ul>
        </div>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Options</div>
        <div class="field">
          <div class="switch-row">
            <span class="field-label">Top-level items only</span>
            <el-switch v-model="settings.top_level_only" size="small" />
          </div>
          <div class="hint">Only explode items that are never a component themselves.</div>
        </div>

        <div class="field">
          <div class="switch-row">
            <span class="field-label">Include each item itself</span>
            <el-switch v-model="settings.include_self" size="small" />
          </div>
          <div class="hint">
            Adds a level-0 row from each exploded item to itself, with quantity 1: every item, or
            only the top-level ones when Top-level items only is on.
          </div>
        </div>

        <div class="field">
          <label class="field-label" for="explode-hierarchy-max-depth">
            Max depth <span class="field-optional">optional</span>
          </label>
          <el-input-number
            id="explode-hierarchy-max-depth"
            v-model="settings.max_depth"
            :min="0"
            :max="MAX_HIERARCHY_DEPTH"
            :precision="0"
            :step="1"
            size="small"
            controls-position="right"
            placeholder="Unlimited"
          />
          <div class="hint">Stop after this many levels. Leave empty to explode every level.</div>
        </div>
      </div>

      <div class="info-banner">
        The result is a new table: other input columns are not carried through. Join them back on
        <code>ancestor</code> or <code>descendant</code> if you need them.
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import type {
  ExplodeHierarchyInput,
  NodeData,
  NodeExplodeHierarchy,
} from "../../../../../types/node.types";
import {
  HIERARCHY_OUTPUT_OPTIONS,
  MAX_HIERARCHY_DEPTH,
  createExplodeHierarchyInput,
  createExplodeHierarchyNode,
  hierarchyOutputColumns,
  hierarchyOutputOption,
  normalizeExplodeHierarchyInput,
} from "./explodeHierarchy";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeExplodeHierarchy = ref<NodeExplodeHierarchy | null>(null);
const nodeData = ref<NodeData | null>(null);

const settings = computed<ExplodeHierarchyInput>(
  () => nodeExplodeHierarchy.value?.explode_hierarchy_input ?? createExplodeHierarchyInput(),
);

const incomingColumns = computed(() => {
  const mainInput = nodeData.value?.main_input;
  if (mainInput?.table_schema?.length) return mainInput.table_schema;
  return (mainInput?.columns ?? []).map((name) => ({ name, data_type: "" }));
});

const sameParentAndChild = computed(
  () =>
    !!settings.value.parent_column && settings.value.parent_column === settings.value.child_column,
);

const selectedOutput = computed(() => hierarchyOutputOption(settings.value.output_detail));
const outputColumns = computed(() => hierarchyOutputColumns(settings.value.output_detail));

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeExplodeHierarchy,
  onBeforeSave: () => {
    if (!nodeExplodeHierarchy.value) return false;
    nodeExplodeHierarchy.value.explode_hierarchy_input = normalizeExplodeHierarchyInput(
      nodeExplodeHierarchy.value.explode_hierarchy_input,
    );
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  if (nodeData.value?.setting_input?.is_setup) {
    nodeExplodeHierarchy.value = nodeData.value.setting_input as NodeExplodeHierarchy;
    nodeExplodeHierarchy.value.explode_hierarchy_input = normalizeExplodeHierarchyInput(
      nodeExplodeHierarchy.value.explode_hierarchy_input,
    );
  } else {
    nodeExplodeHierarchy.value = createExplodeHierarchyNode(
      Number(nodeStore.flow_id),
      Number(nodeStore.node_id ?? nodeId),
    );
    nodeExplodeHierarchy.value.depending_on_id = nodeData.value?.main_input?.node_id;
  }
  dataLoaded.value = true;
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.field {
  padding: var(--spacing-2) var(--spacing-2) 0;
}

.field:last-child {
  padding-bottom: var(--spacing-2);
}

.field-label {
  display: block;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-1);
}

.field-optional {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  margin-left: 4px;
}

.full-width {
  width: 100%;
}

.hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.4;
  margin-top: 4px;
}

.field-warning {
  margin-top: 6px;
  color: var(--color-danger);
  font-size: var(--font-size-xs);
  padding: 4px 8px;
  background: var(--color-danger-light);
  border-radius: 3px;
}

.column-type {
  font-size: 0.75rem;
  color: var(--color-text-tertiary);
  margin-left: 8px;
}

.switch-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
}

.switch-row .field-label {
  margin-bottom: 0;
}

.output-description {
  margin-top: var(--spacing-2);
}

.output-columns {
  list-style: none;
  margin: var(--spacing-2) 0 0;
  padding: 4px 0;
  border: 1px solid var(--color-border-light);
  border-radius: 3px;
}

.output-column {
  display: grid;
  grid-template-columns: 96px 1fr;
  align-items: baseline;
  gap: 8px;
  padding: 2px 8px;
  font-size: var(--font-size-xs);
}

.output-column-description {
  color: var(--color-text-secondary);
}

.info-banner {
  margin: var(--spacing-3) var(--spacing-1) 0;
  padding: var(--spacing-2) var(--spacing-3);
  background-color: var(--color-info-light);
  border-left: 3px solid var(--color-info, #1890ff);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

code {
  background: var(--color-background-secondary, #f5f5f5);
  padding: 0 4px;
  border-radius: 3px;
  font-size: 0.9em;
}
</style>
