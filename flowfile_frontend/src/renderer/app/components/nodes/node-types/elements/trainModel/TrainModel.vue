<template>
  <div v-if="dataLoaded && nodeTrainModel" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeTrainModel"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Algorithm</div>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Algorithm</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeTrainModel.train_input.model_type"
              placeholder="Choose an algorithm"
              @change="onAlgorithmChange"
            >
              <el-option
                v-for="alg in algorithms"
                :key="alg.model_type"
                :label="alg.label"
                :value="alg.model_type"
              />
            </el-select>
          </el-col>
        </el-row>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Catalog</div>
        <p class="hint">
          The trained model is always available to downstream Apply Model nodes
          in this flow. Toggle this on to also publish it to the catalog so it
          can be reused across flows and runs.
        </p>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Save to catalog</el-col>
          <el-col :span="14" class="grid-content">
            <el-switch v-model="nodeTrainModel.train_input.publish_to_catalog" />
          </el-col>
        </el-row>

        <template v-if="nodeTrainModel.train_input.publish_to_catalog">
          <el-row class="setting-row">
            <el-col :span="10" class="grid-content">Model name</el-col>
            <el-col :span="14" class="grid-content">
              <input
                v-model="nodeTrainModel.train_input.model_name"
                type="text"
                placeholder="e.g. house_price_v1"
              />
            </el-col>
          </el-row>

          <el-row class="setting-row">
            <el-col :span="10" class="grid-content">Catalog namespace</el-col>
            <el-col :span="14" class="grid-content">
              <el-select
                v-model="nodeTrainModel.train_input.namespace_id"
                placeholder="Default (flow's namespace)"
                clearable
                filterable
                :loading="loadingNamespaces"
              >
                <el-option label="Default (flow's namespace)" :value="null" />
                <el-option
                  v-for="ns in namespaceOptions"
                  :key="ns.id"
                  :label="ns.label"
                  :value="ns.id"
                />
              </el-select>
            </el-col>
          </el-row>
        </template>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Target & Features</div>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Target column</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeTrainModel.train_input.target_column"
              placeholder="Choose target"
            >
              <el-option
                v-for="c in availableColumns"
                :key="c.name"
                :label="`${c.name} (${c.data_type})`"
                :value="c.name"
              />
            </el-select>
          </el-col>
        </el-row>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Feature columns</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeTrainModel.train_input.feature_columns"
              multiple
              filterable
              placeholder="Choose feature columns"
            >
              <el-option
                v-for="c in featureColumnOptions"
                :key="c.name"
                :label="`${c.name} (${c.data_type})`"
                :value="c.name"
              />
            </el-select>
          </el-col>
        </el-row>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Hyperparameters</div>
        <DynamicParams
          v-if="selectedSpec"
          v-model="nodeTrainModel.train_input.params"
          :spec="selectedSpec.params"
        />
        <p v-else class="hint">Pick an algorithm to see its hyperparameters.</p>
      </div>

      <div v-if="!flowRegistered" class="warning-banner">
        Train Model writes to the catalog, so the flow must be registered first.
        Save and register the flow before running it.
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted } from "vue";
import axios from "axios";
import type {
  NodeTrainModel,
  MLAlgorithmSpec,
  CatalogNamespaceTree,
  NamespaceOption,
} from "../../../../../types/node.types";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import DynamicParams from "./DynamicParams.vue";

const nodeStore = useNodeStore();
const nodeTrainModel = ref<NodeTrainModel | null>(null);
const dataLoaded = ref(false);
const nodeData = ref<NodeData | null>(null);
const algorithms = ref<MLAlgorithmSpec[]>([]);
const namespaceTree = ref<CatalogNamespaceTree[]>([]);
const loadingNamespaces = ref(false);

// Walk the catalog tree and surface every selectable schema as
// "Catalog > Schema". We skip the root catalog nodes themselves because
// artifacts live in schemas, not in the catalog containers.
function flattenNamespaceTree(
  tree: CatalogNamespaceTree[],
  parentLabel: string | null,
): NamespaceOption[] {
  const out: NamespaceOption[] = [];
  for (const node of tree) {
    const label = parentLabel ? `${parentLabel} > ${node.name}` : node.name;
    if (node.level >= 1) {
      out.push({ id: node.id, label });
    }
    if (node.children && node.children.length) {
      out.push(...flattenNamespaceTree(node.children, label));
    }
  }
  return out;
}

const namespaceOptions = computed(() =>
  flattenNamespaceTree(namespaceTree.value, null).sort((a, b) =>
    a.label.localeCompare(b.label),
  ),
);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeTrainModel,
});

const availableColumns = computed(() => nodeData.value?.main_input?.table_schema ?? []);

// Don't let users feed the target column back in as a feature.
const featureColumnOptions = computed(() => {
  const target = nodeTrainModel.value?.train_input.target_column;
  return availableColumns.value.filter((c) => c.name !== target);
});

const selectedSpec = computed(() => {
  const t = nodeTrainModel.value?.train_input.model_type;
  return algorithms.value.find((a) => a.model_type === t) ?? null;
});

// Surface the catalog-registration precondition prominently.
const flowRegistered = computed(() => {
  const settings = (nodeStore as { flowSettings?: { source_registration_id?: number | null } })
    .flowSettings;
  return settings?.source_registration_id != null;
});

function defaultsForSpec(spec: MLAlgorithmSpec): Record<string, unknown> {
  return Object.fromEntries(spec.params.map((p) => [p.name, p.default]));
}

function onAlgorithmChange(modelType: string) {
  const spec = algorithms.value.find((a) => a.model_type === modelType);
  if (spec && nodeTrainModel.value) {
    // Reset hyperparameters when the user switches algorithms so we never carry
    // a now-invalid hyperparam into the new spec.
    nodeTrainModel.value.train_input.params = defaultsForSpec(spec);
  }
}

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeTrainModel.value = nodeData.value?.setting_input as NodeTrainModel;

  if (algorithms.value.length === 0) {
    try {
      const resp = await axios.get<MLAlgorithmSpec[]>("/ml/algorithms");
      algorithms.value = resp.data;
    } catch (e) {
      console.error("Failed to load /ml/algorithms", e);
    }
  }

  if (namespaceTree.value.length === 0) {
    loadingNamespaces.value = true;
    try {
      const resp = await axios.get<CatalogNamespaceTree[]>("/catalog/namespaces/tree");
      namespaceTree.value = resp.data;
    } catch (e) {
      console.error("Failed to load /catalog/namespaces/tree", e);
    } finally {
      loadingNamespaces.value = false;
    }
  }

  if (nodeTrainModel.value) {
    if (!nodeTrainModel.value.is_setup || !nodeTrainModel.value.train_input) {
      const firstAlg = algorithms.value[0];
      nodeTrainModel.value.train_input = {
        target_column: "",
        feature_columns: [],
        model_type: firstAlg?.model_type ?? "linear_regression",
        params: firstAlg ? defaultsForSpec(firstAlg) : {},
        publish_to_catalog: false,
        model_name: "",
        namespace_id: null,
        catalog_description: null,
        catalog_tags: [],
      };
    } else if (nodeTrainModel.value.train_input.params == null) {
      nodeTrainModel.value.train_input.params = {};
    }
    dataLoaded.value = true;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });

onMounted(() => {});
</script>

<style scoped>
.setting-row {
  margin-bottom: var(--spacing-2);
}

.grid-content {
  font-size: var(--font-size-sm);
  align-items: center;
}

.hint {
  color: var(--color-text-secondary);
  font-style: italic;
  font-size: var(--font-size-sm);
}

.warning-banner {
  margin-top: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  background-color: var(--color-warning-background, #fff7e6);
  border-left: 3px solid var(--color-warning, #faad14);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

input[type="text"] {
  width: 100%;
  padding: 6px 8px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-sm);
}
</style>
