<template>
  <div v-if="dataLoaded && nodePredict" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodePredict"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Artefact</div>
        <el-select
          v-model="nodePredict.data_science_predict_input.artefact_name"
          filterable
          size="small"
          style="width: 100%"
          :loading="loadingArtefacts"
          placeholder="Pick a published artefact"
          @change="onArtefactNameChange"
        >
          <el-option
            v-for="name in artefactNames"
            :key="name"
            :label="name"
            :value="name"
          />
        </el-select>

        <div class="listbox-subtitle" style="margin-top: 12px">Version</div>
        <el-select
          v-model="versionSelection"
          size="small"
          style="width: 100%"
          :loading="loadingVersions"
          :disabled="!nodePredict.data_science_predict_input.artefact_name"
          @change="onVersionChange"
        >
          <el-option label="Latest" :value="LATEST" />
          <el-option
            v-for="v in artefactVersions"
            :key="v.version"
            :label="`v${v.version}`"
            :value="v.version"
          />
        </el-select>

        <div v-if="artefactOutputSchema" class="schema-hint">
          Will append:
          <span
            v-for="col in artefactOutputSchema"
            :key="col.name"
            class="schema-pill"
          >
            {{ col.name }}
            <span class="schema-pill-type">{{ col.data_type }}</span>
          </span>
        </div>

        <div class="listbox-subtitle" style="margin-top: 12px">Feature columns</div>
        <ColumnListSelector
          v-model="nodePredict.data_science_predict_input.feature_cols"
          :schema="tableSchema"
          placeholder="Select the same features used at fit time"
        />

        <div class="listbox-subtitle" style="margin-top: 12px">Prediction column</div>
        <el-input
          v-model="nodePredict.data_science_predict_input.prediction_col"
          size="small"
          placeholder="prediction"
        />
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { computed, onMounted, ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { ElMessage } from "element-plus";
import type { NodeDataSciencePredict } from "@/types/node.types";
import type { NodeData } from "@/components/nodes/baseNode/nodeInterfaces";
import { useNodeStore } from "@/stores/node-store";
import { useNodeSettings } from "@/composables/useNodeSettings";
import GenericNodeSettings from "@/components/nodes/baseNode/genericNodeSettings.vue";
import ColumnListSelector from "@/components/nodes/baseNode/page_objects/columnListSelector.vue";
import {
  ArtifactsApi,
  type ArtifactVersionInfo,
  type ArtifactWithVersions,
} from "@/api/artifacts.api";
import { createDataSciencePredictNode } from "./utils";

const LATEST = "__latest__";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodePredict = ref<NodeDataSciencePredict | null>(null);
const nodeData = ref<NodeData | null>(null);

const artefactNames = ref<string[]>([]);
const artefactVersions = ref<ArtifactVersionInfo[]>([]);
const artefactOutputSchema = ref<ArtifactWithVersions["output_schema"]>(null);
const loadingArtefacts = ref(false);
const loadingVersions = ref(false);
const versionSelection = ref<number | typeof LATEST>(LATEST);

const tableSchema = computed(() => nodeData.value?.main_input?.table_schema ?? []);

const loadArtefactList = async () => {
  loadingArtefacts.value = true;
  try {
    const items = await ArtifactsApi.list();
    // The list endpoint returns one row per (name, version); the picker wants
    // unique names.
    const seen = new Set<string>();
    const names: string[] = [];
    for (const item of items) {
      if (item.status !== "active") continue;
      if (seen.has(item.name)) continue;
      seen.add(item.name);
      names.push(item.name);
    }
    artefactNames.value = names.sort();
  } catch (e) {
    ElMessage.error(`Failed to load artefacts: ${String(e)}`);
  } finally {
    loadingArtefacts.value = false;
  }
};

const loadArtefactVersions = async (name: string) => {
  if (!name) {
    artefactVersions.value = [];
    artefactOutputSchema.value = null;
    return;
  }
  loadingVersions.value = true;
  try {
    const withVersions = await ArtifactsApi.getByNameWithVersions(name);
    artefactVersions.value = [...withVersions.all_versions].sort(
      (a, b) => b.version - a.version,
    );
    artefactOutputSchema.value = withVersions.output_schema ?? null;
  } catch (e) {
    ElMessage.error(`Failed to load versions for '${name}': ${String(e)}`);
    artefactVersions.value = [];
    artefactOutputSchema.value = null;
  } finally {
    loadingVersions.value = false;
  }
};

const onArtefactNameChange = async (name: string) => {
  if (!nodePredict.value) return;
  nodePredict.value.data_science_predict_input.artefact_version = null;
  versionSelection.value = LATEST;
  await loadArtefactVersions(name);
};

const onVersionChange = (value: number | typeof LATEST) => {
  if (!nodePredict.value) return;
  nodePredict.value.data_science_predict_input.artefact_version =
    value === LATEST ? null : value;
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodePredict,
  onBeforeSave: () => {
    if (!nodePredict.value) return false;
    if (!nodePredict.value.data_science_predict_input.artefact_name) {
      ElMessage.error("Pick an artefact");
      return false;
    }
    if (nodePredict.value.data_science_predict_input.feature_cols.length === 0) {
      ElMessage.error("At least one feature column is required");
      return false;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  const setting = nodeData.value?.setting_input as NodeDataSciencePredict | undefined;
  const hasValidSetup = Boolean(setting?.is_setup && setting?.data_science_predict_input);
  nodePredict.value = hasValidSetup
    ? (setting as NodeDataSciencePredict)
    : createDataSciencePredictNode(nodeStore.flow_id, nodeStore.node_id);

  const currentVersion = nodePredict.value.data_science_predict_input.artefact_version;
  versionSelection.value = currentVersion === null ? LATEST : currentVersion;

  dataLoaded.value = true;

  await loadArtefactList();
  const existingName = nodePredict.value.data_science_predict_input.artefact_name;
  if (existingName) {
    await loadArtefactVersions(existingName);
  }
};

onMounted(() => {
  // Re-entrant: loadNodeData drives the initial fetch; this keeps the
  // picker warm if the component is remounted without re-calling loadNodeData.
  if (artefactNames.value.length === 0 && !loadingArtefacts.value) {
    loadArtefactList();
  }
});

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.schema-hint {
  margin-top: 6px;
  font-size: 12px;
  color: var(--color-text-secondary, #6b7280);
}
.schema-pill {
  display: inline-flex;
  align-items: baseline;
  margin-right: 6px;
  padding: 2px 6px;
  border-radius: 4px;
  background: var(--color-bg-subtle, #f3f4f6);
  font-family: inherit;
}
.schema-pill-type {
  margin-left: 6px;
  font-size: 10px;
  color: var(--color-text-tertiary, #9ca3af);
}
</style>
