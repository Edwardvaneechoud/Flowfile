<template>
  <div v-if="dataLoaded && nodeApplyModel" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeApplyModel"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Model source</div>
        <el-switch
          v-model="nodeApplyModel.apply_input.source"
          active-value="catalog"
          inactive-value="upstream"
          active-text="From catalog"
          inactive-text="From upstream node"
        />
      </div>

      <!-- Upstream source -->
      <div v-if="nodeApplyModel.apply_input.source === 'upstream'" class="listbox-wrapper">
        <p class="hint">
          Pick a Train Model node from this flow's upstream chain. The trained model is read
          directly from the flow cache — no catalog lookup, and you can wire it up at design time.
        </p>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Train Model node</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeApplyModel.apply_input.upstream_node_id"
              placeholder="Select an upstream Train Model"
              :loading="loadingUpstream"
              filterable
            >
              <el-option
                v-for="opt in upstreamOptions"
                :key="opt.node_id"
                :label="formatUpstreamLabel(opt)"
                :value="opt.node_id"
              />
            </el-select>
          </el-col>
        </el-row>

        <div v-if="upstreamOptions.length === 0 && !loadingUpstream" class="info-banner">
          No Train Model nodes found upstream. Add one and connect it (use a Wait For node if you
          also need to enforce ordering on a parallel branch).
        </div>
      </div>

      <!-- Catalog source -->
      <div v-else class="listbox-wrapper">
        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Catalog namespace</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="nodeApplyModel.apply_input.namespace_id"
              placeholder="Any namespace"
              clearable
              filterable
              :loading="loadingNamespaces"
              @change="onNamespaceChange"
            >
              <el-option
                v-for="ns in namespaceOptions"
                :key="ns.id"
                :label="ns.label"
                :value="ns.id"
              />
            </el-select>
          </el-col>
        </el-row>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Model</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="selectedModelKey"
              filterable
              placeholder="Select a trained model"
              :loading="loadingModels"
              @change="onModelSelected"
            >
              <el-option
                v-for="m in availableModels"
                :key="modelKey(m)"
                :label="modelLabel(m)"
                :value="modelKey(m)"
              />
            </el-select>
          </el-col>
        </el-row>

        <el-row v-if="versionOptions.length > 1" class="setting-row">
          <el-col :span="10" class="grid-content">Version</el-col>
          <el-col :span="14" class="grid-content">
            <el-select
              v-model="versionSelection"
              placeholder="Latest"
              clearable
              @change="onVersionSelected"
            >
              <el-option v-for="v in versionOptions" :key="v" :label="`v${v}`" :value="v" />
            </el-select>
          </el-col>
        </el-row>

        <div v-if="availableModels.length === 0 && !loadingModels" class="info-banner">
          No trained ML models found in the catalog yet. Train one with "Save to catalog" enabled to
          register it.
        </div>
      </div>

      <div class="listbox-wrapper">
        <div class="listbox-subtitle">Output</div>

        <el-row class="setting-row">
          <el-col :span="10" class="grid-content">Output column</el-col>
          <el-col :span="14" class="grid-content">
            <input
              v-model="nodeApplyModel.apply_input.output_column"
              type="text"
              placeholder="prediction"
            />
          </el-col>
        </el-row>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, watch } from "vue";
import axios from "axios";
import type {
  NodeApplyModel,
  MLArtifactListItem,
  CatalogNamespaceTree,
  NamespaceOption,
  UpstreamTrainModelOption,
} from "../../../../../types/node.types";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const nodeApplyModel = ref<NodeApplyModel | null>(null);
const dataLoaded = ref(false);
const nodeData = ref<NodeData | null>(null);
const loadingModels = ref(false);
const loadingNamespaces = ref(false);
const loadingUpstream = ref(false);
const allArtifacts = ref<MLArtifactListItem[]>([]);
const versionSelection = ref<number | null>(null);
const namespaceTree = ref<CatalogNamespaceTree[]>([]);
const upstreamOptions = ref<UpstreamTrainModelOption[]>([]);

// Synthetic key for the model picker so we can pin both name AND namespace
// when the user makes a choice. Two artifacts with the same name in different
// schemas otherwise collapse to one entry and leak each other's versions.
const selectedModelKey = ref<string>("");

function modelKey(m: { name: string; namespace_id?: number | null }): string {
  return `${m.name}::${m.namespace_id ?? ""}`;
}

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeApplyModel,
});

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
  flattenNamespaceTree(namespaceTree.value, null).sort((a, b) => a.label.localeCompare(b.label)),
);

const filteredArtifacts = computed(() => {
  const ns = nodeApplyModel.value?.apply_input.namespace_id ?? null;
  if (ns == null) return allArtifacts.value;
  return allArtifacts.value.filter((a) => a.namespace_id === ns);
});

const availableModels = computed(() => {
  // One entry per (name, namespace) — keep the highest version per pair.
  const byKey = new Map<string, MLArtifactListItem>();
  for (const a of filteredArtifacts.value) {
    const key = modelKey(a);
    const existing = byKey.get(key);
    if (!existing || a.version > existing.version) {
      byKey.set(key, a);
    }
  }
  return Array.from(byKey.values()).sort((a, b) => a.name.localeCompare(b.name));
});

// Names that appear in more than one namespace in the current view need
// disambiguating labels in the picker.
const ambiguousNames = computed(() => {
  const counts = new Map<string, number>();
  for (const m of availableModels.value) {
    counts.set(m.name, (counts.get(m.name) ?? 0) + 1);
  }
  return new Set(
    Array.from(counts.entries())
      .filter(([, c]) => c > 1)
      .map(([n]) => n),
  );
});

function namespaceLabelFor(id: number | null | undefined): string {
  if (id == null) return "default";
  return namespaceOptions.value.find((n) => n.id === id)?.label ?? `ns ${id}`;
}

function modelLabel(m: MLArtifactListItem): string {
  return ambiguousNames.value.has(m.name)
    ? `${m.name} — ${namespaceLabelFor(m.namespace_id)}`
    : m.name;
}

const versionOptions = computed(() => {
  const ai = nodeApplyModel.value?.apply_input;
  if (!ai?.model_name) return [];
  const ns = ai.namespace_id ?? null;
  return allArtifacts.value
    .filter((a) => a.name === ai.model_name && (a.namespace_id ?? null) === ns)
    .map((a) => a.version)
    .sort((a, b) => b - a);
});

function formatUpstreamLabel(opt: UpstreamTrainModelOption): string {
  const desc = opt.description?.trim();
  const target = opt.target_column ? `target=${opt.target_column}` : "";
  const algo = opt.model_type || "";
  const tail = [algo, target].filter(Boolean).join(", ");
  const head = desc || `Node ${opt.node_id}`;
  return tail ? `${head} (${tail})` : head;
}

watch(
  () => nodeApplyModel.value?.apply_input.source,
  (source, prev) => {
    if (!nodeApplyModel.value || source === prev) return;
    const ai = nodeApplyModel.value.apply_input;
    if (source === "upstream") {
      ai.model_name = "";
      ai.model_version = null;
      ai.namespace_id = null;
      selectedModelKey.value = "";
      versionSelection.value = null;
    } else {
      ai.upstream_node_id = null;
    }
  },
);

function onModelSelected(key: string) {
  if (!nodeApplyModel.value) return;
  const m = availableModels.value.find((x) => modelKey(x) === key);
  if (!m) return;
  // Pin both name and namespace so versionOptions filters correctly even when
  // the user picked "Any namespace" first and then chose a model.
  nodeApplyModel.value.apply_input.model_name = m.name;
  nodeApplyModel.value.apply_input.namespace_id = m.namespace_id ?? null;
  nodeApplyModel.value.apply_input.model_version = null;
  versionSelection.value = null;
}

function onNamespaceChange() {
  if (nodeApplyModel.value) {
    nodeApplyModel.value.apply_input.model_name = "";
    nodeApplyModel.value.apply_input.model_version = null;
    selectedModelKey.value = "";
    versionSelection.value = null;
  }
}

function onVersionSelected(v: number | null | undefined) {
  if (nodeApplyModel.value) {
    // el-select with clearable emits undefined when cleared — normalize to null
    // so the saved settings stay consistent with the Pydantic schema.
    nodeApplyModel.value.apply_input.model_version = v ?? null;
  }
}

async function loadModels() {
  loadingModels.value = true;
  try {
    const resp = await axios.get<MLArtifactListItem[]>(
      "/artifacts/?python_type_contains=flowfile.ml.&limit=500",
    );
    allArtifacts.value = resp.data;
  } catch (e) {
    console.error("Failed to fetch ML artifacts", e);
    allArtifacts.value = [];
  } finally {
    loadingModels.value = false;
  }
}

async function loadNamespaces() {
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

async function loadUpstreamTrainNodes(flowId: number, nodeId: number) {
  loadingUpstream.value = true;
  try {
    const resp = await axios.get<UpstreamTrainModelOption[]>(
      `/ml/upstream-train-models?flow_id=${flowId}&node_id=${nodeId}`,
    );
    upstreamOptions.value = resp.data;
  } catch (e) {
    console.error("Failed to load /ml/upstream-train-models", e);
    upstreamOptions.value = [];
  } finally {
    loadingUpstream.value = false;
  }
}

const loadNodeData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeApplyModel.value = nodeData.value?.setting_input as NodeApplyModel;
  const flowId = Number(nodeData.value?.flow_id);
  await Promise.all([loadModels(), loadNamespaces(), loadUpstreamTrainNodes(flowId, nodeId)]);
  if (nodeApplyModel.value) {
    if (!nodeApplyModel.value.is_setup || !nodeApplyModel.value.apply_input) {
      nodeApplyModel.value.apply_input = {
        source: "upstream",
        upstream_node_id: null,
        model_name: "",
        model_version: null,
        namespace_id: null,
        output_column: "prediction",
      };
    } else {
      // Backfill defaults for older saved settings that pre-date the source field.
      if (!nodeApplyModel.value.apply_input.source) {
        nodeApplyModel.value.apply_input.source = nodeApplyModel.value.apply_input.model_name
          ? "catalog"
          : "upstream";
      }
      if (nodeApplyModel.value.apply_input.upstream_node_id === undefined) {
        nodeApplyModel.value.apply_input.upstream_node_id = null;
      }
      if (nodeApplyModel.value.apply_input.namespace_id === undefined) {
        nodeApplyModel.value.apply_input.namespace_id = null;
      }
    }
    // Drop a saved upstream_node_id that no longer matches a valid upstream Train
    // Model — otherwise el-select keeps showing the stale id alongside the
    // "no train models found" banner.
    const savedUpstreamId = nodeApplyModel.value.apply_input.upstream_node_id;
    if (
      savedUpstreamId != null &&
      !upstreamOptions.value.some((opt) => opt.node_id === savedUpstreamId)
    ) {
      nodeApplyModel.value.apply_input.upstream_node_id = null;
    }
    versionSelection.value = nodeApplyModel.value.apply_input.model_version ?? null;
    // Restore the model picker's synthetic key from the saved name + namespace
    // so re-opening a saved Apply node reselects the right entry even when a
    // sibling schema has the same model name.
    if (nodeApplyModel.value.apply_input.model_name) {
      selectedModelKey.value = modelKey({
        name: nodeApplyModel.value.apply_input.model_name,
        namespace_id: nodeApplyModel.value.apply_input.namespace_id ?? null,
      });
    } else {
      selectedModelKey.value = "";
    }
    dataLoaded.value = true;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
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
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.4;
  margin: var(--spacing-2) 0;
}

.info-banner {
  margin-top: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  background-color: var(--color-info-background, #f0f7ff);
  border-left: 3px solid var(--color-info, #1890ff);
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
