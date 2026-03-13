<template>
  <div v-if="dataLoaded && nodeData" class="listbox-wrapper">
    <div class="main-part">
      <div class="catalog-field">
        <label class="catalog-label">Table name</label>
        <el-input
          v-model="nodeData.catalog_write_settings.table_name"
          size="small"
          placeholder="Enter table name"
        />
      </div>

      <div class="catalog-field">
        <label class="catalog-label">Catalog / Schema</label>
        <el-select
          v-model="nodeData.catalog_write_settings.namespace_id"
          size="small"
          placeholder="Select namespace"
          clearable
        >
          <el-option
            v-for="ns in catalogNamespaces"
            :key="ns.id"
            :label="ns.label"
            :value="ns.id"
          />
        </el-select>
      </div>

      <div class="catalog-field">
        <label class="catalog-label">Write mode</label>
        <el-select v-model="nodeData.catalog_write_settings.write_mode" size="small">
          <el-option label="Overwrite" value="overwrite" />
          <el-option label="Error if exists" value="error" />
        </el-select>
      </div>

      <div class="catalog-field">
        <label class="catalog-label">Description (optional)</label>
        <el-input
          v-model="nodeData.catalog_write_settings.description"
          size="small"
          type="textarea"
          :rows="2"
          placeholder="Table description"
        />
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted } from "vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { CatalogApi } from "../../../../../api/catalog.api";
import type { NodeCatalogWriter } from "../../../../../types/node.types";

const nodeStore = useNodeStore();
const nodeData = ref<NodeCatalogWriter | null>(null);
const dataLoaded = ref(false);

const { saveSettings, pushNodeData } = useNodeSettings({
  nodeRef: nodeData,
});

const catalogNamespaces = ref<{ id: number; label: string }[]>([]);

onMounted(async () => {
  try {
    const tree = await CatalogApi.getNamespaceTree();
    for (const catalog of tree) {
      for (const schema of catalog.children ?? []) {
        catalogNamespaces.value.push({
          id: schema.id,
          label: `${catalog.name} / ${schema.name}`,
        });
      }
    }
  } catch {
    // Catalog not available
  }
});

async function loadNodeData(nodeId: number) {
  const nodeResult = await nodeStore.getNodeData(nodeId, false);
  if (nodeResult?.setting_input && nodeResult.setting_input.is_setup) {
    nodeData.value = nodeResult.setting_input;
  } else {
    nodeData.value = {
      catalog_write_settings: {
        table_name: "",
        namespace_id: null,
        description: null,
        write_mode: "overwrite",
      },
      flow_id: nodeStore.flow_id,
      node_id: nodeId,
      cache_results: false,
      pos_x: 0,
      pos_y: 0,
      is_setup: false,
      description: "",
    };
  }
  dataLoaded.value = true;
}

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.main-part {
  display: flex;
  flex-direction: column;
  padding: 20px;
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
  background-color: var(--color-background-primary);
  margin-top: 20px;
  gap: 12px;
}

.catalog-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.catalog-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}
</style>
