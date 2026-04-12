<template>
  <div v-if="dataLoaded && nodeSqlQuery" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodeSqlQuery"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="sql-editor-wrapper">
        <h4 class="section-subtitle">SQL Query</h4>
        <p class="section-hint">
          Connected inputs are available as <code>input_1</code>, <code>input_2</code>, etc.
        </p>
        <div class="editor-container">
          <codemirror
            v-model="sqlCode"
            placeholder="SELECT * FROM input_1"
            :style="{ height: '200px' }"
            :autofocus="true"
            :indent-with-tab="false"
            :tab-size="2"
            :extensions="extensions"
          />
        </div>
      </div>
    </generic-node-settings>
  </div>

  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed, watch } from "vue";
import { CodeLoader } from "vue-content-loader";
import { sql } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { Codemirror } from "vue-codemirror";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import type { NodeData } from "../../../baseNode/nodeInterfaces";
import { createSqlQueryNode } from "./utils";

import type { NodeSqlQuery } from "../../../baseNode/nodeInput";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);

const nodeSqlQuery = ref<NodeSqlQuery | null>(null);
const nodeData = ref<null | NodeData>(null);
const sqlCode = ref("");

const extensions = computed(() => [sql({ upperCaseKeywords: true }), oneDark]);

watch(sqlCode, (newCode) => {
  if (nodeSqlQuery.value) {
    nodeSqlQuery.value.sql_query_input.sql_code = newCode;
  }
});

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeSqlQuery,
  onBeforeSave: () => {
    if (!nodeSqlQuery.value || !nodeSqlQuery.value.sql_query_input.sql_code) {
      return false;
    }
    return true;
  },
});

const loadNodeData = async (nodeId: number) => {
  try {
    nodeData.value = await nodeStore.getNodeData(nodeId, false);
    if (nodeData.value) {
      const hasValidSetup = Boolean(
        nodeData.value?.setting_input?.is_setup && nodeData.value?.setting_input?.sql_query_input,
      );

      nodeSqlQuery.value = hasValidSetup
        ? nodeData.value.setting_input
        : createSqlQueryNode(nodeStore.flow_id, nodeStore.node_id);

      sqlCode.value = nodeSqlQuery.value?.sql_query_input?.sql_code ?? "";
      dataLoaded.value = true;
    }
  } catch (error) {
    console.error("Failed to load node data:", error);
    dataLoaded.value = false;
  }
};

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.sql-editor-wrapper {
  padding: 8px 0;
}

.section-subtitle {
  margin: 0 0 4px 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.section-hint {
  margin: 0 0 8px 0;
  font-size: 0.8rem;
  color: var(--color-text-muted);
}

.section-hint code {
  background: var(--color-background-secondary);
  padding: 1px 4px;
  border-radius: 3px;
  font-size: 0.8rem;
}

.editor-container {
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  overflow: hidden;
  font-size: 13px;
}
</style>
