<template>
  <div v-if="dataLoaded" class="listbox-wrapper">
    <pythonEditor
      v-if="showEditor && nodePolarsCode"
      ref="editorChild"
      :editor-string="nodePolarsCode.polars_code_input.polars_code"
      @update-editor-string="handleEditorUpdate"
    />
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/column-store";
import pythonEditor from "../../../editor/pythonEditor.vue";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { createPolarsCodeNode } from "./utils";

import { NodePolarsCode } from "../../../baseNode/nodeInput";

const showEditor = ref<boolean>(false);
const nodeStore = useNodeStore();
const dataLoaded = ref(false);

interface EditorChildType {
  showHideOptions: () => void;
  showTools: boolean;
}

const editorChild = ref<EditorChildType | null>(null);
const nodePolarsCode = ref<NodePolarsCode | null>(null);
const nodeData = ref<null | NodeData>(null);

const handleEditorUpdate = (newCode: string) => {
  if (nodePolarsCode.value && nodePolarsCode.value.polars_code_input) {
    nodePolarsCode.value.polars_code_input.polars_code = newCode;
  }
};

const loadNodeData = async (nodeId: number) => {
  try {
    nodeData.value = await nodeStore.getNodeData(1, nodeId, false);
    if (nodeData.value) {
      const hasValidSetup = Boolean(
        nodeData.value?.setting_input?.is_setup && nodeData.value?.setting_input?.polars_code_input,
      );

      nodePolarsCode.value = hasValidSetup
        ? nodeData.value.setting_input
        : createPolarsCodeNode(nodeStore.flow_id, nodeStore.node_id);

      showEditor.value = true;
      dataLoaded.value = true;
    }
  } catch (error) {
    console.error("Failed to load node data:", error);
    showEditor.value = false;
    dataLoaded.value = false;
  }
};

const pushNodeData = async () => {
  if (!nodePolarsCode.value || !nodePolarsCode.value.polars_code_input.polars_code) {
    return;
  }
  nodePolarsCode.value.is_setup = true;
  nodeStore.updateSettings(nodePolarsCode);
  showEditor.value = false;
  dataLoaded.value = false;
};

defineExpose({ loadNodeData, pushNodeData });
</script>
