<template>
  <div v-if="dataLoaded && nodePolarsCode" class="listbox-wrapper">
    <generic-node-settings
      :model-value="nodePolarsCode"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div v-if="aiGenerateEnabled && showEditor && nodePolarsCode" class="code-toolbar">
        <AiGenerateCodeButton
          :flow-id="Number(nodePolarsCode.flow_id)"
          :node-id="nodePolarsCode.node_id"
          node-type="polars_code"
          @code-generated="applyGeneratedCode"
        />
      </div>
      <pythonEditor
        v-if="showEditor && nodePolarsCode"
        ref="editorChild"
        :editor-string="nodePolarsCode.polars_code_input.polars_code"
        @update-editor-string="handleEditorUpdate"
      />
    </generic-node-settings>
  </div>

  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref } from "vue";
import { CodeLoader } from "vue-content-loader";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import pythonEditor from "../../../../../features/designer/editor/pythonEditor.vue";
import AiGenerateCodeButton from "../../../../../features/designer/editor/AiGenerateCodeButton.vue";
import { AI_GENERATE_CODE_ENABLED as aiGenerateEnabled } from "../../../../../stores/ai-code-generator-store";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { createPolarsCodeNode } from "./utils";

import { NodePolarsCode } from "../../../baseNode/nodeInput";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

const showEditor = ref<boolean>(false);
const nodeStore = useNodeStore();
const dataLoaded = ref(false);

interface EditorChildType {
  showHideOptions: () => void;
  showTools: boolean;
  setCode?: (text: string) => void;
}

const editorChild = ref<EditorChildType | null>(null);
const nodePolarsCode = ref<NodePolarsCode | null>(null);
const nodeData = ref<null | NodeData>(null);

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodePolarsCode,
  onBeforeSave: () => {
    if (!nodePolarsCode.value || !nodePolarsCode.value.polars_code_input.polars_code) {
      return false;
    }
    return true;
  },
});

const handleEditorUpdate = (newCode: string) => {
  if (nodePolarsCode.value && nodePolarsCode.value.polars_code_input) {
    nodePolarsCode.value.polars_code_input.polars_code = newCode;
  }
};

const applyGeneratedCode = (code: string) => {
  // setCode updates the CodeMirror view; its emit syncs back to the model.
  editorChild.value?.setCode?.(code);
  if (nodePolarsCode.value?.polars_code_input) {
    nodePolarsCode.value.polars_code_input.polars_code = code;
  }
};

const loadNodeData = async (nodeId: number) => {
  try {
    nodeData.value = await nodeStore.getNodeData(nodeId, false);
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

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.code-toolbar {
  display: flex;
  justify-content: flex-end;
  margin-bottom: 6px;
}
</style>
