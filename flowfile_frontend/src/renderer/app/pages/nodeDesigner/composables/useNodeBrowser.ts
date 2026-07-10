/**
 * Node browser state: list custom nodes and preview a node's raw code.
 * Edit/duplicate/delete actions are orchestrated by NodeDesigner.vue via loadSave.
 */
import { ref } from "vue";
import { getCustomNode, listCustomNodes } from "../../../api/nodeDesigner";
import type { CustomNodeInfo } from "../types";

export function useNodeBrowser() {
  const showNodeBrowser = ref(false);
  const customNodes = ref<CustomNodeInfo[]>([]);
  const loadingNodes = ref(false);

  const viewingNodeCode = ref("");
  const viewingNodeName = ref("");
  const viewingNodeFileName = ref("");

  async function fetchCustomNodes() {
    loadingNodes.value = true;
    try {
      customNodes.value = (await listCustomNodes()) as unknown as CustomNodeInfo[];
    } catch (error) {
      console.error("Failed to fetch custom nodes:", error);
      customNodes.value = [];
    } finally {
      loadingNodes.value = false;
    }
  }

  async function viewCustomNode(fileName: string) {
    try {
      const data = await getCustomNode(fileName);
      viewingNodeFileName.value = fileName;
      viewingNodeName.value = data.file_name || fileName;
      viewingNodeCode.value = data.code || data.content || "// No content available";
    } catch (error: unknown) {
      const e = error as { message?: string };
      viewingNodeCode.value = `// Error loading node: ${e.message || "Unknown error"}`;
    }
  }

  function openNodeBrowser() {
    fetchCustomNodes();
    backToNodeList();
    showNodeBrowser.value = true;
  }

  function closeNodeBrowser() {
    showNodeBrowser.value = false;
    backToNodeList();
  }

  function backToNodeList() {
    viewingNodeCode.value = "";
    viewingNodeName.value = "";
    viewingNodeFileName.value = "";
  }

  return {
    showNodeBrowser,
    customNodes,
    loadingNodes,
    viewingNodeCode,
    viewingNodeName,
    viewingNodeFileName,
    fetchCustomNodes,
    viewCustomNode,
    openNodeBrowser,
    closeNodeBrowser,
    backToNodeList,
  };
}
