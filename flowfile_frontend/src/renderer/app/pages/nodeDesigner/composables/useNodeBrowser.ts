/**
 * Node browser state: list custom nodes and preview a node's raw code.
 * Edit/duplicate/delete actions are orchestrated by NodeDesigner.vue via loadSave.
 */
import { ref } from "vue";
import { getCustomNode, listCustomNodes } from "../../../api/nodeDesigner";
import {
  fetchInstalled,
  type InstallReceipt,
  type InstalledCommunityNode,
} from "../../../api/communityNodes";
import type { CustomNodeInfo } from "../types";

export function useNodeBrowser() {
  const showNodeBrowser = ref(false);
  const customNodes = ref<CustomNodeInfo[]>([]);
  const loadingNodes = ref(false);
  // Keyed by receipt.file_name: the receipt is the only marker that a local
  // node came from the registry, and file_name is what both surfaces carry
  // on disk (node_key can diverge from node_id).
  const communityByFile = ref<Record<string, InstallReceipt>>({});

  const viewingNodeCode = ref("");
  const viewingNodeName = ref("");
  const viewingNodeFileName = ref("");

  async function fetchCustomNodes() {
    loadingNodes.value = true;
    try {
      // /community_nodes/installed is JWT-only and reads a local sidecar, so it
      // works offline; a failure just means no Community chips.
      const [nodes, installed] = await Promise.all([
        listCustomNodes(),
        fetchInstalled().catch(() => [] as InstalledCommunityNode[]),
      ]);
      customNodes.value = nodes as unknown as CustomNodeInfo[];
      communityByFile.value = Object.fromEntries(
        installed.map((entry) => [entry.receipt.file_name, entry.receipt]),
      );
    } catch (error) {
      console.error("Failed to fetch custom nodes:", error);
      customNodes.value = [];
      communityByFile.value = {};
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
    communityByFile,
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
