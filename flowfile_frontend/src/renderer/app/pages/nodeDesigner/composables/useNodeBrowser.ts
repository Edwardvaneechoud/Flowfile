/**
 * Composable for node browser functionality
 */
import { ref } from 'vue';
import axios from 'axios';
import type { CustomNodeInfo } from '../types';

export function useNodeBrowser() {
  const showNodeBrowser = ref(false);
  const customNodes = ref<CustomNodeInfo[]>([]);
  const loadingNodes = ref(false);

  // View node code state
  const viewingNodeCode = ref('');
  const viewingNodeName = ref('');
  const viewingNodeFileName = ref('');
  const showDeleteConfirm = ref(false);

  async function fetchCustomNodes() {
    loadingNodes.value = true;
    try {
      const response = await axios.get('/user_defined_components/list-custom-nodes');
      customNodes.value = response.data;
    } catch (error) {
      console.error('Failed to fetch custom nodes:', error);
      customNodes.value = [];
    } finally {
      loadingNodes.value = false;
    }
  }

  async function viewCustomNode(fileName: string) {
    try {
      const response = await axios.get(`/user_defined_components/get-custom-node/${fileName}`);
      const nodeData = response.data;

      viewingNodeFileName.value = fileName;
      viewingNodeName.value = nodeData.metadata?.node_name || fileName;
      viewingNodeCode.value = nodeData.content || '// No content available';
    } catch (error: any) {
      console.error('Failed to load custom node:', error);
      viewingNodeCode.value = `// Error loading node: ${error.message || 'Unknown error'}`;
    }
  }

  function openNodeBrowser() {
    fetchCustomNodes();
    viewingNodeCode.value = '';
    viewingNodeName.value = '';
    viewingNodeFileName.value = '';
    showNodeBrowser.value = true;
  }

  function closeNodeBrowser() {
    showNodeBrowser.value = false;
    viewingNodeCode.value = '';
    viewingNodeName.value = '';
    viewingNodeFileName.value = '';
  }

  function backToNodeList() {
    viewingNodeCode.value = '';
    viewingNodeName.value = '';
    viewingNodeFileName.value = '';
  }

  function confirmDeleteNode() {
    showDeleteConfirm.value = true;
  }

  async function deleteNode() {
    if (!viewingNodeFileName.value) return;

    try {
      await axios.delete(`/user_defined_components/delete-custom-node/${viewingNodeFileName.value}`);
      showDeleteConfirm.value = false;
      backToNodeList();
      fetchCustomNodes();
    } catch (error: any) {
      console.error('Failed to delete custom node:', error);
      alert(`Error deleting node: ${error.response?.data?.detail || error.message || 'Unknown error'}`);
      showDeleteConfirm.value = false;
    }
  }

  return {
    // State
    showNodeBrowser,
    customNodes,
    loadingNodes,
    viewingNodeCode,
    viewingNodeName,
    viewingNodeFileName,
    showDeleteConfirm,

    // Methods
    fetchCustomNodes,
    viewCustomNode,
    openNodeBrowser,
    closeNodeBrowser,
    backToNodeList,
    confirmDeleteNode,
    deleteNode,
  };
}
