<template>
  <div class="node-designer-container">
    <DesignerHeader
      :node-name="store.nodeMetadata.node_name"
      :is-dirty="store.isDirty"
      :saving="saving"
      @browse="openBrowser"
      @new="handleNew"
      @view-code="handleViewCode"
      @save="handleSave"
    />

    <el-alert
      v-if="store.restoreAvailable"
      type="info"
      class="restore-banner"
      :closable="false"
      show-icon
    >
      <template #title>
        <span>You have unsaved changes from a previous session.</span>
        <button class="btn btn-sm btn-primary" @click="store.restore()">Restore</button>
        <button class="btn btn-sm btn-secondary" @click="store.discardDraft()">Discard</button>
      </template>
    </el-alert>

    <div class="designer-layout">
      <MetadataPanel />
      <WorkspaceTabs :extensions="autocompletion.extensions" />
    </div>

    <CodePreviewModal
      :show="showPreviewModal"
      :code="previewedCode"
      @close="showPreviewModal = false"
    />

    <NodeBrowserModal
      :show="browser.showNodeBrowser.value"
      :nodes="browser.customNodes.value"
      :loading="browser.loadingNodes.value"
      :viewing-node-code="browser.viewingNodeCode.value"
      :viewing-node-name="browser.viewingNodeName.value"
      :read-only-extensions="autocompletion.readOnlyExtensions"
      @close="browser.closeNodeBrowser()"
      @view-node="browser.viewCustomNode($event)"
      @back="browser.backToNodeList()"
      @edit="handleEdit"
      @duplicate="handleDuplicate"
      @delete="handleDelete"
    />
  </div>
</template>

<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from "vue";
import { onBeforeRouteLeave, useRoute } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";

import DesignerHeader from "./nodeDesigner/designer/DesignerHeader.vue";
import MetadataPanel from "./nodeDesigner/designer/MetadataPanel.vue";
import WorkspaceTabs from "./nodeDesigner/designer/WorkspaceTabs.vue";
import CodePreviewModal from "./nodeDesigner/CodePreviewModal.vue";
import NodeBrowserModal from "./nodeDesigner/NodeBrowserModal.vue";

import { useNodeDesignerStore } from "@/stores/node-designer-store";
import { usePolarsAutocompletion } from "./nodeDesigner/composables";
import { useNodeBrowser } from "./nodeDesigner/composables/useNodeBrowser";
import { duplicateNode, loadNodeForEdit, previewCode, saveNode } from "./nodeDesigner/loadSave";
import { deleteCustomNode } from "../api/nodeDesigner";

const store = useNodeDesignerStore();
const browser = useNodeBrowser();
const autocompletion = usePolarsAutocompletion(
  () => store.sections,
  () => store.codeOnly,
);

const saving = ref(false);
const showPreviewModal = ref(false);
const previewedCode = ref("");

const route = useRoute();

onMounted(() => {
  store.initialize();
  window.addEventListener("beforeunload", handleBeforeUnload);
  // Deep link from the catalog's Custom Nodes tab (?openFile=<file.py>).
  const openFile = route.query.openFile;
  if (typeof openFile === "string" && openFile) {
    loadNodeForEdit(openFile).catch((error) => {
      console.error("Failed to open node from query param:", error);
    });
  }
});

onBeforeUnmount(() => {
  window.removeEventListener("beforeunload", handleBeforeUnload);
});

function handleBeforeUnload(event: BeforeUnloadEvent) {
  if (store.isDirty) {
    event.preventDefault();
    event.returnValue = "";
  }
}

async function confirmDiscardChanges(): Promise<boolean> {
  if (!store.isDirty) return true;
  try {
    await ElMessageBox.confirm(
      "You have unsaved changes. Leaving now keeps a local draft you can restore later. Continue?",
      "Unsaved changes",
      { confirmButtonText: "Leave", cancelButtonText: "Stay", type: "warning" },
    );
    return true;
  } catch {
    return false;
  }
}

onBeforeRouteLeave(async () => await confirmDiscardChanges());

async function handleNew() {
  if (!(await confirmDiscardChanges())) return;
  store.newNode();
}

function openBrowser() {
  browser.openNodeBrowser();
}

async function handleViewCode() {
  try {
    previewedCode.value = await previewCode();
    showPreviewModal.value = true;
  } catch (e: unknown) {
    const err = e as { response?: { data?: { detail?: string } }; message?: string };
    ElMessage.error(
      `Failed to render code: ${err.response?.data?.detail || err.message || "error"}`,
    );
  }
}

async function handleSave() {
  saving.value = true;
  try {
    await saveNode();
  } finally {
    saving.value = false;
  }
}

async function handleEdit(fileName: string) {
  if (!(await confirmDiscardChanges())) return;
  try {
    await loadNodeForEdit(fileName);
    browser.closeNodeBrowser();
  } catch (e: unknown) {
    const err = e as { message?: string };
    ElMessage.error(`Failed to open node: ${err.message || "error"}`);
  }
}

async function handleDuplicate(fileName: string) {
  if (!(await confirmDiscardChanges())) return;
  try {
    await duplicateNode(fileName);
    browser.closeNodeBrowser();
    ElMessage.info("Loaded a copy — give it a new name and save.");
  } catch (e: unknown) {
    const err = e as { message?: string };
    ElMessage.error(`Failed to duplicate node: ${err.message || "error"}`);
  }
}

async function handleDelete(fileName: string) {
  try {
    await deleteCustomNode(fileName);
    if (store.sourceFile === fileName) store.newNode();
    await browser.fetchCustomNodes();
    ElMessage.success("Node deleted.");
  } catch (e: unknown) {
    const err = e as { response?: { data?: { detail?: string } }; message?: string };
    ElMessage.error(`Failed to delete: ${err.response?.data?.detail || err.message || "error"}`);
  }
}
</script>

<style>
@import "./nodeDesigner/nodeDesigner.css";
</style>

<style scoped>
.node-designer-container {
  padding: 1rem;
  height: 100vh;
  max-height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  box-sizing: border-box;
}

.restore-banner {
  margin-bottom: 1rem;
  flex-shrink: 0;
}

.restore-banner :deep(.el-alert__title) {
  display: inline-flex;
  align-items: center;
  gap: 0.75rem;
}

.designer-layout {
  display: grid;
  grid-template-columns: 280px 1fr;
  gap: 1rem;
  flex: 1;
  min-height: 0;
  height: 0;
  overflow: hidden;
}
</style>
