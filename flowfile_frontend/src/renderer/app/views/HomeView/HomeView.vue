<template>
  <welcome-screen
    :recent-flows="recentFlows"
    @create="handleQuickCreate"
    @create-at-location="createDialogVisible = true"
    @open="openDialogVisible = true"
    @browse-templates="browseTemplates"
    @open-recent="handleOpenRecent"
    @start-tutorial="handleStartTutorial"
  />

  <open-dialog v-model:visible="openDialogVisible" @open-flow="handleOpenFromDialog" />
  <create-dialog v-model:visible="createDialogVisible" @create-complete="handleCreateComplete" />
</template>

<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import WelcomeScreen from "./WelcomeScreen.vue";
import OpenDialog from "../../features/designer/components/OpenDialog.vue";
import CreateDialog from "../../features/designer/components/CreateDialog.vue";
import { createFlow, getFlowSettings } from "../../components/nodes/nodeLogic";
import { useNodeStore } from "../../stores/column-store";
import { useRecentFlows } from "../../composables/useRecentFlows";
import { useFlowOpener } from "../../composables/useFlowOpener";
import { useTutorialStore } from "../../stores/tutorial-store";
import { gettingStartedTutorial } from "../../components/tutorial/tutorials";

const router = useRouter();
const nodeStore = useNodeStore();
const tutorialStore = useTutorialStore();
const { recentFlows, recordFlow, refreshCatalogRefs } = useRecentFlows();
const { openFlow } = useFlowOpener();

const openDialogVisible = ref(false);
const createDialogVisible = ref(false);

// The designer Canvas (which owns Cmd+N / Cmd+O) isn't mounted here, so without
// this the WebView's native Cmd+O opens the OS file dialog. Honor the shortcut
// hints shown on the welcome tiles by opening the in-app dialogs instead.
const handleKeyDown = (event: KeyboardEvent) => {
  if (!(event.metaKey || event.ctrlKey)) return;
  const target = event.target as HTMLElement;
  const isTyping =
    target.tagName === "INPUT" ||
    target.tagName === "TEXTAREA" ||
    target.isContentEditable ||
    target.closest(".cm-editor") !== null;
  if (isTyping) return;

  const key = event.key.toLowerCase();
  if (key === "o") {
    event.preventDefault();
    openDialogVisible.value = true;
  } else if (key === "n") {
    event.preventDefault();
    handleQuickCreate();
  }
};

onMounted(() => {
  refreshCatalogRefs();
  window.addEventListener("keydown", handleKeyDown);
});

onBeforeUnmount(() => {
  window.removeEventListener("keydown", handleKeyDown);
});

const goToDesigner = () => router.push({ name: "designer" });
const browseTemplates = () => router.push({ name: "templates" });

const recordCreatedFlow = async (flowId: number, catalogRef?: string) => {
  try {
    const settings = await getFlowSettings(flowId);
    if (settings?.path) {
      recordFlow({ path: settings.path, name: settings.name, catalogRef });
    }
  } catch (error) {
    console.warn("Failed to record created flow as recent:", error);
  }
};

const handleQuickCreate = async () => {
  try {
    const flowId = await createFlow(null, null);
    nodeStore.setFlowId(flowId);
    await recordCreatedFlow(flowId);
    ElMessage.success("Flow created");
    goToDesigner();
  } catch (error) {
    console.error("Failed to create flow:", error);
    ElMessage.error("Failed to create flow");
  }
};

const handleCreateComplete = async (flowId: number, catalogRef?: string) => {
  createDialogVisible.value = false;
  if (!flowId) return;
  nodeStore.setFlowId(flowId);
  await recordCreatedFlow(flowId, catalogRef);
  goToDesigner();
};

const handleOpenFromDialog = async (payload: {
  message: string;
  flowPath: string;
  flowName?: string;
  catalogRef?: string;
}) => {
  const flowId = await openFlow(payload.flowPath, {
    name: payload.flowName,
    catalogRef: payload.catalogRef,
  });
  if (flowId !== null) goToDesigner();
};

const handleOpenRecent = async (flowPath: string) => {
  const flowId = await openFlow(flowPath);
  if (flowId !== null) goToDesigner();
};

// The tutorial's data-tutorial anchors live in the designer header, so route
// there first (mirrors Sidebar's guard).
const handleStartTutorial = async () => {
  await router.push({ name: "designer" });
  tutorialStore.startTutorial(gettingStartedTutorial);
};
</script>
