<template>
  <welcome-screen
    :recent-flows="recentFlows"
    :open-flows="openFlows"
    @create="handleQuickCreate"
    @create-at-location="createDialogVisible = true"
    @open="openDialogVisible = true"
    @browse-templates="browseTemplates"
    @open-recent="handleOpenRecent"
    @open-session="handleOpenSession"
    @remove-recent="handleRemoveRecent"
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
import { FlowApi } from "../../api";
import type { FlowSettings } from "../../types";
import { useNodeStore } from "../../stores/column-store";
import { useRecentFlows } from "../../composables/useRecentFlows";
import { useFlowOpener } from "../../composables/useFlowOpener";
import { isDesktop } from "../../../lib/desktop";
import { useTutorialStore } from "../../stores/tutorial-store";
import { gettingStartedTutorial } from "../../components/tutorial/tutorials";

const router = useRouter();
const nodeStore = useNodeStore();
const tutorialStore = useTutorialStore();
const { recentFlows, recordFlowFromSettings, refreshCatalogRefs, removeFlow } = useRecentFlows();
const { openFlow } = useFlowOpener();

const openDialogVisible = ref(false);
const createDialogVisible = ref(false);
const openFlows = ref<FlowSettings[]>([]);

const loadOpenFlows = async () => {
  try {
    openFlows.value = await FlowApi.getAllFlows();
  } catch (error) {
    console.warn("Failed to load open flows:", error);
    openFlows.value = [];
  }
};

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
  } else if (key === "n" && isDesktop) {
    // Browsers reserve Cmd/Ctrl+N (new window) and don't deliver a preventable
    // keydown, so the hint and this branch are desktop-only (matches the tile).
    event.preventDefault();
    handleQuickCreate();
  }
};

onMounted(() => {
  refreshCatalogRefs();
  void loadOpenFlows();
  window.addEventListener("keydown", handleKeyDown);
  // Refresh open sessions when the user tabs back (a run may have finished or a
  // flow closed elsewhere). HomeView itself re-mounts on each nav (no keep-alive).
  window.addEventListener("focus", loadOpenFlows);
});

onBeforeUnmount(() => {
  window.removeEventListener("keydown", handleKeyDown);
  window.removeEventListener("focus", loadOpenFlows);
});

const goToDesigner = () => router.push({ name: "designer" });
const browseTemplates = () => router.push({ name: "templates" });

const recordCreatedFlow = async (flowId: number, catalogRef?: string) => {
  try {
    recordFlowFromSettings(await getFlowSettings(flowId), catalogRef);
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

// Already-open session: just activate it (setFlowId triggers the Canvas watcher
// which loads the flow). Never importFlow here — that would spawn a duplicate.
const handleOpenSession = (flowId: number) => {
  nodeStore.setFlowId(flowId);
  goToDesigner();
};

// Non-destructive: only clears the localStorage entry, never the file.
const handleRemoveRecent = (flowPath: string) => removeFlow(flowPath);

// The tutorial's data-tutorial anchors live in the designer header, so route
// there first (mirrors Sidebar's guard).
const handleStartTutorial = async () => {
  await router.push({ name: "designer" });
  tutorialStore.startTutorial(gettingStartedTutorial);
};
</script>
