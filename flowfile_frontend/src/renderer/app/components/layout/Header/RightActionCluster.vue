<script setup lang="ts">
// Right-aligned designer header cluster.
//
// Layout: [Ask AI ⌘K] [<> Code] [▶ Run] | [⚙] [👁/—]
//
// Ask AI / Code / Run / Settings live here so the cluster reads as one
// unified action set, matching the target mockup. The results toggle
// (formerly in status.vue) tucks in at the right end as an icon-only
// button — too useful to drop, small enough to coexist.
//
// Keyboard shortcuts (Cmd+E, Cmd+,) reach this cluster via DesignerView's
// canvas event handlers binding `@run` / `@open-settings` to the
// rightCluster ref's exposed runFlow / openSettings methods.

import { computed, ref } from "vue";
import { View, Minus } from "@element-plus/icons-vue";
import { useNodeStore } from "../../../stores/column-store";
import { useEditorStore } from "../../../stores/editor-store";
import { useItemStore } from "../../common/DraggableItem/stateStore";
import { useTutorialStore } from "../../../stores/tutorial-store";
import AiAssistantTrigger from "../../../features/ai/AiAssistantTrigger.vue";
import RunButton from "./run.vue";
import PopOver from "../../../features/designer/editor/PopOver.vue";

const nodeStore = useNodeStore();
const editorStore = useEditorStore();
const tutorialStore = useTutorialStore();
const draggableItemStore = useItemStore();

const runButton = ref<InstanceType<typeof RunButton> | null>(null);

const showFlowResult = computed(() => nodeStore.showFlowResult);

const emit = defineEmits(["open-settings"]);

const toggleCodeGenerator = (): void => {
  nodeStore.toggleCodeGenerator();
  if (tutorialStore.isActive && tutorialStore.currentStep?.id === "generate-code") {
    setTimeout(() => {
      tutorialStore.nextStep();
    }, 300);
  }
};

const openSettings = (): void => {
  emit("open-settings");
};

const runFlow = (): void => {
  runButton.value?.runFlow();
};

const toggleResults = (): void => {
  editorStore.showFlowResult = !editorStore.showFlowResult;
  editorStore.isShowingLogViewer = editorStore.showFlowResult;
  if (editorStore.isShowingLogViewer) {
    draggableItemStore.bringToFront("logViewer");
    draggableItemStore.bringToFront("flowresults");
  }
};

defineExpose({
  runFlow,
  openSettings,
});
</script>

<template>
  <div class="right-cluster">
    <ai-assistant-trigger />

    <pop-over content="Toggle Code Generator" placement="bottom">
      <button
        class="action-btn"
        data-tutorial="generate-code-btn"
        :class="{ active: nodeStore.showCodeGenerator }"
        :aria-label="nodeStore.showCodeGenerator ? 'Hide Code Generator' : 'Show Code Generator'"
        :aria-pressed="nodeStore.showCodeGenerator"
        type="button"
        @click="toggleCodeGenerator"
      >
        <span class="material-icons btn-icon" aria-hidden="true">code</span>
        <span class="btn-text">Code</span>
      </button>
    </pop-over>

    <pop-over content="Run flow (⌘E)" placement="bottom">
      <run-button ref="runButton" :flow-id="nodeStore.flow_id" data-tutorial="run-btn" />
    </pop-over>

    <span class="cluster-separator" aria-hidden="true" />

    <pop-over content="Flow settings (⌘,)" placement="bottom">
      <button
        class="action-btn action-btn--icon-only"
        data-tutorial="settings-btn"
        type="button"
        aria-label="Flow settings"
        @click="openSettings"
      >
        <span class="material-icons btn-icon" aria-hidden="true">settings</span>
      </button>
    </pop-over>

    <pop-over content="Show or hide logs and status" placement="bottom">
      <button
        class="action-btn action-btn--icon-only"
        :class="{ active: showFlowResult }"
        type="button"
        :aria-label="showFlowResult ? 'Hide results panel' : 'Show results panel'"
        :aria-pressed="showFlowResult"
        @click="toggleResults"
      >
        <el-icon v-if="!showFlowResult"><View /></el-icon>
        <el-icon v-else><Minus /></el-icon>
      </button>
    </pop-over>
  </div>
</template>

<style scoped>
.right-cluster {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  height: 50px;
  padding-right: var(--spacing-3);
  font-family: var(--font-family-base);
}

.action-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  height: 28px;
  padding: 0 10px;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  cursor: pointer;
  transition: all var(--transition-fast);
  color: var(--color-text-primary);
  font-size: 12px;
  font-weight: var(--font-weight-medium);
  box-shadow: var(--shadow-xs);
  white-space: nowrap;
}

.action-btn:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-border-secondary);
}

.action-btn:active {
  transform: translateY(1px);
  box-shadow: none;
}

.action-btn.active {
  background-color: var(--color-accent-subtle);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.action-btn--icon-only {
  padding: 0;
  min-width: 28px;
  justify-content: center;
}

.btn-icon {
  font-size: 14px;
  color: var(--color-text-secondary);
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-rendering: optimizeLegibility;
}

.action-btn:hover .btn-icon {
  color: var(--color-text-primary);
}

.action-btn.active .btn-icon {
  color: var(--color-accent);
}

.btn-text {
  white-space: nowrap;
}

.cluster-separator {
  display: inline-block;
  width: 1px;
  height: 20px;
  background: var(--color-border-primary);
  margin: 0 var(--spacing-1);
}

.action-btn--icon-only :deep(.el-icon) {
  font-size: 14px;
  line-height: 1;
}
</style>
