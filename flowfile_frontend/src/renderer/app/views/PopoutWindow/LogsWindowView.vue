<template>
  <PopoutWindowFrame
    :title="title"
    :state="state"
    test-id="logs-window"
    @return="returnToDesigner"
    @close="closeThisWindow"
  >
    <LogViewer :key="flowId" />
  </PopoutWindowFrame>
</template>

<script lang="ts" setup>
// One flow's run logs in its own window: the viewer streams the tail and follows the flow's runs.
import LogViewer from "../DesignerView/LogViewer/LogViewer.vue";
import PopoutWindowFrame from "./PopoutWindowFrame.vue";
import { usePopoutWindowHost } from "./usePopoutWindowHost";
import { useEditorStore } from "../../stores/editor-store";

// This window's own store: the viewer streams whenever it mounts, running or not.
useEditorStore().isShowingLogViewer = true;

const { flowId, state, title, returnToDesigner, closeThisWindow } = usePopoutWindowHost({
  kind: "logs",
});
</script>
