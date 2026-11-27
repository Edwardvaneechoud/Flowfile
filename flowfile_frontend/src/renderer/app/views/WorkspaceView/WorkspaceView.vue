<template>
  <div class="workspace-page">
    <div class="workspace-heading">
      <h2 class="page-title">Workspace</h2>
      <p class="page-description">{{ WORKSPACE_PAGE_DESCRIPTION }}</p>
    </div>
    <div class="workspace-browser">
      <file-browser mode="open" context="workspace" page @file-selected="openFlowFile" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { useRouter } from "vue-router";
import FileBrowser from "../../components/common/FileBrowser/fileBrowser.vue";
import type { FileInfo } from "../../components/common/FileBrowser/types";
import { WORKSPACE_PAGE_DESCRIPTION } from "../../components/common/FileBrowser/workspaceRoots";
import { useFlowOpener } from "../../composables/useFlowOpener";
import { useFileBrowserStore } from "../../stores/fileBrowserStore";

const router = useRouter();
const { openFlow } = useFlowOpener();

// The page always opens on the person's own folder.
useFileBrowserStore().resetContext("workspace");

/** A flow file opens in the designer the way a catalog flow does: the flow is opened, then the designer shown. */
async function openFlowFile(file: FileInfo) {
  const flowId = await openFlow(file.path);
  if (flowId !== null) await router.push({ name: "designer" });
}
</script>

<style scoped>
.workspace-page {
  display: flex;
  flex-direction: column;
  height: 100%;
  max-width: 1200px;
  margin: 0 auto;
  padding: var(--spacing-5);
  box-sizing: border-box;
}

.workspace-heading {
  margin-bottom: var(--spacing-3);
}

.workspace-browser {
  flex: 1;
  min-height: 0;
}
</style>
