<script setup lang="ts">
// W35 — Drawer-side wrapper that connects the renderer to the store.
//
// Mounted from `AiAssistant.vue` above the message list. Renders
// nothing while no diff is staged (so the chat surface remains clean
// for read-only conversation), and the diff preview otherwise. The
// "last applied" toast is a brief confirmation that the user can
// dismiss; it auto-clears when the next diff is staged.

import { useAiDiffStore } from "../../stores/ai-diff-store";
import AiDiffPreview from "./AiDiffPreview.vue";

const aiDiffStore = useAiDiffStore();

const handleAccept = (): void => {
  void aiDiffStore.accept();
};

const handleReject = (): void => {
  void aiDiffStore.reject();
};

const handleDismissToast = (): void => {
  aiDiffStore.lastApplyResult = null;
};
</script>

<template>
  <div v-if="aiDiffStore.currentDiff || aiDiffStore.lastApplyResult" class="ai-diff-panel">
    <AiDiffPreview
      v-if="aiDiffStore.currentDiff"
      :diff="aiDiffStore.currentDiff"
      :disabled="aiDiffStore.loading"
      :error="aiDiffStore.error"
      @accept="handleAccept"
      @reject="handleReject"
    />

    <div v-else-if="aiDiffStore.lastApplyResult" class="ai-diff-panel__toast" role="status">
      <span class="ai-diff-panel__toast-icon">✓</span>
      <span class="ai-diff-panel__toast-text">
        Applied: {{ aiDiffStore.lastApplyResult.applied_node_ids.length }} node(s),
        {{ aiDiffStore.lastApplyResult.applied_connection_count }} connection(s).
      </span>
      <button
        type="button"
        class="ai-diff-panel__toast-dismiss"
        aria-label="Dismiss"
        @click="handleDismissToast"
      >
        ×
      </button>
    </div>
  </div>
</template>

<style scoped>
.ai-diff-panel {
  margin-bottom: 8px;
}

.ai-diff-panel__toast {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 6px;
  background-color: var(--color-success-light, #e6f7eb);
  border: 1px solid var(--color-success, #28a745);
  color: var(--color-success, #28a745);
  font-size: 12px;
}

.ai-diff-panel__toast-icon {
  font-weight: 700;
}

.ai-diff-panel__toast-text {
  flex: 1;
}

.ai-diff-panel__toast-dismiss {
  background: none;
  border: none;
  cursor: pointer;
  font-size: 16px;
  line-height: 1;
  color: var(--color-success, #28a745);
  padding: 0 4px;
}
</style>
