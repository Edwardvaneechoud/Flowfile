<script setup lang="ts">
// W35 — Drawer-side wrapper that connects the renderer to the store.
//
// Mounted from `AiAssistant.vue` above the message list. Renders
// nothing while no diff is staged (so the chat surface remains clean
// for read-only conversation), and the diff preview otherwise. The
// "last applied" toast is a brief confirmation that the user can
// dismiss; it auto-clears when the next diff is staged.
//
// W49 — the optional rejection-note input now lives inside AiDiffPreview's
// sticky footer (replaces Accept/Reject in-place when the user clicks
// Reject). Keeps the confirmation in the same prominent location instead
// of pushing it down toward the composer, where the buttons used to
// visually collide with the composer's top border. AiDiffPreview emits
// `reject` with a `note` payload (string | null); we forward it to the
// store as before.

import { useAiDiffStore } from "../../stores/ai-diff-store";
import AiDiffPreview from "./AiDiffPreview.vue";

const aiDiffStore = useAiDiffStore();

const handleAccept = (): void => {
  void aiDiffStore.accept();
};

const handleReject = (note: string | null): void => {
  void aiDiffStore.reject(note);
};

const handleDismissToast = (): void => {
  aiDiffStore.lastApplyResult = null;
};

const handleDismissStaleNotice = (): void => {
  aiDiffStore.dismissStaleNotice();
};
</script>

<template>
  <div
    v-if="aiDiffStore.currentDiff || aiDiffStore.lastApplyResult || aiDiffStore.staleNotice"
    class="ai-diff-panel"
  >
    <AiDiffPreview
      v-if="aiDiffStore.currentDiff"
      :diff="aiDiffStore.currentDiff"
      :disabled="aiDiffStore.loading"
      :error="aiDiffStore.error"
      @accept="handleAccept"
      @reject="handleReject"
    />

    <div v-if="aiDiffStore.lastApplyResult" class="ai-diff-panel__toast" role="status">
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

    <!-- Surfaced when the backend 404s on accept/reject (e.g. flowfile_core
         was restarted and the on-disk diff sidecar didn't survive). The
         store auto-dismisses the notice after ~6s; this button lets the
         user clear it sooner. -->
    <div
      v-if="aiDiffStore.staleNotice"
      class="ai-diff-panel__toast ai-diff-panel__toast--stale"
      role="status"
    >
      <span class="ai-diff-panel__toast-icon">⚠</span>
      <span class="ai-diff-panel__toast-text">{{ aiDiffStore.staleNotice }}</span>
      <button
        type="button"
        class="ai-diff-panel__toast-dismiss ai-diff-panel__toast-dismiss--stale"
        aria-label="Dismiss"
        @click="handleDismissStaleNotice"
      >
        ×
      </button>
    </div>
  </div>
</template>

<style scoped>
.ai-diff-panel {
  margin-bottom: 8px;
  /* Flex-child rules so the panel can shrink inside .ai-assistant__chat
     when the drawer is short. Pairs with the bounded scroll +
     sticky-bottom footer on the inner .ai-diff-preview so a long
     change list never pushes the composer below the drawer fold. */
  flex-shrink: 1;
  min-height: 0;
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

/* Stale-diff notice — amber palette mirrors the drift error block in
   AiDiffPreview.vue, signalling "warning, action needed" rather than
   the success-green of the apply toast. */
.ai-diff-panel__toast--stale {
  background-color: var(--color-warning-light, #fff8e1);
  border-color: var(--color-warning, #b07b00);
  color: var(--color-warning, #b07b00);
}

.ai-diff-panel__toast-dismiss--stale {
  color: var(--color-warning, #b07b00);
}
</style>
