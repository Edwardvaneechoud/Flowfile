<script setup lang="ts">
// W35 — Drawer-side wrapper that connects the renderer to the store.
//
// Mounted from `AiAssistant.vue` above the message list. Renders
// nothing while no diff is staged (so the chat surface remains clean
// for read-only conversation), and the diff preview otherwise. The
// "last applied" toast is a brief confirmation that the user can
// dismiss; it auto-clears when the next diff is staged.
//
// W49 — adds an optional rejection-note input that surfaces when the user
// is about to Reject. Empty note → server emits the generic "no specific
// reason" placeholder; filled note → it travels through to the agent's
// followup turn so the model can course-correct.

import { ref } from "vue";
import { useAiDiffStore } from "../../stores/ai-diff-store";
import AiDiffPreview from "./AiDiffPreview.vue";

const aiDiffStore = useAiDiffStore();

const showRejectionNote = ref(false);
const rejectionNote = ref("");

const handleAccept = (): void => {
  void aiDiffStore.accept();
};

const handleReject = (): void => {
  // W49 — first click reveals the note input; second click commits the
  // reject with whatever text the user typed (or empty for the generic
  // fallback). Keeps the legacy "click once → done" cadence reachable by
  // clicking the confirmation button again without typing anything.
  if (!showRejectionNote.value) {
    showRejectionNote.value = true;
    return;
  }
  const note = rejectionNote.value.trim();
  rejectionNote.value = "";
  showRejectionNote.value = false;
  void aiDiffStore.reject(note.length > 0 ? note : null);
};

const handleCancelReject = (): void => {
  showRejectionNote.value = false;
  rejectionNote.value = "";
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

    <div v-if="showRejectionNote" class="ai-diff-panel__rejection">
      <label class="ai-diff-panel__rejection-label" for="ai-diff-rejection-note">
        Tell the agent why you're rejecting (optional) — empty is fine.
      </label>
      <textarea
        id="ai-diff-rejection-note"
        v-model="rejectionNote"
        class="ai-diff-panel__rejection-input"
        rows="2"
        placeholder="e.g. use the read node directly, not after the filter"
      ></textarea>
      <div class="ai-diff-panel__rejection-actions">
        <button type="button" class="ai-diff-panel__rejection-cancel" @click="handleCancelReject">
          Cancel
        </button>
        <button
          type="button"
          class="ai-diff-panel__rejection-confirm"
          :disabled="aiDiffStore.loading"
          @click="handleReject"
        >
          Confirm reject
        </button>
      </div>
    </div>

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

.ai-diff-panel__rejection {
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 8px 10px;
  border-radius: 6px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-size: 12px;
}

.ai-diff-panel__rejection-label {
  color: var(--color-text-secondary, #586069);
}

.ai-diff-panel__rejection-input {
  width: 100%;
  resize: vertical;
  font-family: inherit;
  font-size: 12px;
  padding: 6px 8px;
  border-radius: 4px;
  border: 1px solid var(--color-border-primary, #e1e4e8);
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
}

.ai-diff-panel__rejection-actions {
  display: flex;
  justify-content: flex-end;
  gap: 6px;
}

.ai-diff-panel__rejection-cancel,
.ai-diff-panel__rejection-confirm {
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 12px;
  cursor: pointer;
  border: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-diff-panel__rejection-cancel {
  background-color: transparent;
  color: var(--color-text-primary, #24292e);
}

.ai-diff-panel__rejection-confirm {
  background-color: var(--color-danger, #d73a49);
  color: var(--color-text-inverse, #ffffff);
  border-color: var(--color-danger, #d73a49);
}

.ai-diff-panel__rejection-confirm:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
</style>
