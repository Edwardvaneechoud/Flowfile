<template>
  <div class="page-header">
    <div class="header-left">
      <span class="page-eyebrow">Node Designer</span>
      <h2 class="working-title">
        {{ nodeName || "Untitled node" }}
        <span v-if="isDirty" class="status-badge status-badge--warning">Unsaved</span>
      </h2>
      <p class="page-purpose">
        Build a reusable node for your palette — a settings form plus a Python transform.
      </p>
    </div>
    <div class="header-actions">
      <HelpButton />
      <button class="btn btn-secondary" data-testid="designer-browse" @click="emit('browse')">
        <i class="fa-solid fa-folder-open"></i>
        Browse
      </button>
      <button class="btn btn-secondary" data-testid="designer-new" @click="emit('new')">
        <i class="fa-solid fa-file"></i>
        New
      </button>
      <button class="btn btn-secondary" data-testid="designer-view-code" @click="emit('view-code')">
        <i class="fa-solid fa-code"></i>
        View code
      </button>
      <button
        class="btn btn-primary"
        data-testid="designer-save"
        :disabled="saving"
        @click="emit('save')"
      >
        <i class="fa-solid fa-save"></i>
        {{ saving ? "Saving…" : "Save" }}
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import HelpButton from "./HelpButton.vue";

withDefaults(
  defineProps<{
    nodeName: string;
    isDirty: boolean;
    saving?: boolean;
  }>(),
  { saving: false },
);

const emit = defineEmits<{
  (e: "browse"): void;
  (e: "new"): void;
  (e: "view-code"): void;
  (e: "save"): void;
}>();
</script>

<style scoped>
.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
  flex-shrink: 0;
}

.header-left {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

/* Small page-context label (where you are) above the node name (what you edit). */
.page-eyebrow {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-tertiary);
}

.working-title {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin: 0.125rem 0 0;
  min-width: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.page-purpose {
  margin: 0.25rem 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.header-actions {
  display: flex;
  gap: 0.5rem;
}
</style>
