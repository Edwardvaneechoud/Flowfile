<template>
  <div class="page-header">
    <div class="header-left">
      <h2 class="page-title">
        Node Designer
        <span v-if="nodeName" class="node-name-chip">{{ nodeName }}</span>
        <span v-if="isDirty" class="dirty-dot" title="Unsaved changes"></span>
      </h2>
      <p class="page-description">Design custom nodes visually</p>
    </div>
    <div class="header-actions">
      <HelpPopover />
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
import HelpPopover from "./HelpPopover.vue";

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
}

.page-title {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin: 0;
  font-size: 1.5rem;
  font-weight: 600;
}

.node-name-chip {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--color-text-secondary, #6b7280);
  background: var(--color-background-tertiary, #f1f3f5);
  border-radius: 999px;
  padding: 0.125rem 0.625rem;
}

.dirty-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-warning, #f59e0b);
  flex-shrink: 0;
}

.page-description {
  margin: 0.25rem 0 0 0;
  color: var(--text-secondary);
  font-size: 0.875rem;
}

.header-actions {
  display: flex;
  gap: 0.5rem;
}
</style>
