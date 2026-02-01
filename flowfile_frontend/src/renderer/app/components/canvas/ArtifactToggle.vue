<template>
  <div v-if="flowStore.hasArtifacts" class="artifact-toggle">
    <button
      class="toggle-btn"
      :class="{ active: flowStore.showArtifactEdges }"
      :title="
        flowStore.showArtifactEdges ? 'Hide artifact connections' : 'Show artifact connections'
      "
      @click="flowStore.toggleArtifactEdges()"
    >
      <svg
        width="16"
        height="16"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        stroke-width="2"
        stroke-linecap="round"
        stroke-linejoin="round"
      >
        <path d="M16 16v1a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h1" />
        <rect x="8" y="2" width="14" height="14" rx="2" ry="2" />
        <path
          v-if="flowStore.showArtifactEdges"
          d="M12 11l2 2 4-4"
          stroke-width="2"
        />
      </svg>
      <span class="toggle-label">Artifacts</span>
    </button>
  </div>
</template>

<script setup lang="ts">
import { useFlowStore } from "../../stores/flow-store";

const flowStore = useFlowStore();
</script>

<style scoped>
.artifact-toggle {
  position: absolute;
  top: 12px;
  left: 290px;
  display: flex;
  z-index: 1000;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  padding: 4px;
  box-shadow: var(--shadow-md);
}

.toggle-btn {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 4px 8px;
  background: transparent;
  border: none;
  border-radius: var(--border-radius-md);
  cursor: pointer;
  color: var(--color-text-secondary);
  font-size: 11px;
  font-family: var(--font-family-base);
  transition: all 0.15s ease;
  white-space: nowrap;
}

.toggle-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.toggle-btn.active {
  color: #8b5cf6;
}

.toggle-btn.active:hover {
  background: rgba(139, 92, 246, 0.1);
}

.toggle-label {
  font-weight: 500;
}
</style>
