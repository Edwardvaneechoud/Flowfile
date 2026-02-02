<template>
  <div v-if="hasArtifacts" class="kernel-artifacts">
    <div class="kernel-artifacts__header" @click="expanded = !expanded">
      <span class="kernel-artifacts__label">
        <i class="fa-solid fa-cube"></i>
        Artifacts ({{ artifactCount }})
      </span>
      <span v-if="persistenceInfo.disk_usage_bytes > 0" class="kernel-artifacts__disk">
        {{ formatBytes(persistenceInfo.disk_usage_bytes) }}
      </span>
      <i
        class="fa-solid kernel-artifacts__chevron"
        :class="expanded ? 'fa-chevron-up' : 'fa-chevron-down'"
      ></i>
    </div>

    <div v-if="expanded" class="kernel-artifacts__list">
      <div
        v-for="(artifact, name) in persistenceInfo.artifacts"
        :key="name"
        class="kernel-artifacts__item"
      >
        <span class="kernel-artifacts__name" :title="String(name)">{{ name }}</span>
        <span class="kernel-artifacts__type">{{ artifact.type_name || "unknown" }}</span>
        <span
          class="kernel-artifacts__status"
          :class="statusClass(artifact)"
          :title="statusTitle(artifact)"
        >
          {{ statusLabel(artifact) }}
        </span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted } from "vue";
import { KernelApi } from "../../api/kernel.api";
import type { PersistenceInfo, ArtifactPersistenceStatus } from "../../types";

const props = defineProps<{
  kernelId: string;
  kernelState: string;
}>();

const expanded = ref(false);

const persistenceInfo = ref<PersistenceInfo>({
  persistence_enabled: false,
  total_artifacts: 0,
  persisted_count: 0,
  memory_only_count: 0,
  disk_usage_bytes: 0,
  artifacts: {},
});

const hasArtifacts = computed(() => persistenceInfo.value.total_artifacts > 0);
const artifactCount = computed(() => persistenceInfo.value.total_artifacts);

const isRunning = computed(
  () => props.kernelState === "idle" || props.kernelState === "executing",
);

const fetchPersistence = async () => {
  if (!isRunning.value) return;
  persistenceInfo.value = await KernelApi.getPersistenceInfo(props.kernelId);
};

onMounted(fetchPersistence);

watch(() => props.kernelState, (newState, oldState) => {
  if ((newState === "idle" || newState === "executing") && oldState !== newState) {
    fetchPersistence();
  }
});

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function statusClass(artifact: ArtifactPersistenceStatus): string {
  if (artifact.persisted && artifact.in_memory) return "kernel-artifacts__status--synced";
  if (artifact.persisted) return "kernel-artifacts__status--disk";
  return "kernel-artifacts__status--memory";
}

function statusLabel(artifact: ArtifactPersistenceStatus): string {
  if (artifact.persisted && artifact.in_memory) return "synced";
  if (artifact.persisted) return "disk only";
  return "memory only";
}

function statusTitle(artifact: ArtifactPersistenceStatus): string {
  if (artifact.persisted && artifact.in_memory) return "Persisted to disk and loaded in memory";
  if (artifact.persisted) return "Persisted to disk, not yet loaded in memory";
  return "In memory only, not persisted to disk";
}
</script>

<style scoped>
.kernel-artifacts {
  border-top: 1px solid var(--color-border-light);
  padding-top: var(--spacing-2);
}

.kernel-artifacts__header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  cursor: pointer;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  user-select: none;
}

.kernel-artifacts__header:hover {
  color: var(--color-text-primary);
}

.kernel-artifacts__label {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  font-weight: var(--font-weight-medium);
}

.kernel-artifacts__label i {
  font-size: var(--font-size-2xs);
  color: var(--color-text-muted);
}

.kernel-artifacts__disk {
  font-size: var(--font-size-2xs);
  color: var(--color-text-muted);
  font-family: var(--font-family-mono);
}

.kernel-artifacts__chevron {
  margin-left: auto;
  font-size: var(--font-size-2xs);
  color: var(--color-text-muted);
}

.kernel-artifacts__list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
  margin-top: var(--spacing-2);
}

.kernel-artifacts__item {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-2xs);
  padding: var(--spacing-1) var(--spacing-1-5);
  background-color: var(--color-background-secondary);
  border-radius: var(--border-radius-sm);
}

.kernel-artifacts__name {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  min-width: 0;
  flex: 1;
}

.kernel-artifacts__type {
  color: var(--color-text-muted);
  font-family: var(--font-family-mono);
  flex-shrink: 0;
}

.kernel-artifacts__status {
  flex-shrink: 0;
  padding: 1px var(--spacing-1);
  border-radius: var(--border-radius-sm);
  font-weight: var(--font-weight-medium);
}

.kernel-artifacts__status--synced {
  background-color: var(--color-success-light, #e6f9ee);
  color: var(--color-success, #22c55e);
}

.kernel-artifacts__status--disk {
  background-color: var(--color-accent-subtle);
  color: var(--color-accent);
}

.kernel-artifacts__status--memory {
  background-color: var(--color-warning-light, #fff8e6);
  color: var(--color-warning-dark, #b45309);
}
</style>
