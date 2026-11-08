<template>
  <div class="hub-flow-detail" data-testid="hub-flow-detail">
    <button class="back-btn" @click="$emit('close')">
      <i class="fa-solid fa-arrow-left"></i> Back
    </button>
    <div class="detail-header">
      <div class="header-main">
        <div class="header-name">
          <h2>{{ flow.name }}</h2>
          <span class="resource-badge" title="This flow is published on the hub">
            <i class="fa-solid fa-building"></i>
            Hub
          </span>
        </div>
        <p class="description" data-testid="hub-flow-home">{{ home }}</p>
      </div>
      <!-- On the hub itself Open makes no copy there: the flow is run from this panel (plan/PLAN-one-catalog.md) -->
      <div v-if="!onHub" class="header-actions">
        <button
          class="btn btn-primary btn-sm"
          :disabled="selectedVersion === null"
          data-testid="hub-flow-open"
          @click="open()"
        >
          <i class="fa-solid fa-up-right-from-square"></i>
          Open in Designer
        </button>
      </div>
    </div>

    <section class="versions-section">
      <h3>Versions</h3>
      <p v-if="loading" class="status">Loading versions...</p>
      <p v-else-if="error" class="status error" data-testid="hub-flow-error">{{ error }}</p>
      <p v-else-if="versions.length === 0" class="status">No published version.</p>
      <ul v-else class="versions" data-testid="hub-flow-versions">
        <li
          v-for="version in versions"
          :key="version.version"
          :class="{ selected: selectedVersion === version.version }"
          @click="selectedVersion = version.version"
          @dblclick="open()"
        >
          <span class="version-number">v{{ version.version }}</span>
          <span class="version-author">{{ authorLabel(version) }}</span>
          <span class="version-time">{{ publishedTime(version.published_at) }}</span>
          <span v-if="version.message?.trim()" class="version-message">{{ version.message }}</span>
        </li>
      </ul>
    </section>
    <p v-if="!onHub" class="note">
      Open makes your own copy of the version you pick, as Open Flow → Hub does; publish it back
      from the designer. The hub runs it and keeps its schedules.
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { listHubFlowVersions, type HubFlowVersion } from "../../api/hubFlows.api";
import { authorLabel, publishedTime } from "../../features/designer/hubFlows";
import { detailOf } from "../../utils/apiDetail";
import setupService from "../../services/setup.service";
import type { FlowRegistration } from "../../types";

// A flow published on the hub, selected in the catalog tree: its versions, and Open for the one picked. The copy is
// written by whoever listens to `open`, so this panel never navigates.
const props = defineProps<{
  flow: FlowRegistration;
  // The hub's name, as the tree says where the flow's catalog lives.
  place: string | null;
  // The flow's `catalog.schema` on the hub.
  schemaPath: string;
}>();

const emit = defineEmits<{
  (e: "close"): void;
  (e: "open", version: number): void;
}>();

const onHub = setupService.isOnHub();
const versions = ref<HubFlowVersion[]>([]);
const selectedVersion = ref<number | null>(null);
const loading = ref(false);
const error = ref("");

const home = computed(() => {
  const where = props.place ?? "the hub";
  return props.schemaPath ? `On ${where}, in ${props.schemaPath}` : `On ${where}`;
});

async function load(flowUuid: string | null | undefined) {
  versions.value = [];
  selectedVersion.value = null;
  error.value = "";
  if (!flowUuid) return;
  loading.value = true;
  try {
    versions.value = await listHubFlowVersions(flowUuid);
    selectedVersion.value = versions.value[0]?.version ?? null;
  } catch (err) {
    error.value = detailOf(err, "Couldn't list the flow's versions.");
  } finally {
    loading.value = false;
  }
}

watch(() => props.flow.flow_uuid, load, { immediate: true });

function open() {
  if (selectedVersion.value !== null) emit("open", selectedVersion.value);
}
</script>

<style scoped>
.hub-flow-detail {
  max-width: 1000px;
  margin: 0 auto;
}

.detail-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: var(--spacing-5);
}

.header-name {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.header-main h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.description {
  margin: var(--spacing-1) 0 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.header-actions {
  display: flex;
  gap: var(--spacing-2);
}

.versions-section h3 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.status {
  margin: 0;
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
}

.status.error {
  color: var(--color-danger);
}

.versions {
  list-style: none;
  margin: 0;
  padding: 0;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.versions li {
  display: flex;
  align-items: baseline;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.versions li + li {
  border-top: 1px solid var(--color-border-light);
}

.versions li:hover {
  background: var(--color-background-hover);
}

.versions li.selected {
  background: var(--color-primary-light, rgba(59, 130, 246, 0.1));
}

.version-number {
  font-family: var(--font-family-mono);
  font-weight: var(--font-weight-medium);
  min-width: 3ch;
}

.version-time,
.version-message {
  color: var(--color-text-muted);
}

.version-message {
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.note {
  margin: var(--spacing-4) 0 0;
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}
</style>
