<template>
  <div class="hub-flow-detail" data-testid="hub-flow-detail">
    <div class="detail-header">
      <div class="header-main">
        <p class="description" data-testid="hub-flow-home">{{ home }}</p>
      </div>
      <div class="header-actions">
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
    <p class="note">
      Open makes your own copy of the version you pick, as Open Flow does; publish it back from the
      designer. The hub runs it and keeps its schedules.
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { listHubFlowVersions, type HubFlowVersion } from "../../api/hubFlows.api";
import { authorLabel, publishedTime } from "../../features/designer/hubFlows";
import { detailOf } from "../../utils/apiDetail";
import { findNamespacePath, type FlowRegistration } from "../../types";
import { useCatalogStore } from "../../stores/catalog-store";
import { rootOf } from "./hubRows";

// A hub flow's page: where it lives, its versions, and Open for the one picked; it never navigates.
const props = defineProps<{ flow: FlowRegistration }>();

const emit = defineEmits<{ (e: "open", version: number): void }>();
const catalogStore = useCatalogStore();

const versions = ref<HubFlowVersion[]>([]);
const selectedVersion = ref<number | null>(null);
const loading = ref(false);
const error = ref("");

const home = computed(() => {
  const ns = props.flow.namespace_id;
  const where = (ns != null && rootOf(catalogStore.tree, ns)?.place) || "the hub";
  const schema = ns != null ? findNamespacePath(catalogStore.tree, ns).join(".") : "";
  return schema ? `On ${where}, in ${schema}` : `On ${where}`;
});

let loadSeq = 0;

async function load(flowUuid: string | null | undefined) {
  const seq = ++loadSeq;
  versions.value = [];
  selectedVersion.value = null;
  error.value = "";
  loading.value = Boolean(flowUuid);
  if (!flowUuid) return;
  try {
    const listed = await listHubFlowVersions(flowUuid);
    // The panel serves every hub flow picked, so an answer for a flow no longer shown is dropped.
    if (seq !== loadSeq) return;
    versions.value = listed;
    selectedVersion.value = listed[0]?.version ?? null;
  } catch (err) {
    if (seq === loadSeq) error.value = detailOf(err, "Couldn't list the flow's versions.");
  } finally {
    if (seq === loadSeq) loading.value = false;
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
