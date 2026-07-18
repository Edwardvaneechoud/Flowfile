<template>
  <div class="dp-tab-content artifacts-panel">
    <div v-if="summary.kernel_id" class="artifact-section-meta">
      Kernel: <code>{{ summary.kernel_id }}</code>
    </div>

    <div v-if="summary.published.length > 0" class="artifact-section">
      <div class="artifact-section-header">Published</div>
      <table class="artifact-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Module</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="art in summary.published" :key="art.name">
            <td class="artifact-name">{{ art.name }}</td>
            <td>{{ art.type_name || "-" }}</td>
            <td class="artifact-module">{{ art.module || "-" }}</td>
            <td class="artifact-action">
              <button
                v-if="art.has_preview"
                class="artifact-view-button"
                :disabled="loadingName === art.name"
                @click="viewPreview(art)"
              >
                {{ loadingName === art.name ? "Loading…" : "View" }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="summary.consumed.length > 0" class="artifact-section">
      <div class="artifact-section-header">Consumed</div>
      <table class="artifact-table">
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Source Node</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="art in summary.consumed" :key="art.name">
            <td class="artifact-name">{{ art.name }}</td>
            <td>{{ art.type_name || "-" }}</td>
            <td>{{ art.source_node_id != null ? `Node ${art.source_node_id}` : "-" }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="summary.deleted.length > 0" class="artifact-section">
      <div class="artifact-section-header">Deleted</div>
      <table class="artifact-table">
        <thead>
          <tr>
            <th>Name</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="name in summary.deleted" :key="name">
            <td class="artifact-name artifact-deleted">{{ name }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div
      v-if="
        summary.published.length === 0 &&
        summary.consumed.length === 0 &&
        summary.deleted.length === 0
      "
      class="artifact-empty"
    >
      No artifacts recorded for this node.
    </div>

    <ArtifactPreviewModal v-model="modalOpen" :payload="previewPayload" :name="previewName" />
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { ElMessage } from "element-plus";
import type { ArtifactPublished, DisplayOutput, NodeArtifactSummary } from "@/types";
import { KernelApi } from "../../api/kernel.api";
import ArtifactPreviewModal from "./ArtifactPreviewModal.vue";

interface Props {
  summary: NodeArtifactSummary;
  flowId: number;
}

const props = defineProps<Props>();

const loadingName = ref<string | null>(null);
const modalOpen = ref(false);
const previewPayload = ref<DisplayOutput | null>(null);
const previewName = ref("");

async function viewPreview(art: ArtifactPublished) {
  if (loadingName.value) return;
  loadingName.value = art.name;
  try {
    const payload = await KernelApi.getArtifactPreview(
      props.summary.kernel_id,
      props.flowId,
      art.name,
    );
    if (!payload) {
      ElMessage.warning(
        "Kernel stopped — preview no longer available. Re-run the node to regenerate it.",
      );
      return;
    }
    previewPayload.value = payload;
    previewName.value = art.name;
    modalOpen.value = true;
  } finally {
    loadingName.value = null;
  }
}
</script>

<style scoped>
.artifacts-panel {
  padding: 12px 16px;
  overflow-y: auto;
}

.artifact-section-meta {
  font-size: 11px;
  color: var(--color-text-secondary);
  margin-bottom: 12px;
}

.artifact-section-meta code {
  background: var(--color-background-tertiary, var(--color-background-secondary));
  padding: 1px 5px;
  border-radius: 3px;
  font-size: 11px;
}

.artifact-section {
  margin-bottom: 16px;
}

.artifact-section-header {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary);
  margin-bottom: 6px;
}

.artifact-table {
  width: 100%;
  /* Keep rows readable on wide docks — otherwise the Actions column drifts
     to the far right edge, hundreds of pixels from the row content. */
  max-width: 880px;
  border-collapse: collapse;
  font-size: 12px;
}

.artifact-table th {
  text-align: left;
  padding: 4px 8px;
  font-weight: 500;
  color: var(--color-text-secondary);
  border-bottom: 1px solid var(--color-border-primary);
  font-size: 11px;
}

.artifact-table td {
  padding: 5px 8px;
  border-bottom: 1px solid var(--color-border-light, var(--color-border-primary));
  color: var(--color-text-primary);
}

.artifact-table tbody tr:hover {
  background: var(--color-background-hover);
}

.artifact-name {
  font-weight: 500;
}

.artifact-module {
  font-size: 11px;
  color: var(--color-text-secondary);
}

.artifact-deleted {
  text-decoration: line-through;
  color: var(--color-text-secondary);
  opacity: 0.7;
}

.artifact-empty {
  color: var(--color-text-secondary);
  font-size: 13px;
  text-align: center;
  padding: 24px;
}

.artifact-action {
  text-align: right;
  width: 1%;
  white-space: nowrap;
}

.artifact-view-button {
  padding: 2px 10px;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  background: var(--color-background-primary);
  color: var(--color-text-secondary);
  font-size: 11px;
  cursor: pointer;
  transition: all 0.12s ease;
}

.artifact-view-button:hover:not(:disabled) {
  background: var(--color-accent, #6366f1);
  border-color: var(--color-accent, #6366f1);
  color: var(--color-text-inverse, #fff);
}

.artifact-view-button:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}
</style>
