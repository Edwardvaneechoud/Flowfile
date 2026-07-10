<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container modal-large" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">
          <i class="fa-solid fa-folder-open"></i>
          {{ viewingNodeCode ? viewingNodeName : "Browse Custom Nodes" }}
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <template v-if="viewingNodeCode">
          <div class="node-code-view">
            <Codemirror
              :model-value="viewingNodeCode"
              :style="{ height: 'auto', maxHeight: 'calc(80vh - 180px)' }"
              :autofocus="false"
              :indent-with-tab="false"
              :tab-size="4"
              :extensions="readOnlyExtensions"
            />
          </div>
        </template>

        <template v-else>
          <div v-if="loading" class="loading-indicator">
            <i class="fa-solid fa-spinner fa-spin"></i>
            Loading custom nodes...
          </div>
          <EmptyState
            v-else-if="nodes.length === 0"
            icon="fa-solid fa-folder-open"
            title="No custom nodes found"
            description="Save a node to see it here."
          />
          <div v-else class="nodes-grid">
            <div
              v-for="node in nodes"
              :key="node.file_name"
              class="node-card"
              :data-testid="`node-card-${node.file_name}`"
            >
              <div class="node-card-header">
                <i class="fa-solid fa-puzzle-piece"></i>
                <span class="node-name">{{ node.node_name || node.file_name }}</span>
                <span v-if="node.error" class="node-broken" title="Failed to load">
                  <i class="fa-solid fa-triangle-exclamation"></i>
                </span>
              </div>
              <div class="node-card-body">
                <span class="node-category">{{ node.node_category }}</span>
                <p class="node-description">{{ node.intro || "No description" }}</p>
              </div>
              <div class="node-card-actions">
                <button
                  class="btn btn-sm"
                  title="Edit"
                  :data-testid="`node-edit-${node.file_name}`"
                  @click="emit('edit', node.file_name)"
                >
                  <i class="fa-solid fa-pen"></i> Edit
                </button>
                <button
                  class="btn btn-sm"
                  title="Duplicate"
                  @click="emit('duplicate', node.file_name)"
                >
                  <i class="fa-solid fa-copy"></i> Duplicate
                </button>
                <button
                  class="btn btn-sm"
                  title="View code"
                  @click="emit('viewNode', node.file_name)"
                >
                  <i class="fa-solid fa-code"></i> Code
                </button>
                <button
                  class="btn btn-sm btn-icon btn-danger card-action-danger"
                  title="Delete"
                  @click="askDelete(node.file_name, node.node_name || node.file_name)"
                >
                  <i class="fa-solid fa-trash"></i>
                </button>
              </div>
            </div>
          </div>
        </template>
      </div>
      <div class="modal-actions">
        <button v-if="viewingNodeCode" class="btn btn-secondary" @click="emit('back')">
          <i class="fa-solid fa-arrow-left"></i>
          Back
        </button>
        <button class="btn btn-secondary" @click="emit('close')">
          {{ viewingNodeCode ? "Close" : "Cancel" }}
        </button>
      </div>
    </div>
  </div>

  <div v-if="pendingDelete" class="modal-overlay" @click="pendingDelete = null">
    <div class="modal-container" @click.stop>
      <div class="modal-header modal-header-error">
        <h3 class="modal-title">
          <i class="fa-solid fa-triangle-exclamation"></i>
          Confirm Delete
        </h3>
        <button class="modal-close" @click="pendingDelete = null">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <p>
          Are you sure you want to delete <strong>{{ pendingDelete.name }}</strong
          >?
        </p>
        <p class="delete-warning">This action cannot be undone.</p>
      </div>
      <div class="modal-actions">
        <button class="btn btn-secondary" @click="pendingDelete = null">Cancel</button>
        <button class="btn btn-danger-filled" @click="confirmDelete">
          <i class="fa-solid fa-trash"></i>
          Delete
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { Codemirror } from "vue-codemirror";
import type { Extension } from "@codemirror/state";
import type { CustomNodeInfo } from "./types";
import EmptyState from "../../components/common/EmptyState/EmptyState.vue";

defineProps<{
  show: boolean;
  nodes: CustomNodeInfo[];
  loading: boolean;
  viewingNodeCode: string;
  viewingNodeName: string;
  readOnlyExtensions: Extension[];
}>();

const emit = defineEmits<{
  (e: "close"): void;
  (e: "viewNode", fileName: string): void;
  (e: "edit", fileName: string): void;
  (e: "duplicate", fileName: string): void;
  (e: "back"): void;
  (e: "delete", fileName: string): void;
}>();

const pendingDelete = ref<{ fileName: string; name: string } | null>(null);

function askDelete(fileName: string, name: string) {
  pendingDelete.value = { fileName, name };
}

function confirmDelete() {
  if (pendingDelete.value) emit("delete", pendingDelete.value.fileName);
  pendingDelete.value = null;
}
</script>

<style scoped>
.loading-indicator {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
  padding: 2rem;
  color: var(--color-text-secondary);
}

.loading-indicator i {
  font-size: 1.25rem;
}

.nodes-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 1rem;
}

.node-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.node-card-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.75rem 1rem;
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.node-card-header i {
  color: var(--color-accent);
}

.node-broken {
  margin-left: auto;
  color: var(--color-warning);
}

.node-name {
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-base);
}

.node-card-body {
  padding: 0.75rem 1rem;
  flex: 1;
}

.node-category {
  display: inline-block;
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 0.125rem 0.5rem;
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-full);
  margin-bottom: 0.5rem;
}

.node-description {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.node-card-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.25rem;
  padding: 0.5rem 0.75rem;
  border-top: 1px solid var(--color-border-primary);
  background: var(--color-background-secondary);
}

.card-action-danger {
  margin-left: auto;
}

.node-code-view {
  border-radius: var(--border-radius-md);
  overflow: hidden;
  border: 1px solid var(--color-border-primary);
}

.modal-header-error {
  background: var(--color-danger-light);
  border-bottom-color: var(--color-danger);
}

.modal-header-error .modal-title {
  color: var(--color-danger-hover);
}

.modal-header-error .modal-title i {
  margin-right: 0.5rem;
}

.delete-warning {
  color: var(--color-danger-hover);
  font-size: var(--font-size-base);
  margin-top: 0.5rem;
}
</style>
