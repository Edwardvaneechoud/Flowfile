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
          <div v-else-if="nodes.length === 0" class="empty-nodes">
            <i class="fa-solid fa-folder-open"></i>
            <p>No custom nodes found</p>
            <p class="empty-hint">Save a node to see it here</p>
          </div>
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
                  class="card-action"
                  title="Edit"
                  :data-testid="`node-edit-${node.file_name}`"
                  @click="emit('edit', node.file_name)"
                >
                  <i class="fa-solid fa-pen"></i> Edit
                </button>
                <button
                  class="card-action"
                  title="Duplicate"
                  @click="emit('duplicate', node.file_name)"
                >
                  <i class="fa-solid fa-copy"></i> Duplicate
                </button>
                <button
                  class="card-action"
                  title="View code"
                  @click="emit('viewNode', node.file_name)"
                >
                  <i class="fa-solid fa-code"></i> Code
                </button>
                <button
                  class="card-action card-action-danger"
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
        <button class="btn btn-danger" @click="confirmDelete">
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
  color: var(--text-secondary);
}

.loading-indicator i {
  font-size: 1.25rem;
}

.empty-nodes {
  text-align: center;
  padding: 3rem;
  color: var(--text-secondary);
}

.empty-nodes i {
  font-size: 3rem;
  margin-bottom: 1rem;
  opacity: 0.5;
}

.empty-nodes p {
  margin: 0;
}

.empty-hint {
  font-size: 0.8125rem;
  margin-top: 0.5rem !important;
}

.nodes-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 1rem;
}

.node-card {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.node-card-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.75rem 1rem;
  background: var(--bg-secondary);
  border-bottom: 1px solid var(--border-color);
}

.node-card-header i {
  color: var(--primary-color);
}

.node-broken {
  margin-left: auto;
  color: var(--color-warning, #f59e0b);
}

.node-name {
  font-weight: 600;
  font-size: 0.9375rem;
}

.node-card-body {
  padding: 0.75rem 1rem;
  flex: 1;
}

.node-category {
  display: inline-block;
  font-size: 0.6875rem;
  font-weight: 500;
  text-transform: uppercase;
  padding: 0.125rem 0.5rem;
  background: var(--primary-color);
  color: var(--color-text-inverse);
  border-radius: 3px;
  margin-bottom: 0.5rem;
}

.node-description {
  margin: 0;
  font-size: 0.8125rem;
  color: var(--text-secondary);
  line-height: 1.4;
}

.node-card-actions {
  display: flex;
  gap: 0.25rem;
  padding: 0.5rem 0.75rem;
  border-top: 1px solid var(--border-color);
  background: var(--bg-secondary);
}

.card-action {
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--card-bg);
  color: var(--text-primary, #374151);
  cursor: pointer;
}

.card-action:hover {
  border-color: var(--primary-color);
  color: var(--primary-color);
}

.card-action-danger {
  margin-left: auto;
}

.card-action-danger:hover {
  border-color: var(--color-danger, #ef4444);
  color: var(--color-danger, #ef4444);
}

.node-code-view {
  border-radius: 6px;
  overflow: hidden;
  border: 1px solid var(--border-color, #3a3a4a);
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
  font-size: 0.875rem;
  margin-top: 0.5rem;
}

.btn-danger {
  background: var(--color-danger-hover);
  color: var(--color-text-inverse);
}

.btn-danger:hover:not(:disabled) {
  background: var(--color-danger-dark);
}
</style>
