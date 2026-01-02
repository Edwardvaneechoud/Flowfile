<template>
  <!-- Node Browser Modal -->
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container modal-large" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">
          <i class="fa-solid fa-folder-open"></i>
          {{ viewingNodeCode ? viewingNodeName : 'Browse Custom Nodes' }}
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <!-- Viewing a specific node's code -->
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

        <!-- Node list -->
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
              @click="emit('viewNode', node.file_name)"
            >
              <div class="node-card-header">
                <i class="fa-solid fa-puzzle-piece"></i>
                <span class="node-name">{{ node.node_name || node.file_name }}</span>
              </div>
              <div class="node-card-body">
                <span class="node-category">{{ node.node_category }}</span>
                <p class="node-description">{{ node.intro || 'No description' }}</p>
              </div>
              <div class="node-card-footer">
                <span class="node-file">{{ node.file_name }}</span>
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
        <button v-if="viewingNodeCode" class="btn btn-danger" @click="emit('confirmDelete')">
          <i class="fa-solid fa-trash"></i>
          Delete
        </button>
        <button class="btn btn-secondary" @click="emit('close')">
          {{ viewingNodeCode ? 'Close' : 'Cancel' }}
        </button>
      </div>
    </div>
  </div>

  <!-- Delete Confirmation Modal -->
  <div v-if="showDeleteConfirm" class="modal-overlay" @click="emit('cancelDelete')">
    <div class="modal-container" @click.stop>
      <div class="modal-header modal-header-error">
        <h3 class="modal-title">
          <i class="fa-solid fa-triangle-exclamation"></i>
          Confirm Delete
        </h3>
        <button class="modal-close" @click="emit('cancelDelete')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <p>Are you sure you want to delete <strong>{{ viewingNodeName }}</strong>?</p>
        <p class="delete-warning">This action cannot be undone.</p>
      </div>
      <div class="modal-actions">
        <button class="btn btn-secondary" @click="emit('cancelDelete')">
          Cancel
        </button>
        <button class="btn btn-danger" @click="emit('delete')">
          <i class="fa-solid fa-trash"></i>
          Delete
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { Codemirror } from 'vue-codemirror';
import type { Extension } from '@codemirror/state';
import type { CustomNodeInfo } from './types';

defineProps<{
  show: boolean;
  nodes: CustomNodeInfo[];
  loading: boolean;
  viewingNodeCode: string;
  viewingNodeName: string;
  showDeleteConfirm: boolean;
  readOnlyExtensions: Extension[];
}>();

const emit = defineEmits<{
  (e: 'close'): void;
  (e: 'viewNode', fileName: string): void;
  (e: 'back'): void;
  (e: 'confirmDelete'): void;
  (e: 'cancelDelete'): void;
  (e: 'delete'): void;
}>();
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
  grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
  gap: 1rem;
}

.node-card {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s;
  overflow: hidden;
}

.node-card:hover {
  border-color: var(--primary-color);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
  transform: translateY(-2px);
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

.node-name {
  font-weight: 600;
  font-size: 0.9375rem;
}

.node-card-body {
  padding: 0.75rem 1rem;
}

.node-category {
  display: inline-block;
  font-size: 0.6875rem;
  font-weight: 500;
  text-transform: uppercase;
  padding: 0.125rem 0.5rem;
  background: var(--primary-color);
  color: white;
  border-radius: 3px;
  margin-bottom: 0.5rem;
}

.node-description {
  margin: 0;
  font-size: 0.8125rem;
  color: var(--text-secondary);
  line-height: 1.4;
}

.node-card-footer {
  padding: 0.5rem 1rem;
  border-top: 1px solid var(--border-color);
  background: var(--bg-secondary);
}

.node-file {
  font-size: 0.75rem;
  color: var(--text-secondary);
  font-family: 'Fira Code', 'Monaco', monospace;
}

.node-code-view {
  border-radius: 6px;
  overflow: hidden;
  border: 1px solid var(--border-color, #3a3a4a);
}

.modal-header-error {
  background: #fef2f2;
  border-bottom-color: #fecaca;
}

.modal-header-error .modal-title {
  color: #dc2626;
}

.modal-header-error .modal-title i {
  margin-right: 0.5rem;
}

.delete-warning {
  color: #dc2626;
  font-size: 0.875rem;
  margin-top: 0.5rem;
}

.btn-danger {
  background: #dc2626;
  color: white;
}

.btn-danger:hover:not(:disabled) {
  background: #b91c1c;
}
</style>
