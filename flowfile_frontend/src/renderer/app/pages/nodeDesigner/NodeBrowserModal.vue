<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container modal-xl" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">
          <i class="fa-solid fa-folder-open"></i>
          {{ viewingNodeCode ? viewingNodeName : "Browse Custom Nodes" }}
          <span v-if="!viewingNodeCode && countLabel" class="modal-title-count">
            {{ countLabel }}
          </span>
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>

      <!-- Sibling of .modal-content so the search box never scrolls away. -->
      <div v-if="!viewingNodeCode && nodes.length" class="browser-toolbar">
        <el-input
          v-model="search"
          placeholder="Search nodes"
          clearable
          size="small"
          class="node-search"
          data-testid="node-browser-search"
        >
          <template #prefix><i class="fa-solid fa-magnifying-glass" /></template>
        </el-input>
      </div>

      <div class="modal-content" :class="{ 'modal-content--flush': !viewingNodeCode }">
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
          <EmptyState
            v-else-if="visibleNodes.length === 0"
            icon="fa-solid fa-magnifying-glass"
            title="No nodes match your search"
            :description="`Nothing found for “${search.trim()}”.`"
          >
            <template #actions>
              <button class="btn btn-secondary btn-sm" @click="search = ''">Clear search</button>
            </template>
          </EmptyState>
          <template v-else>
            <section v-for="group in groups" :key="group.key">
              <div class="node-group__header">
                <span>{{ group.label }}</span>
                <span>{{ group.nodes.length }}</span>
              </div>
              <NodeBrowserRow
                v-for="node in group.nodes"
                :key="node.file_name"
                :node="node"
                :is-current="node.file_name === currentFile"
                :community="communityNodes?.[node.file_name] ?? null"
                @edit="emit('edit', $event)"
                @duplicate="emit('duplicate', $event)"
                @view="emit('viewNode', $event)"
                @delete="askDelete(node.file_name, node.node_name || node.file_name)"
              />
            </section>
          </template>
        </template>
      </div>
      <div class="modal-actions">
        <button v-if="!viewingNodeCode" class="community-link" @click="browseCommunity">
          <i class="fa-solid fa-store"></i>
          Browse community nodes
          <i class="fa-solid fa-arrow-right community-link-arrow"></i>
        </button>
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
import { computed, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { Codemirror } from "vue-codemirror";
import { ElInput } from "element-plus";
import type { Extension } from "@codemirror/state";
import type { InstallReceipt } from "../../api/communityNodes";
import type { CustomNodeInfo } from "./types";
import { filterNodes, groupByCategory } from "./customNodeFilter";
import NodeBrowserRow from "./NodeBrowserRow.vue";
import EmptyState from "../../components/common/EmptyState/EmptyState.vue";

const router = useRouter();

const props = defineProps<{
  show: boolean;
  nodes: CustomNodeInfo[];
  loading: boolean;
  viewingNodeCode: string;
  viewingNodeName: string;
  readOnlyExtensions: Extension[];
  currentFile?: string | null;
  communityNodes?: Record<string, InstallReceipt>;
}>();

const emit = defineEmits<{
  (e: "close"): void;
  (e: "viewNode", fileName: string): void;
  (e: "edit", fileName: string): void;
  (e: "duplicate", fileName: string): void;
  (e: "back"): void;
  (e: "delete", fileName: string): void;
}>();

const search = ref("");

const visibleNodes = computed(() => filterNodes(props.nodes, search.value));
const groups = computed(() => groupByCategory(visibleNodes.value));

const countLabel = computed(() => {
  const total = props.nodes.length;
  if (!total) return "";
  const shown = visibleNodes.value.length;
  return shown === total ? `${total}` : `${shown} of ${total}`;
});

// The component never unmounts (v-if lives on the inner overlay), so a stale
// query would silently hide nodes on the next visit.
watch(
  () => props.show,
  (open) => {
    if (open) search.value = "";
  },
);

const pendingDelete = ref<{ fileName: string; name: string } | null>(null);

function askDelete(fileName: string, name: string) {
  pendingDelete.value = { fileName, name };
}

function confirmDelete() {
  if (pendingDelete.value) emit("delete", pendingDelete.value.fileName);
  pendingDelete.value = null;
}

function browseCommunity() {
  emit("close");
  router.push({ name: "catalog", query: { tab: "community" } });
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

.modal-title-count {
  margin-left: var(--spacing-2);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-normal);
  color: var(--color-text-tertiary);
}

.browser-toolbar {
  flex-shrink: 0;
  padding: var(--spacing-2) var(--spacing-4);
  border-bottom: 1px solid var(--color-border-primary);
}

/* Not `.search-input` — _forms.css defines a global one whose own border and
   background would double up on the el-input root. */
.node-search {
  max-width: 320px;
}

.modal-content--flush {
  padding: 0;
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

.community-link {
  margin-right: auto;
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
}

.community-link:hover {
  text-decoration: underline;
}

.community-link-arrow {
  font-size: var(--font-size-xs);
  transition: transform var(--transition-fast);
}

.community-link:hover .community-link-arrow {
  transform: translateX(2px);
}
</style>
