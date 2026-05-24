<template>
  <div class="notebook-view">
    <!-- Sidebar: saved notebooks -->
    <aside class="notebook-sidebar">
      <div class="sidebar-header">
        <span>Notebooks</span>
        <button class="icon-btn" title="New notebook" @click="onCreate">
          <i class="fa-solid fa-plus"></i>
        </button>
      </div>
      <div v-if="store.loading && store.notebooks.length === 0" class="sidebar-hint">Loading…</div>
      <div v-else-if="store.notebooks.length === 0" class="sidebar-hint">
        No notebooks yet. Create one to start writing code against
        <code>flowfile_ctx</code>.
      </div>
      <ul v-else class="notebook-list">
        <li
          v-for="nb in store.notebooks"
          :key="nb.id"
          class="notebook-item"
          :class="{ active: store.active?.id === nb.id }"
          @click="onOpen(nb.id)"
        >
          <span class="notebook-name">{{ nb.name }}</span>
          <button class="icon-btn danger" title="Delete" @click.stop="onDelete(nb.id)">
            <i class="fa-solid fa-trash"></i>
          </button>
        </li>
      </ul>
    </aside>

    <!-- Main area -->
    <section class="notebook-main">
      <template v-if="store.active">
        <header class="notebook-header">
          <input
            v-model="nameDraft"
            class="notebook-title-input"
            title="Rename notebook"
            @change="onRename"
          />
          <div class="header-spacer"></div>

          <label class="kernel-label">Kernel</label>
          <KernelSelect
            class="header-kernel-select"
            :model-value="store.selectedKernelId"
            :kernels="kernels"
            @change="onKernelChange"
          />
          <button
            v-if="store.selectedKernelId && !kernelReady"
            class="start-btn"
            title="Start the selected kernel"
            @click="onStartKernel"
          >
            <i class="fa-solid fa-play"></i> Start
          </button>
          <span class="save-state">{{ store.saving ? "Saving…" : "Saved" }}</span>
        </header>

        <div v-if="!kernelReady" class="kernel-warning">
          <i class="fa-solid fa-circle-info"></i>
          Select and start a kernel to run cells. Code uses <code>flowfile_ctx</code> and
          <code>pl</code> (Polars).
        </div>

        <div class="notebook-editor-host">
          <NotebookEditor
            :key="store.active.id"
            :cells="editorCells"
            :kernel-id="kernelReady ? store.selectedKernelId : null"
            :flow-id="store.active.flow_id"
            :node-id="0"
            :depending-on-ids="[]"
            @update:cells="onCellsUpdate"
          />
        </div>
      </template>

      <div v-else class="empty-state">
        <i class="fa-solid fa-book-open empty-icon"></i>
        <p>Select or create a notebook to begin.</p>
      </div>
    </section>
  </div>
</template>

<script lang="ts" setup>
import { computed, onMounted, ref, watch } from "vue";

import type { NotebookCell as EditorCell } from "../../types/node.types";
import { useNotebookStore } from "../../stores/notebook-store";
import { useKernelManager } from "../KernelManagerView/useKernelManager";
import KernelSelect from "../../components/kernel/KernelSelect.vue";
import NotebookEditor from "../../components/nodes/node-types/elements/pythonScript/NotebookEditor.vue";

const store = useNotebookStore();
// Reuse the shared kernel composable (list + polling + start) rather than
// re-implementing kernel loading in the notebook store.
const { kernels, startKernel } = useKernelManager();
const nameDraft = ref("");

// The reused NotebookEditor speaks the in-node cell shape ({ id, code }); the
// stored notebook uses the .ipynb-friendly { id, source }. Map between them.
const editorCells = computed<EditorCell[]>(() =>
  store.activeCells.map((c) => ({ id: c.id, code: c.source, output: null })),
);

const selectedKernel = computed(
  () => kernels.value.find((k) => k.id === store.selectedKernelId) ?? null,
);
const kernelReady = computed(() => {
  const k = selectedKernel.value;
  return !!k && (k.state === "idle" || k.state === "executing");
});

watch(
  () => store.active?.name,
  (name) => {
    nameDraft.value = name ?? "";
  },
  { immediate: true },
);

// Default the kernel selection (in-memory only) when opening a notebook that
// hasn't bound one yet — explicit changes are persisted via selectKernel().
watch([() => store.active?.id, kernels], () => {
  if (store.active && !store.selectedKernelId && kernels.value.length) {
    store.selectedKernelId =
      kernels.value.find((k) => k.state === "idle")?.id ?? kernels.value[0].id;
  }
});

onMounted(() => {
  void store.loadNotebooks();
});

const onCellsUpdate = (cells: EditorCell[]) => {
  store.persistCells(cells.map((c) => ({ id: c.id, source: c.code })));
};

const onOpen = (id: string) => store.openNotebook(id);

const onCreate = async () => {
  await store.createNotebook("Untitled notebook");
};

const onDelete = async (id: string) => {
  await store.deleteNotebook(id);
};

const onRename = () => {
  if (nameDraft.value.trim()) void store.renameActive(nameDraft.value.trim());
};

const onKernelChange = (id: string | null) => {
  void store.selectKernel(id);
};

const onStartKernel = () => {
  if (store.selectedKernelId) void startKernel(store.selectedKernelId);
};
</script>

<style scoped>
.notebook-view {
  display: flex;
  height: 100%;
  min-height: 0;
  background: var(--el-bg-color);
}

.notebook-sidebar {
  width: 240px;
  flex-shrink: 0;
  border-right: 1px solid var(--el-border-color);
  display: flex;
  flex-direction: column;
  overflow-y: auto;
}

.sidebar-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.6rem 0.75rem;
  font-weight: 600;
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.sidebar-hint {
  padding: 0.75rem;
  font-size: 0.8rem;
  color: var(--el-text-color-secondary);
}

.notebook-list {
  list-style: none;
  margin: 0;
  padding: 0;
}

.notebook-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.45rem 0.75rem;
  cursor: pointer;
  font-size: 0.85rem;
  border-left: 3px solid transparent;
}

.notebook-item:hover {
  background: var(--el-fill-color);
}

.notebook-item.active {
  background: var(--el-color-primary-light-9);
  border-left-color: var(--el-color-primary);
}

.notebook-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.notebook-main {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  overflow-y: auto;
}

.notebook-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.notebook-title-input {
  font-size: 0.95rem;
  font-weight: 600;
  border: 1px solid transparent;
  background: transparent;
  padding: 0.2rem 0.4rem;
  border-radius: 3px;
  color: var(--el-text-color-primary);
  min-width: 12rem;
}

.notebook-title-input:hover,
.notebook-title-input:focus {
  border-color: var(--el-border-color);
  outline: none;
}

.header-spacer {
  flex: 1;
}

.kernel-label {
  font-size: 0.75rem;
  color: var(--el-text-color-secondary);
}

.header-kernel-select {
  min-width: 16rem;
}

.start-btn {
  font-size: 0.75rem;
  padding: 0.2rem 0.5rem;
  border: 1px solid var(--el-border-color);
  border-radius: 3px;
  background: var(--el-bg-color);
  cursor: pointer;
  color: var(--el-text-color-regular);
}

.save-state {
  font-size: 0.7rem;
  color: var(--el-text-color-secondary);
  min-width: 3.5rem;
  text-align: right;
}

.kernel-warning {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  font-size: 0.78rem;
  color: var(--el-text-color-secondary);
  background: var(--el-fill-color-lighter);
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.notebook-editor-host {
  flex: 1;
  min-height: 0;
  padding: 0.5rem 0.75rem;
}

.icon-btn {
  background: none;
  border: none;
  cursor: pointer;
  color: var(--el-text-color-secondary);
  padding: 0.2rem 0.35rem;
  border-radius: 3px;
}

.icon-btn:hover {
  background: var(--el-fill-color);
  color: var(--el-text-color-primary);
}

.icon-btn.danger:hover {
  color: var(--el-color-danger);
}

.empty-state {
  margin: auto;
  text-align: center;
  color: var(--el-text-color-secondary);
}

.empty-icon {
  font-size: 2.5rem;
  margin-bottom: 0.75rem;
  opacity: 0.5;
}
</style>
