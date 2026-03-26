<template>
  <div class="tree-node">
    <div class="tree-row" :class="{ expanded }" @click="toggle">
      <i
        :class="expanded ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"
        class="chevron"
      ></i>
      <i
        :class="node.level === 0 ? 'fa-solid fa-box-archive' : 'fa-solid fa-layer-group'"
        class="ns-icon"
      ></i>
      <span class="ns-name">{{ node.name }}</span>
      <span v-if="totalFlows > 0" class="ns-count">{{ totalFlows }}</span>
      <div class="tree-actions" @click.stop>
        <button
          v-if="node.level === 0"
          class="action-btn"
          title="Add schema"
          @click="$emit('createSchema', node.id)"
        >
          <i class="fa-solid fa-plus"></i>
        </button>
        <button
          v-if="node.level === 1"
          class="action-btn"
          title="Register table"
          @click="$emit('registerTable', node.id)"
        >
          <i class="fa-solid fa-table"></i>
        </button>
        <button
          v-if="node.level === 1"
          class="action-btn"
          title="Register flow"
          @click="$emit('registerFlow', node.id)"
        >
          <i class="fa-solid fa-file-circle-plus"></i>
        </button>
      </div>
    </div>

    <div v-if="expanded" class="tree-children">
      <!-- Child namespaces (schemas) -->
      <div v-for="child in node.children" :key="child.id" class="tree-child-wrapper">
        <CatalogTreeNode
          :node="child"
          :selected-flow-id="selectedFlowId"
          :selected-artifact-id="selectedArtifactId"
          :selected-table-id="selectedTableId"
          :search-query="searchQuery"
          :show-unavailable="showUnavailable"
          @select-flow="$emit('selectFlow', $event)"
          @select-artifact="$emit('selectArtifact', $event)"
          @select-table="$emit('selectTable', $event)"
          @toggle-favorite="$emit('toggleFavorite', $event)"
          @toggle-table-favorite="$emit('toggleTableFavorite', $event)"
          @register-flow="$emit('registerFlow', $event)"
          @register-table="$emit('registerTable', $event)"
          @create-schema="$emit('createSchema', $event)"
        />
      </div>

      <div v-if="showFlowsSection" class="tree-section">
        <button class="section-header" @click.stop="toggleFlows">
          <i
            :class="flowsExpanded ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"
            class="section-chevron"
          ></i>
          <span class="section-title">Flows</span>
          <span class="section-count">{{ visibleFlows.length }}</span>
        </button>
        <div v-if="flowsExpanded" class="section-content">
          <div
            v-for="flow in visibleFlows"
            :key="'f-' + flow.id"
            class="tree-flow"
            :class="{ selected: selectedFlowId === flow.id, 'file-missing': !flow.file_exists }"
            @click.stop="$emit('selectFlow', flow.id)"
          >
            <i class="fa-solid fa-diagram-project flow-icon"></i>
            <span class="flow-name">{{ flow.name }}</span>
            <i
              v-if="!flow.file_exists"
              class="fa-solid fa-triangle-exclamation missing-icon"
              title="Flow file not found on disk"
            ></i>
            <div class="flow-actions" @click.stop>
              <button
                class="action-btn star-btn"
                :class="{ active: flow.is_favorite }"
                :title="flow.is_favorite ? 'Unfavorite' : 'Favorite'"
                @click="$emit('toggleFavorite', flow.id)"
              >
                <i :class="flow.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
              </button>
              <span
                v-if="flow.last_run_success !== null"
                class="run-indicator"
                :class="flow.last_run_success ? 'success' : 'failure'"
                :title="flow.last_run_success ? 'Last run succeeded' : 'Last run failed'"
              ></span>
            </div>
          </div>
        </div>
      </div>

      <div v-if="showModelsSection" class="tree-section">
        <button class="section-header" @click.stop="toggleModels">
          <i
            :class="modelsExpanded ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"
            class="section-chevron"
          ></i>
          <span class="section-title">Models</span>
          <span class="section-count">{{ visibleArtifacts.length }}</span>
        </button>
        <div v-if="modelsExpanded" class="section-content">
          <div
            v-for="group in visibleArtifacts"
            :key="'ag-' + group.name"
            class="tree-artifact"
            :class="{ selected: selectedArtifactId === group.latest.id }"
            @click.stop="$emit('selectArtifact', group.latest.id)"
          >
            <i class="fa-solid fa-cube artifact-icon"></i>
            <span class="artifact-name">{{ group.name }}</span>
            <span v-if="group.versionCount > 1" class="artifact-versions-count"
              >{{ group.versionCount }} versions</span
            >
            <span class="artifact-version">v{{ group.latest.version }}</span>
          </div>
        </div>
      </div>

      <div v-if="showTablesSection" class="tree-section">
        <button class="section-header" @click.stop="toggleTables">
          <i
            :class="tablesExpanded ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"
            class="section-chevron"
          ></i>
          <span class="section-title">Tables</span>
          <span class="section-count">{{ visibleTables.length }}</span>
        </button>
        <div v-if="tablesExpanded" class="section-content">
          <div
            v-for="table in visibleTables"
            :key="'t-' + table.id"
            class="tree-table"
            :class="{
              selected: selectedTableId === table.id,
              'file-missing': table.file_exists === false,
            }"
            @click.stop="$emit('selectTable', table.id)"
          >
            <i class="fa-solid fa-table table-icon"></i>
            <span class="table-name">{{ table.name }}</span>
            <span v-if="table.row_count !== null" class="table-rows">
              {{ formatRowCount(table.row_count) }} rows
            </span>
            <div class="flow-actions" @click.stop>
              <button
                class="action-btn star-btn"
                :class="{ active: table.is_favorite }"
                :title="table.is_favorite ? 'Unfavorite' : 'Favorite'"
                @click="$emit('toggleTableFavorite', table.id)"
              >
                <i :class="table.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import type { GlobalArtifact, NamespaceTree } from "../../types";

interface ArtifactGroup {
  name: string;
  latest: GlobalArtifact;
  versionCount: number;
}

const props = withDefaults(
  defineProps<{
    node: NamespaceTree;
    selectedFlowId: number | null;
    selectedArtifactId: number | null;
    selectedTableId: number | null;
    searchQuery?: string;
    showUnavailable?: boolean;
  }>(),
  {
    searchQuery: "",
    showUnavailable: false,
  },
);

defineEmits([
  "selectFlow",
  "selectArtifact",
  "selectTable",
  "toggleFavorite",
  "toggleTableFavorite",
  "registerFlow",
  "registerTable",
  "createSchema",
]);

function formatRowCount(n: number | null): string {
  if (n === null) return "0";
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

function containsFlow(node: NamespaceTree, flowId: number): boolean {
  if (node.flows.some((f) => f.id === flowId)) return true;
  return node.children.some((child) => containsFlow(child, flowId));
}

function containsArtifact(node: NamespaceTree, artifactId: number): boolean {
  if ((node.artifacts ?? []).some((a) => a.id === artifactId)) return true;
  return node.children.some((child) => containsArtifact(child, artifactId));
}

function containsTable(node: NamespaceTree, tableId: number): boolean {
  if ((node.tables ?? []).some((t) => t.id === tableId)) return true;
  return node.children.some((child) => containsTable(child, tableId));
}

const query = computed(() => props.searchQuery.toLowerCase());

const visibleFlows = computed(() => {
  let flows = props.node.flows;
  if (!props.showUnavailable) {
    flows = flows.filter((f) => f.file_exists);
  }
  if (query.value) {
    flows = flows.filter((f) => f.name.toLowerCase().includes(query.value));
  }
  return flows;
});

const visibleTables = computed(() => {
  let tables = props.node.tables ?? [];
  if (!props.showUnavailable) {
    tables = tables.filter((t) => (t as any).file_exists !== false);
  }
  if (query.value) {
    tables = tables.filter((t) => t.name.toLowerCase().includes(query.value));
  }
  return tables;
});

const groupedArtifacts = computed((): ArtifactGroup[] => {
  const byName = new Map<string, GlobalArtifact[]>();
  for (const a of props.node.artifacts ?? []) {
    const list = byName.get(a.name) ?? [];
    list.push(a);
    byName.set(a.name, list);
  }
  return [...byName.entries()].map(([name, versions]) => {
    const sorted = [...versions].sort((a, b) => b.version - a.version);
    return { name, latest: sorted[0], versionCount: versions.length };
  });
});

const visibleArtifacts = computed((): ArtifactGroup[] => {
  let groups = groupedArtifacts.value;
  if (query.value) {
    groups = groups.filter((g) => g.name.toLowerCase().includes(query.value));
  }
  return groups;
});

const expanded = ref(true);
const toggle = () => {
  expanded.value = !expanded.value;
};

const flowsExpanded = ref(false);
const modelsExpanded = ref(false);
const tablesExpanded = ref(false);

const showFlowsSection = computed(() => props.node.level === 1 && visibleFlows.value.length > 0);
const showModelsSection = computed(
  () => props.node.level === 1 && visibleArtifacts.value.length > 0,
);
const showTablesSection = computed(() => props.node.level === 1 && visibleTables.value.length > 0);

const toggleFlows = () => {
  flowsExpanded.value = !flowsExpanded.value;
};
const toggleModels = () => {
  modelsExpanded.value = !modelsExpanded.value;
};
const toggleTables = () => {
  tablesExpanded.value = !tablesExpanded.value;
};

watch(
  () => props.selectedFlowId,
  (flowId) => {
    if (flowId !== null && containsFlow(props.node, flowId)) {
      expanded.value = true;
      flowsExpanded.value = true;
    }
  },
);

watch(
  () => props.selectedArtifactId,
  (artifactId) => {
    if (artifactId !== null && containsArtifact(props.node, artifactId)) {
      expanded.value = true;
      modelsExpanded.value = true;
    }
  },
);

watch(
  () => props.selectedTableId,
  (tableId) => {
    if (tableId !== null && containsTable(props.node, tableId)) {
      expanded.value = true;
      tablesExpanded.value = true;
    }
  },
);

watch(query, (value) => {
  if (!value) return;
  if (visibleFlows.value.length > 0) flowsExpanded.value = true;
  if (visibleArtifacts.value.length > 0) modelsExpanded.value = true;
  if (visibleTables.value.length > 0) tablesExpanded.value = true;
});

watch(
  () => props.node.id,
  () => {
    flowsExpanded.value = false;
    modelsExpanded.value = false;
    tablesExpanded.value = false;
  },
);

function countUniqueArtifactNames(artifacts: GlobalArtifact[]): number {
  return new Set(artifacts.map((a) => a.name)).size;
}

const totalFlows = computed(() => {
  let count =
    props.node.flows.length +
    countUniqueArtifactNames(props.node.artifacts ?? []) +
    (props.node.tables ?? []).length;
  for (const child of props.node.children) {
    count +=
      child.flows.length +
      countUniqueArtifactNames(child.artifacts ?? []) +
      (child.tables ?? []).length;
  }
  return count;
});
</script>

<style scoped>
.tree-node {
  user-select: none;
}

.tree-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: background var(--transition-fast);
}

.tree-row:hover {
  background: var(--color-background-hover);
}

.chevron {
  width: 14px;
  font-size: 10px;
  color: var(--color-text-muted);
  text-align: center;
  flex-shrink: 0;
}

.ns-icon {
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  flex-shrink: 0;
}

.ns-name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  flex: 1;
}

.ns-count {
  font-size: 11px;
  color: var(--color-text-muted);
  background: var(--color-background-tertiary);
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  line-height: 18px;
}

.tree-actions {
  display: flex;
  gap: 2px;
  opacity: 0;
  transition: opacity var(--transition-fast);
}

.tree-row:hover .tree-actions {
  opacity: 1;
}

.tree-children {
  padding-left: var(--spacing-4);
}

.tree-child-wrapper + .tree-child-wrapper {
  margin-top: var(--spacing-2);
  padding-top: var(--spacing-2);
  border-top: 1px solid var(--color-border-light);
}

.tree-section {
  margin-top: var(--spacing-2);
}

.section-header {
  width: 100%;
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.section-header:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.section-chevron {
  width: 12px;
  font-size: 10px;
  text-align: center;
  color: var(--color-text-muted);
}

.section-title {
  flex: 1;
  text-align: left;
}

.section-count {
  font-size: 10px;
  color: var(--color-text-muted);
  background: var(--color-background-primary);
  padding: 0 6px;
  border-radius: var(--border-radius-full);
  line-height: 16px;
}

.section-content {
  margin-top: var(--spacing-1);
  padding-left: var(--spacing-2);
}

.tree-flow {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.tree-flow:hover {
  background: var(--color-background-hover);
}

.tree-flow.selected {
  background: var(--color-primary-light, rgba(59, 130, 246, 0.1));
}

.flow-icon {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  flex-shrink: 0;
}

.flow-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.flow-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  opacity: 0;
  transition: opacity var(--transition-fast);
}

.tree-flow:hover .flow-actions,
.tree-flow.selected .flow-actions {
  opacity: 1;
}

.action-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 22px;
  height: 22px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: 12px;
  transition: all var(--transition-fast);
}

.action-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-primary);
}

.star-btn.active {
  color: #f59e0b;
}

.run-indicator {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.run-indicator.success {
  background: #22c55e;
}
.run-indicator.failure {
  background: #ef4444;
}

.tree-flow.file-missing,
.tree-table.file-missing {
  opacity: 0.55;
}
.tree-flow.file-missing .flow-icon,
.tree-table.file-missing .table-icon {
  color: #f59e0b;
}

.missing-icon {
  font-size: 11px;
  color: #f59e0b;
  flex-shrink: 0;
}

/* ========== Artifact Items ========== */
.tree-artifact {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.tree-artifact:hover {
  background: var(--color-background-hover);
}

.tree-artifact.selected {
  background: var(--color-primary-light, rgba(59, 130, 246, 0.1));
}

.tree-artifact .artifact-icon {
  color: var(--color-primary);
  font-size: var(--font-size-sm);
  flex-shrink: 0;
}

.tree-artifact .artifact-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.artifact-versions-count {
  font-size: 11px;
  color: var(--color-text-muted);
  flex-shrink: 0;
}

.artifact-version {
  font-size: 11px;
  color: var(--color-text-muted);
  background: var(--color-background-tertiary);
  padding: 0 5px;
  border-radius: var(--border-radius-sm);
  line-height: 18px;
  flex-shrink: 0;
  font-family: var(--font-family-mono);
}

/* ========== Catalog Table Items ========== */
.tree-table {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-2);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  transition: all var(--transition-fast);
}

.tree-table:hover {
  background: var(--color-background-hover);
}

.tree-table:hover .flow-actions,
.tree-table.selected .flow-actions {
  opacity: 1;
}

.tree-table.selected {
  background: var(--color-primary-light, rgba(59, 130, 246, 0.1));
}

.tree-table .table-icon {
  color: #10b981;
  font-size: var(--font-size-sm);
  flex-shrink: 0;
}

.tree-table .table-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.table-rows {
  font-size: 11px;
  color: var(--color-text-muted);
  background: var(--color-background-tertiary);
  padding: 0 5px;
  border-radius: var(--border-radius-sm);
  line-height: 18px;
  flex-shrink: 0;
  font-family: var(--font-family-mono);
}
</style>
