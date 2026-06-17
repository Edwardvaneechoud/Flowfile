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
      <SharedBadge :access="node.access" />
      <span v-if="totalFlows > 0" class="ns-count">{{ totalFlows }}</span>
      <div class="tree-actions" @click.stop>
        <button
          v-if="isMultiUser && isOwned(node)"
          class="action-btn"
          title="Share namespace"
          @click="$emit('namespaceShare', node)"
        >
          <i class="fa-solid fa-share-nodes"></i>
        </button>
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
          @table-context-menu="$emit('tableContextMenu', $event)"
          @artifact-context-menu="$emit('artifactContextMenu', $event)"
          @flow-context-menu="$emit('flowContextMenu', $event)"
          @select-visualization="$emit('selectVisualization', $event)"
          @toggle-favorite="$emit('toggleFavorite', $event)"
          @toggle-table-favorite="$emit('toggleTableFavorite', $event)"
          @register-flow="$emit('registerFlow', $event)"
          @register-table="$emit('registerTable', $event)"
          @create-schema="$emit('createSchema', $event)"
          @delete-table="$emit('deleteTable', $event)"
          @delete-flow="$emit('deleteFlow', $event)"
          @namespace-share="$emit('namespaceShare', $event)"
        />
      </div>

      <TreeSection
        v-if="showFlowsSection"
        ref="flowsSection"
        title="Flows"
        :count="visibleFlows.length"
        :storage-key="`sec:${node.id}:flows`"
        :default-expanded="sectionsDefaultExpanded"
      >
        <div
          v-for="flow in visibleFlows"
          :key="'f-' + flow.id"
          class="tree-flow"
          :class="{ selected: selectedFlowId === flow.id, 'file-missing': !flow.file_exists }"
          @click.stop="$emit('selectFlow', flow.id)"
          @contextmenu.prevent.stop="
            $emit('flowContextMenu', { flow, x: $event.clientX, y: $event.clientY })
          "
        >
          <i class="fa-solid fa-diagram-project flow-icon"></i>
          <span class="flow-name">{{ flow.name }}</span>
          <el-tooltip
            v-if="!flow.file_exists"
            content="Flow file not found on disk"
            placement="top"
            :show-after="300"
          >
            <i class="fa-solid fa-triangle-exclamation missing-icon"></i>
          </el-tooltip>
          <div class="flow-actions" @click.stop>
            <button
              class="action-btn star-btn"
              :class="{ active: flow.is_favorite }"
              :title="flow.is_favorite ? 'Unfavorite' : 'Favorite'"
              @click="$emit('toggleFavorite', flow.id)"
            >
              <i :class="flow.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
            </button>
            <button
              class="action-btn delete-btn"
              title="Delete flow"
              @click="$emit('deleteFlow', flow.id)"
            >
              <i class="fa-solid fa-trash"></i>
            </button>
            <span
              v-if="flow.last_run_success !== null"
              class="run-indicator"
              :class="flow.last_run_success ? 'success' : 'failure'"
              :title="flow.last_run_success ? 'Last run succeeded' : 'Last run failed'"
            ></span>
          </div>
        </div>
      </TreeSection>

      <TreeSection
        v-if="showModelsSection"
        ref="modelsSection"
        title="Models"
        :count="visibleArtifacts.length"
        :storage-key="`sec:${node.id}:models`"
        :default-expanded="sectionsDefaultExpanded"
      >
        <div
          v-for="group in visibleArtifacts"
          :key="'ag-' + group.name"
          class="tree-artifact"
          :class="{
            selected: selectedArtifactId === group.latest.id,
            'file-missing': group.latest.blob_exists === false,
          }"
          @click.stop="$emit('selectArtifact', group.latest.id)"
          @contextmenu.prevent.stop="
            $emit('artifactContextMenu', {
              artifact: group.latest,
              x: $event.clientX,
              y: $event.clientY,
            })
          "
        >
          <i class="fa-solid fa-cube artifact-icon"></i>
          <span class="artifact-name">{{ group.name }}</span>
          <span v-if="group.versionCount > 1" class="artifact-versions-count">
            {{ group.versionCount }} versions
          </span>
          <span class="artifact-version">v{{ group.latest.version }}</span>
          <el-tooltip
            v-if="group.latest.blob_exists === false"
            content="Model data file not found on disk"
            placement="top"
            :show-after="300"
          >
            <i class="fa-solid fa-triangle-exclamation missing-icon"></i>
          </el-tooltip>
        </div>
      </TreeSection>

      <TreeSection
        v-if="showTablesSection"
        ref="tablesSection"
        title="Tables"
        :count="visibleTables.length"
        :storage-key="`sec:${node.id}:tables`"
        :default-expanded="sectionsDefaultExpanded"
      >
        <div
          v-for="table in visibleTables"
          :key="'t-' + table.id"
          class="tree-table"
          :class="{
            selected: selectedTableId === table.id,
            'file-missing': table.table_type !== 'virtual' && table.file_exists === false,
          }"
          @click.stop="$emit('selectTable', table.id)"
          @contextmenu.prevent.stop="
            $emit('tableContextMenu', { table, x: $event.clientX, y: $event.clientY })
          "
        >
          <i
            v-if="table.table_type === 'virtual'"
            class="fa-solid fa-bolt table-icon virtual-icon"
            title="Virtual Flow Table"
          ></i>
          <i v-else class="fa-solid fa-table table-icon"></i>
          <span class="table-name">{{ table.name }}</span>
          <span
            v-if="table.table_type === 'virtual'"
            class="table-virtual-badge"
            title="Virtual Flow Table"
            >virtual</span
          >
          <el-tooltip
            v-if="table.table_type !== 'virtual' && table.file_exists === false"
            content="Table data file not found on disk"
            placement="top"
            :show-after="300"
          >
            <i class="fa-solid fa-triangle-exclamation missing-icon"></i>
          </el-tooltip>
          <div class="flow-actions" @click.stop>
            <button
              class="action-btn star-btn"
              :class="{ active: table.is_favorite }"
              :title="table.is_favorite ? 'Unfavorite' : 'Favorite'"
              @click="$emit('toggleTableFavorite', table.id)"
            >
              <i :class="table.is_favorite ? 'fa-solid fa-star' : 'fa-regular fa-star'"></i>
            </button>
            <button
              class="action-btn delete-btn"
              title="Delete table"
              @click="$emit('deleteTable', table.id)"
            >
              <i class="fa-solid fa-trash"></i>
            </button>
          </div>
        </div>
      </TreeSection>

      <TreeSection
        v-if="showVisualizationsSection"
        ref="visualizationsSection"
        title="Visualizations"
        :count="visibleVisualizations.length"
        :storage-key="`sec:${node.id}:visualizations`"
        :default-expanded="sectionsDefaultExpanded"
      >
        <div
          v-for="viz in visibleVisualizations"
          :key="'v-' + viz.id"
          class="tree-table"
          @click.stop="$emit('selectVisualization', viz.id)"
        >
          <i
            :class="
              viz.source_type === 'sql'
                ? 'fa-solid fa-code table-icon viz-sql-icon'
                : 'fa-solid fa-chart-column table-icon viz-icon'
            "
            :title="viz.source_type === 'sql' ? 'SQL-source visualization' : 'Visualization'"
          ></i>
          <span class="table-name">{{ viz.name }}</span>
          <span v-if="viz.source_type === 'sql'" class="table-virtual-badge" title="SQL source">
            sql
          </span>
        </div>
      </TreeSection>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { SYSTEM_NAMESPACE_NAMES } from "../../types";
import type { GlobalArtifact, NamespaceTree } from "../../types";
import TreeSection from "./components/TreeSection.vue";
import { useCatalogTreeExpansion } from "./useCatalogTreeExpansion";
import SharedBadge from "../../components/sharing/SharedBadge.vue";
import { useResourceSharing } from "../../composables/useResourceSharing";

const { isMultiUser, isOwned } = useResourceSharing();

type TreeSectionRef = InstanceType<typeof TreeSection> | null;

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
  "tableContextMenu",
  "artifactContextMenu",
  "flowContextMenu",
  "selectVisualization",
  "toggleFavorite",
  "toggleTableFavorite",
  "registerFlow",
  "registerTable",
  "createSchema",
  "deleteTable",
  "deleteFlow",
  "namespaceShare",
]);

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

const visibleVisualizations = computed(() => {
  let viz = props.node.visualizations ?? [];
  if (query.value) {
    viz = viz.filter((v) => v.name.toLowerCase().includes(query.value));
  }
  return viz;
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
  if (!props.showUnavailable) {
    groups = groups.filter((g) => g.latest.blob_exists !== false);
  }
  if (query.value) {
    groups = groups.filter((g) => g.name.toLowerCase().includes(query.value));
  }
  return groups;
});

// System namespaces holding disk-backed/quick-created flows — they accumulate a
// lot of entries, so collapse them by default to keep the tree tidy.
const AUTO_COLLAPSE_NAMESPACES = new Set(["Local Flows", "Unnamed Flows"]);
// Expansion state is shared + persisted to localStorage; user toggles and
// selection/search-driven expands both stick, so the tree reopens as left.
const treeState = useCatalogTreeExpansion();
const expanded = computed({
  get: () =>
    treeState.isExpanded(`ns:${props.node.id}`, !AUTO_COLLAPSE_NAMESPACES.has(props.node.name)),
  set: (value) => treeState.setExpanded(`ns:${props.node.id}`, value),
});
const toggle = () => {
  expanded.value = !expanded.value;
};

// Sections inside system namespaces stay collapsed by default; the default
// schema and user-created namespaces open fully uncollapsed.
const sectionsDefaultExpanded = computed(() => !SYSTEM_NAMESPACE_NAMES.has(props.node.name));

const flowsSection = ref<TreeSectionRef>(null);
const modelsSection = ref<TreeSectionRef>(null);
const tablesSection = ref<TreeSectionRef>(null);
const visualizationsSection = ref<TreeSectionRef>(null);

const showFlowsSection = computed(() => props.node.level === 1 && visibleFlows.value.length > 0);
const showModelsSection = computed(
  () => props.node.level === 1 && visibleArtifacts.value.length > 0,
);
const showTablesSection = computed(() => props.node.level === 1 && visibleTables.value.length > 0);
const showVisualizationsSection = computed(
  () => props.node.level === 1 && visibleVisualizations.value.length > 0,
);

watch(
  () => props.selectedFlowId,
  (flowId) => {
    if (flowId !== null && containsFlow(props.node, flowId)) {
      expanded.value = true;
      flowsSection.value?.expand();
    }
  },
);

watch(
  () => props.selectedArtifactId,
  (artifactId) => {
    if (artifactId !== null && containsArtifact(props.node, artifactId)) {
      expanded.value = true;
      modelsSection.value?.expand();
    }
  },
);

watch(
  () => props.selectedTableId,
  (tableId) => {
    if (tableId !== null && containsTable(props.node, tableId)) {
      expanded.value = true;
      tablesSection.value?.expand();
    }
  },
);

watch(query, (value) => {
  if (!value) return;
  if (visibleFlows.value.length > 0) flowsSection.value?.expand();
  if (visibleArtifacts.value.length > 0) modelsSection.value?.expand();
  if (visibleTables.value.length > 0) tablesSection.value?.expand();
  if (visibleVisualizations.value.length > 0) visualizationsSection.value?.expand();
});

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
  color: var(--color-warning);
}

.run-indicator {
  width: 8px;
  height: 8px;
  border-radius: var(--border-radius-full);
  flex-shrink: 0;
}

.run-indicator.success {
  background: var(--color-success);
}
.run-indicator.failure {
  background: var(--color-danger);
}

.tree-flow.file-missing,
.tree-table.file-missing,
.tree-artifact.file-missing {
  opacity: 0.55;
}
.tree-flow.file-missing .flow-icon,
.tree-table.file-missing .table-icon,
.tree-artifact.file-missing .artifact-icon {
  color: var(--color-warning);
}

.missing-icon {
  font-size: 11px;
  color: var(--color-warning);
  flex-shrink: 0;
}

.delete-btn:hover {
  color: var(--color-danger) !important;
  background: color-mix(in srgb, var(--color-danger) 10%, transparent) !important;
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
  color: var(--color-success);
  font-size: var(--font-size-sm);
  flex-shrink: 0;
}

.tree-table .virtual-icon {
  color: var(--el-color-primary, var(--color-primary));
}

.tree-table .viz-icon {
  color: var(--el-color-primary, var(--color-primary));
}

.tree-table .viz-sql-icon {
  color: var(--el-color-warning, #e6a23c);
}

.table-virtual-badge {
  font-size: 10px;
  color: var(--el-color-primary, var(--color-primary));
  background: var(--el-color-primary-light-9, rgba(64, 158, 255, 0.1));
  padding: 0 5px;
  border-radius: var(--border-radius-sm);
  line-height: 16px;
  flex-shrink: 0;
  font-weight: var(--font-weight-medium);
}

.tree-table .table-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  flex: 1;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
