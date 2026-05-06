<script setup lang="ts">
// W35 — Inline-diff renderer.
//
// Pure presentation: takes a `GraphDiffPayload` (W41 shapes) and emits
// `accept` / `reject`. No store coupling — `AiDiffPanel.vue` wires
// these to the `useAiDiffStore` actions. Per-op cards render in W41's
// apply order so the visual order matches what `apply_diff` will do
// when the user clicks Accept.
//
// Destructive ops (deletions) are red-bordered per plan §9.2; the
// drift error block at the top mirrors D006's "snapshot+warn-and-pause"
// shape — when the backend reports `error: "diff_drift"` the diff
// stays staged and the user sees which node ids vanished.

import { computed, ref } from "vue";

import { useFlowStore } from "../../stores/flow-store";
import type {
  DiffStoreError,
  GraphDiffPayload,
  StagedAddition,
  StagedConnection,
  StagedSchemaColumn,
} from "./aiDiffTypes";
import { buildAdditionNodeTypes, opCount, richConnectionLabel } from "./aiDiffTypes";
import { renderSafeMarkdown } from "./markdown";

const flowStore = useFlowStore();

const lookupExistingNodeType = (nodeId: number): string | undefined => {
  // Vue Flow keys nodes by stringified id; `node.data.nodeTemplate.item` is the
  // canonical snake_case node type that matches what `additions` carry on the
  // wire. Defensive about the chain since `vueFlowInstance` is null before the
  // canvas mounts.
  const node = flowStore.vueFlowInstance?.findNode(String(nodeId));
  const nodeType = node?.data?.nodeTemplate?.item;
  return typeof nodeType === "string" ? nodeType : undefined;
};

interface Props {
  diff: GraphDiffPayload;
  disabled?: boolean;
  error?: DiffStoreError | null;
}

const props = withDefaults(defineProps<Props>(), {
  disabled: false,
  error: null,
});

const emit = defineEmits<{
  (e: "accept"): void;
  (e: "reject"): void;
}>();

const expandedSettings = ref<Set<number>>(new Set());

const toggleSettings = (index: number): void => {
  if (expandedSettings.value.has(index)) {
    expandedSettings.value.delete(index);
  } else {
    expandedSettings.value.add(index);
  }
  // Trigger reactivity — Set mutations don't notify by reference change.
  expandedSettings.value = new Set(expandedSettings.value);
};

const summaryLine = computed(() => {
  const parts: string[] = [];
  if (props.diff.additions.length > 0) {
    parts.push(
      `${props.diff.additions.length} addition${props.diff.additions.length === 1 ? "" : "s"}`,
    );
  }
  if (props.diff.connections_added.length > 0) {
    parts.push(
      `${props.diff.connections_added.length} new connection${props.diff.connections_added.length === 1 ? "" : "s"}`,
    );
  }
  if (props.diff.deletions.length > 0) {
    parts.push(
      `${props.diff.deletions.length} deletion${props.diff.deletions.length === 1 ? "" : "s"}`,
    );
  }
  if (props.diff.connections_removed.length > 0) {
    parts.push(
      `${props.diff.connections_removed.length} removed connection${props.diff.connections_removed.length === 1 ? "" : "s"}`,
    );
  }
  return parts.length > 0 ? parts.join(", ") : "No operations";
});

const formatSchemaColumn = (col: StagedSchemaColumn): string => {
  const dtype = col.data_type ?? "?";
  return `${col.name}: ${dtype}`;
};

const nodeTypeById = computed<Map<number, string>>(() => {
  // Start with newly-staged nodes from the diff itself (their type + id are
  // already on the wire). Layer existing-node lookups from the live flow store
  // on top so connections that reference pre-existing nodes also get a type.
  const map = buildAdditionNodeTypes(props.diff);
  const seedExisting = (id: number | null | undefined): void => {
    if (typeof id !== "number" || map.has(id)) return;
    const t = lookupExistingNodeType(id);
    if (t) map.set(id, t);
  };
  for (const c of props.diff.connections_added) {
    seedExisting(c.connection.output_connection?.node_id);
    seedExisting(c.connection.input_connection?.node_id);
  }
  for (const c of props.diff.connections_removed) {
    seedExisting(c.connection.output_connection?.node_id);
    seedExisting(c.connection.input_connection?.node_id);
  }
  for (const d of props.diff.deletions) seedExisting(d.delete_node_id);
  for (const add of props.diff.additions) {
    for (const upId of add.insertion_context.upstream_node_ids) seedExisting(upId);
    seedExisting(add.insertion_context.right_input_node_id);
  }
  return map;
});

const formatNodeRef = (nodeId: number): string => {
  const nodeType = nodeTypeById.value.get(nodeId);
  return nodeType ? `${nodeType} #${nodeId}` : `#${nodeId}`;
};

const renderConnection = (c: StagedConnection): string =>
  richConnectionLabel(c, nodeTypeById.value);

const additionUpstreamLabel = (add: StagedAddition): string => {
  const ids = add.insertion_context.upstream_node_ids;
  if (ids.length === 0) return "(no upstream — source node)";
  const right = add.insertion_context.right_input_node_id;
  const main = ids.map(formatNodeRef).join(", ");
  return right !== null ? `main: ${main} | right: ${formatNodeRef(right)}` : main;
};

const formatSettings = (settings: Record<string, unknown>): string => {
  try {
    return JSON.stringify(settings, null, 2);
  } catch {
    return String(settings);
  }
};

const renderedRationale = computed(() => renderSafeMarkdown(props.diff.rationale ?? ""));

const handleAccept = (): void => {
  if (props.disabled) return;
  emit("accept");
};

const handleReject = (): void => {
  if (props.disabled) return;
  emit("reject");
};
</script>

<template>
  <div class="ai-diff-preview">
    <header class="ai-diff-preview__header">
      <div class="ai-diff-preview__header-line">
        <span class="ai-diff-preview__title">AI proposed changes</span>
        <span class="ai-diff-preview__summary">{{ summaryLine }} ({{ opCount(diff) }} ops)</span>
      </div>
      <!-- W66 — rationale is markdown-formatted on the wire; render as
           sanitised HTML so bold / code / lists / links land properly.
           `renderedRationale` runs through marked + a DOMPurify-or-no-op
           pass with an explicit allow-list, see `markdown.ts`. -->
      <!-- eslint-disable vue/no-v-html -->
      <div
        v-if="diff.rationale"
        class="ai-diff-preview__rationale"
        v-html="renderedRationale"
      ></div>
      <!-- eslint-enable vue/no-v-html -->
    </header>

    <div
      v-if="error"
      class="ai-diff-preview__error"
      :class="`ai-diff-preview__error--${error.kind}`"
    >
      <p class="ai-diff-preview__error-line">{{ error.message }}</p>
      <p
        v-if="error.kind === 'drift' && error.missingNodeIds.length > 0"
        class="ai-diff-preview__error-hint"
      >
        Missing node ids:
        <code v-for="id in error.missingNodeIds" :key="id" class="ai-diff-preview__node-id">
          #{{ id }}
        </code>
      </p>
    </div>

    <section v-if="diff.additions.length > 0" class="ai-diff-preview__section">
      <h4 class="ai-diff-preview__section-title">Additions</h4>
      <article
        v-for="(add, idx) in diff.additions"
        :key="`add-${idx}`"
        class="ai-diff-preview__card ai-diff-preview__card--add"
      >
        <header class="ai-diff-preview__card-header">
          <span class="ai-diff-preview__chip">{{ add.node_type }}</span>
          <span class="ai-diff-preview__upstream">{{ additionUpstreamLabel(add) }}</span>
        </header>
        <p
          v-if="add.predicted_output_schema && add.predicted_output_schema.length > 0"
          class="ai-diff-preview__schema"
        >
          <span class="ai-diff-preview__schema-label">Predicted output:</span>
          <span
            v-for="col in add.predicted_output_schema"
            :key="col.name"
            class="ai-diff-preview__schema-col"
          >
            {{ formatSchemaColumn(col) }}
          </span>
        </p>
        <p v-else class="ai-diff-preview__schema ai-diff-preview__schema--unknown">
          Predicted output schema unavailable
        </p>
        <button type="button" class="ai-diff-preview__settings-toggle" @click="toggleSettings(idx)">
          {{ expandedSettings.has(idx) ? "Hide settings" : "Show settings" }}
        </button>
        <pre v-if="expandedSettings.has(idx)" class="ai-diff-preview__settings">{{
          formatSettings(add.settings)
        }}</pre>
      </article>
    </section>

    <section v-if="diff.connections_added.length > 0" class="ai-diff-preview__section">
      <h4 class="ai-diff-preview__section-title">New connections</h4>
      <article
        v-for="(c, idx) in diff.connections_added"
        :key="`con-add-${idx}`"
        class="ai-diff-preview__card ai-diff-preview__card--connection"
      >
        <span class="ai-diff-preview__connection-line">{{ renderConnection(c) }}</span>
      </article>
    </section>

    <section v-if="diff.deletions.length > 0" class="ai-diff-preview__section">
      <h4 class="ai-diff-preview__section-title">Deletions</h4>
      <article
        v-for="(d, idx) in diff.deletions"
        :key="`del-${idx}`"
        class="ai-diff-preview__card ai-diff-preview__card--delete"
      >
        <span class="ai-diff-preview__chip ai-diff-preview__chip--danger">delete node</span>
        <span class="ai-diff-preview__upstream">{{ formatNodeRef(d.delete_node_id) }}</span>
      </article>
    </section>

    <section v-if="diff.connections_removed.length > 0" class="ai-diff-preview__section">
      <h4 class="ai-diff-preview__section-title">Removed connections</h4>
      <article
        v-for="(c, idx) in diff.connections_removed"
        :key="`con-rem-${idx}`"
        class="ai-diff-preview__card ai-diff-preview__card--delete"
      >
        <span class="ai-diff-preview__connection-line">{{ renderConnection(c) }}</span>
      </article>
    </section>

    <footer class="ai-diff-preview__footer">
      <button
        type="button"
        class="ai-diff-preview__btn ai-diff-preview__btn--secondary"
        :disabled="disabled"
        @click="handleReject"
      >
        Reject
      </button>
      <button
        type="button"
        class="ai-diff-preview__btn ai-diff-preview__btn--primary"
        :disabled="disabled"
        @click="handleAccept"
      >
        Accept
      </button>
    </footer>
  </div>
</template>

<style scoped>
.ai-diff-preview {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 10px;
  border-radius: 8px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border: 1px solid var(--color-border-primary, #e1e4e8);
  font-size: 12px;
}

.ai-diff-preview__header {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding-bottom: 6px;
  border-bottom: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-diff-preview__header-line {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 8px;
}

.ai-diff-preview__title {
  font-weight: 600;
  font-size: 13px;
  color: var(--color-text-primary, #24292e);
}

.ai-diff-preview__summary {
  font-size: 11px;
  color: var(--color-text-muted, #6a737d);
}

.ai-diff-preview__rationale {
  margin: 0;
  color: var(--color-text-secondary, #4a5560);
  font-size: 12px;
  line-height: 1.45;
}

.ai-diff-preview__rationale :deep(p) {
  margin: 0 0 6px 0;
}

.ai-diff-preview__rationale :deep(p:last-child) {
  margin-bottom: 0;
}

.ai-diff-preview__rationale :deep(strong) {
  font-weight: 600;
  color: var(--color-text-primary, #24292e);
}

.ai-diff-preview__rationale :deep(em) {
  font-style: italic;
}

.ai-diff-preview__rationale :deep(code) {
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
  background-color: var(--color-background-secondary, #f6f8fa);
  padding: 1px 5px;
  border-radius: 3px;
  color: var(--color-text-primary, #24292e);
}

.ai-diff-preview__rationale :deep(pre) {
  margin: 6px 0;
  padding: 8px 10px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border-radius: 4px;
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
  color: var(--color-text-primary, #24292e);
  overflow-x: auto;
  white-space: pre;
}

.ai-diff-preview__rationale :deep(pre code) {
  background: transparent;
  padding: 0;
  border-radius: 0;
}

.ai-diff-preview__rationale :deep(ul),
.ai-diff-preview__rationale :deep(ol) {
  margin: 4px 0 6px 0;
  padding-left: 20px;
}

.ai-diff-preview__rationale :deep(li) {
  margin: 2px 0;
}

.ai-diff-preview__rationale :deep(a) {
  color: var(--color-info, #0366d6);
  text-decoration: underline;
}

.ai-diff-preview__rationale :deep(hr) {
  border: 0;
  border-top: 1px solid var(--color-border-primary, #e1e4e8);
  margin: 8px 0;
}

.ai-diff-preview__error {
  padding: 8px 10px;
  border-radius: 6px;
  font-size: 12px;
}

.ai-diff-preview__error--drift {
  background-color: var(--color-warning-light, #fff8e1);
  color: var(--color-warning, #b07b00);
  border: 1px solid var(--color-warning, #b07b00);
}

.ai-diff-preview__error--http {
  background-color: var(--color-danger-light, #ffe5e5);
  color: var(--color-danger, #c53030);
  border: 1px solid var(--color-danger, #c53030);
}

.ai-diff-preview__error-line {
  margin: 0 0 4px 0;
  font-weight: 500;
}

.ai-diff-preview__error-hint {
  margin: 0;
  font-size: 11px;
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  align-items: center;
}

.ai-diff-preview__node-id {
  background-color: var(--color-background-primary, #ffffff);
  padding: 1px 5px;
  border-radius: 4px;
  font-family: var(--font-family-mono, monospace);
}

.ai-diff-preview__section {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.ai-diff-preview__section-title {
  margin: 0;
  font-size: 11px;
  font-weight: 600;
  color: var(--color-text-muted, #6a737d);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.ai-diff-preview__card {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding: 6px 8px;
  border-radius: 6px;
  background-color: var(--color-background-primary, #ffffff);
  border-left: 3px solid var(--color-border-primary, #e1e4e8);
}

.ai-diff-preview__card--add {
  border-left-color: var(--color-success, #28a745);
}

.ai-diff-preview__card--connection {
  border-left-color: var(--color-info, #0366d6);
}

.ai-diff-preview__card--delete {
  border-left-color: var(--color-danger, #c53030);
  background-color: var(--color-danger-light, #ffe5e5);
}

.ai-diff-preview__card-header {
  display: flex;
  align-items: center;
  gap: 6px;
  flex-wrap: wrap;
}

.ai-diff-preview__chip {
  display: inline-block;
  padding: 1px 6px;
  border-radius: 4px;
  background-color: var(--color-background-secondary, #f6f8fa);
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
  color: var(--color-text-primary, #24292e);
  border: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-diff-preview__chip--danger {
  background-color: var(--color-danger, #c53030);
  color: var(--color-text-inverse, #ffffff);
  border-color: var(--color-danger, #c53030);
}

.ai-diff-preview__upstream {
  font-size: 11px;
  color: var(--color-text-muted, #6a737d);
  font-family: var(--font-family-mono, monospace);
}

.ai-diff-preview__schema {
  margin: 0;
  font-size: 11px;
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  align-items: baseline;
}

.ai-diff-preview__schema--unknown {
  color: var(--color-text-muted, #6a737d);
  font-style: italic;
}

.ai-diff-preview__schema-label {
  color: var(--color-text-muted, #6a737d);
  font-weight: 500;
}

.ai-diff-preview__schema-col {
  font-family: var(--font-family-mono, monospace);
  background-color: var(--color-background-secondary, #f6f8fa);
  padding: 1px 5px;
  border-radius: 3px;
  color: var(--color-text-primary, #24292e);
}

.ai-diff-preview__settings-toggle {
  align-self: flex-start;
  background: none;
  border: none;
  font-size: 11px;
  color: var(--color-info, #0366d6);
  cursor: pointer;
  padding: 0;
  text-decoration: underline;
}

.ai-diff-preview__settings {
  margin: 0;
  padding: 6px 8px;
  background-color: var(--color-background-secondary, #f6f8fa);
  border-radius: 4px;
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
  color: var(--color-text-primary, #24292e);
  white-space: pre-wrap;
  word-break: break-word;
  max-height: 200px;
  overflow-y: auto;
}

.ai-diff-preview__connection-line {
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
  color: var(--color-text-primary, #24292e);
}

.ai-diff-preview__footer {
  display: flex;
  gap: 6px;
  justify-content: flex-end;
  padding-top: 6px;
  border-top: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-diff-preview__btn {
  padding: 5px 12px;
  border-radius: 5px;
  font-size: 12px;
  font-weight: 500;
  cursor: pointer;
  border: 1px solid transparent;
}

.ai-diff-preview__btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.ai-diff-preview__btn--primary {
  background: linear-gradient(
    135deg,
    var(--color-gradient-purple-start, #6f42c1) 0%,
    var(--color-gradient-purple-end, #5933a8) 100%
  );
  color: var(--color-text-inverse, #ffffff);
}

.ai-diff-preview__btn--secondary {
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #24292e);
  border: 1px solid var(--color-border-primary, #e1e4e8);
}

.ai-diff-preview__btn--secondary:hover:enabled {
  background-color: var(--color-background-hover, #ececec);
}
</style>
