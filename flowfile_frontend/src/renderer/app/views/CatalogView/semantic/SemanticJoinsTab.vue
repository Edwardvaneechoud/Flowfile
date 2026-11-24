<template>
  <div class="joins-tab">
    <div class="toolbar">
      <p class="hint">
        A join connects the many side (from) to the one side (to), so a metric knows how to reach
        its dimensions.
      </p>
      <button class="btn btn-primary btn-sm" @click="showDialog = true">
        <i class="fa-solid fa-plus"></i> Add join
      </button>
    </div>

    <div v-if="edges.length > 0" class="diagram" data-testid="semantic-join-diagram">
      <VueFlow
        :nodes="nodes"
        :edges="edges"
        :nodes-draggable="false"
        :nodes-connectable="false"
        :elements-selectable="false"
        :zoom-on-scroll="false"
        :pan-on-drag="false"
        :zoom-on-double-click="false"
        fit-view-on-init
      />
    </div>

    <p v-if="model.joins.length === 0" class="empty">No joins yet.</p>
    <ul v-else class="join-list">
      <li v-for="join in model.joins" :key="join.key" class="join-row">
        <div class="join-main">
          <span class="join-name">{{ join.name }}</span>
          <span class="layer-badge" :class="`layer-${join.layer}`">{{ join.layer }}</span>
          <SemanticStatusChip :status="join.status" />
        </div>
        <div class="join-sides">
          <code>{{ join.from_table }}.{{ join.from_columns.join(", ") }}</code>
          <i class="fa-solid fa-arrow-right"></i>
          <code>{{ join.to_table }}.{{ join.to_columns.join(", ") }}</code>
        </div>
        <p v-if="join.description" class="join-desc">{{ join.description }}</p>
      </li>
    </ul>

    <SemanticJoinDialog v-model="showDialog" :namespace-id="model.namespace_id" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { VueFlow, type Edge, type Node } from "@vue-flow/core";
import type { SemanticModel } from "../../../types/semantic.types";
import SemanticJoinDialog from "./SemanticJoinDialog.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";

const props = defineProps<{ model: SemanticModel }>();
const showDialog = ref(false);

const tableNames = computed(() => {
  const names = new Set<string>();
  for (const j of props.model.joins) {
    names.add(j.from_table);
    names.add(j.to_table);
  }
  return [...names];
});

const nodes = computed<Node[]>(() =>
  tableNames.value.map((name, i) => ({
    id: name,
    label: name,
    position: { x: (i % 4) * 190, y: Math.floor(i / 4) * 90 },
    class: "semantic-diagram-node",
  })),
);

const edges = computed<Edge[]>(() =>
  props.model.joins.map((j) => ({
    id: j.key,
    source: j.from_table,
    target: j.to_table,
    label: j.name,
    animated: j.status !== "accepted",
  })),
);
</script>

<style scoped>
@import "@vue-flow/core/dist/style.css";

.joins-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.hint,
.empty {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.diagram {
  height: 220px;
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
}

.join-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  margin: 0;
  padding: 0;
  list-style: none;
}

.join-row {
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
}

.join-main {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.join-name {
  font-weight: var(--font-weight-semibold);
}

.layer-badge {
  padding: 0 var(--spacing-2);
  border-radius: var(--border-radius-sm);
  border: 1px solid var(--color-border-light);
  font-size: var(--font-size-xs);
  line-height: 18px;
  color: var(--color-text-secondary);
}

.layer-catalog {
  color: var(--color-primary);
  border-color: var(--color-primary);
}

.join-sides {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  margin-top: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.join-sides code {
  color: var(--color-text-primary);
}

.join-desc {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
</style>
