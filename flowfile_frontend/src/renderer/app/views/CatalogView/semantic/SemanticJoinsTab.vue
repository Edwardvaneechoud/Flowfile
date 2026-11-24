<template>
  <div class="joins-tab">
    <SemanticSuggestMore
      :kinds="['join']"
      label="Suggest joins"
      @review="emit('open-tab', 'review')"
    >
      <template #hint>
        <p class="hint">
          A join connects the many side (from) to the one side (to), so a metric knows how to reach
          its dimensions.
        </p>
      </template>
      <button class="btn btn-primary btn-sm" @click="showDialog = true">
        <i class="fa-solid fa-plus"></i> Add join
      </button>
    </SemanticSuggestMore>

    <div
      v-if="edges.length > 0"
      class="diagram"
      :style="{ height: `${diagramHeight}px` }"
      data-testid="semantic-join-diagram"
    >
      <VueFlow
        :nodes="nodes"
        :edges="edges"
        :nodes-draggable="false"
        :nodes-connectable="false"
        :elements-selectable="false"
        :zoom-on-scroll="false"
        :pan-on-drag="false"
        :zoom-on-double-click="false"
        :min-zoom="0.2"
        :max-zoom="1"
        fit-view-on-init
        @pane-ready="onPaneReady"
        @nodes-initialized="fitDiagram"
      />
    </div>

    <p v-if="model.joins.length === 0" class="empty">No joins yet.</p>
    <ul v-else class="join-list">
      <SemanticJoinRow
        v-for="join in model.joins"
        :key="join.key"
        :join="join"
        :check="checkOf(join)"
      >
        <template #actions>
          <button
            v-if="join.can_remove"
            type="button"
            class="btn btn-ghost btn-danger btn-sm remove-btn"
            data-testid="semantic-join-remove"
            :disabled="removing !== null"
            @click="removeJoin(join)"
          >
            <i class="fa-solid fa-trash"></i>
            {{ removing === join.key ? "Removing…" : "Remove" }}
          </button>
        </template>
        <template #check-actions>
          <button
            v-if="canFix(join)"
            type="button"
            class="btn btn-secondary btn-sm"
            data-testid="semantic-join-fix"
            :disabled="fixing !== null"
            @click="fixDirection(join)"
          >
            <i class="fa-solid fa-right-left"></i>
            {{ fixing === join.key ? "Fixing…" : "Fix direction" }}
          </button>
        </template>
      </SemanticJoinRow>
    </ul>

    <SemanticJoinDialog v-model="showDialog" :namespace-id="model.namespace_id" />
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import {
  MarkerType,
  Position,
  VueFlow,
  type Edge,
  type Node,
  type VueFlowStore,
} from "@vue-flow/core";
import type { JoinCheck, SemanticJoin, SemanticModel } from "../../../types/semantic.types";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import SemanticJoinDialog from "./SemanticJoinDialog.vue";
import SemanticJoinRow from "./SemanticJoinRow.vue";
import SemanticSuggestMore from "./SemanticSuggestMore.vue";
import { errorText } from "./semanticFormat";
import { confirmRemoveJoin } from "./semanticRemoval";

const props = defineProps<{ model: SemanticModel }>();
const emit = defineEmits<{ "open-tab": [tab: string] }>();
const store = useSemanticModelStore();
const showDialog = ref(false);
const fixing = ref<string | null>(null);
const removing = ref<string | null>(null);

// Re-measure when the tab opens and whenever a join's ends, columns or status change.
const joinsSignature = computed(() =>
  JSON.stringify([
    props.model.namespace_id,
    props.model.joins.map((j) => [
      j.key,
      j.from_table,
      j.to_table,
      j.from_columns,
      j.to_columns,
      j.status,
    ]),
  ]),
);
watch(joinsSignature, () => void store.checkJoins(), { immediate: true });

const checkOf = (join: SemanticJoin): JoinCheck | undefined => store.joinChecks[join.key];

const isWrong = (join: SemanticJoin) => {
  const verdict = checkOf(join)?.verdict;
  return verdict === "reversed" || verdict === "many_to_many";
};

/** A reversed, accepted join this model owns can be re-proposed under its name with the sides swapped. */
const canFix = (join: SemanticJoin) =>
  checkOf(join)?.verdict === "reversed" &&
  join.status === "accepted" &&
  join.from_table_id !== null &&
  join.to_table_id !== null &&
  (join.layer === "schema" || props.model.level === 0);

async function fixDirection(join: SemanticJoin) {
  if (join.from_table_id === null || join.to_table_id === null) return;
  fixing.value = join.key;
  try {
    const facts = await store.propose([
      {
        kind: "join",
        name: join.name,
        from_table_id: join.to_table_id,
        to_table_id: join.from_table_id,
        from_columns: join.to_columns,
        to_columns: join.from_columns,
        description: join.description,
      },
    ]);
    if (facts.some((f) => f.status !== "accepted")) {
      ElMessage.info(
        "Proposed with its sides swapped. It replaces this join once accepted in Review.",
      );
    } else {
      ElMessage.success("Direction fixed.");
    }
  } catch (e) {
    ElMessage.error(errorText(e, "Could not fix the direction"));
  } finally {
    fixing.value = null;
  }
}

async function removeJoin(join: SemanticJoin) {
  if (!(await confirmRemoveJoin(props.model.namespace_id, join))) return;
  removing.value = join.key;
  try {
    await store.removeSubject(join.key);
    ElMessage.success(`Removed ${join.name}.`);
  } catch (e) {
    ElMessage.error(errorText(e, "Could not remove the join"));
  } finally {
    removing.value = null;
  }
}

const tableNames = computed(() => {
  const names = new Set<string>();
  for (const j of props.model.joins) {
    names.add(j.from_table);
    names.add(j.to_table);
  }
  return [...names];
});

/** The table's own name, or `schema.table` when two tables in the diagram share a name. */
const shortNames = computed(() => {
  const last = (key: string) => key.split(".").slice(-1)[0];
  const counts = new Map<string, number>();
  for (const key of tableNames.value) counts.set(last(key), (counts.get(last(key)) ?? 0) + 1);
  return new Map(
    tableNames.value.map((key) => [
      key,
      counts.get(last(key)) === 1 ? last(key) : key.split(".").slice(-2).join("."),
    ]),
  );
});

/** Column per table: the many side sits left, what it points at one step right. Capped, so a cycle ends. */
const columns = computed(() => {
  const names = tableNames.value;
  const column = new Map(names.map((n) => [n, 0]));
  for (let pass = 0; pass < names.length; pass++) {
    let moved = false;
    for (const j of props.model.joins) {
      if (j.from_table === j.to_table) continue;
      const next = (column.get(j.from_table) ?? 0) + 1;
      if (next > (column.get(j.to_table) ?? 0) && next < names.length) {
        column.set(j.to_table, next);
        moved = true;
      }
    }
    if (!moved) break;
  }
  return column;
});

/** Tables joined to themselves: marked on their box, since a loop edge has nowhere sensible to go. */
const selfJoined = computed(
  () =>
    new Set(props.model.joins.filter((j) => j.from_table === j.to_table).map((j) => j.from_table)),
);

const nodes = computed<Node[]>(() => {
  const rows = new Map<number, number>();
  return tableNames.value.map((name) => {
    const col = columns.value.get(name) ?? 0;
    const row = rows.get(col) ?? 0;
    rows.set(col, row + 1);
    return {
      id: name,
      label: `${shortNames.value.get(name) ?? name}${selfJoined.value.has(name) ? " ⟲" : ""}`,
      position: { x: col * 280, y: row * 80 },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      class: "semantic-diagram-node",
    };
  });
});

/** Tall enough for the fullest column of tables, within limits; the view is refitted when it changes. */
const diagramHeight = computed(() => {
  const perColumn = new Map<number, number>();
  for (const col of columns.value.values()) perColumn.set(col, (perColumn.get(col) ?? 0) + 1);
  const rows = Math.max(1, ...perColumn.values());
  return Math.min(480, Math.max(180, rows * 80 + 100));
});

let flow: VueFlowStore | null = null;

function fitDiagram() {
  void nextTick(() => flow?.fitView({ padding: 0.2, maxZoom: 1 }));
}

function onPaneReady(instance: VueFlowStore) {
  flow = instance;
  fitDiagram();
}

// A join added or removed changes the tables shown and the box's height, so the view is fitted again.
watch([() => nodes.value.map((n) => n.id).join("|"), diagramHeight], fitDiagram, { flush: "post" });

const edges = computed<Edge[]>(() =>
  props.model.joins
    .filter((j) => j.from_table !== j.to_table)
    .map((j) => ({
      id: j.key,
      source: j.from_table,
      target: j.to_table,
      label: "many → one",
      markerEnd: isWrong(j)
        ? { type: MarkerType.ArrowClosed, color: "var(--color-warning-dark)" }
        : MarkerType.ArrowClosed,
      animated: j.status !== "accepted",
      style: isWrong(j) ? { stroke: "var(--color-warning-dark)", strokeWidth: 2 } : undefined,
      labelStyle: isWrong(j) ? { fill: "var(--color-warning-dark)" } : undefined,
    })),
);
</script>

<style>
/* Unscoped: a scoped import never reaches VueFlow's own elements, and the diagram stays blank. */
@import "@vue-flow/core/dist/style.css";
@import "@vue-flow/core/dist/theme-default.css";

.semantic-diagram-node.vue-flow__node-default {
  width: auto;
  min-width: 120px;
  padding: 8px 14px;
  border: 1px solid var(--color-border-primary, #cbd5e1);
  border-radius: var(--border-radius-md, 8px);
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary, #1f2937);
  font-size: var(--font-size-sm, 13px);
  font-weight: var(--font-weight-semibold, 600);
  cursor: default;
}
</style>

<style scoped>
.joins-tab {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.hint,
.empty {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.diagram {
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

.remove-btn {
  white-space: nowrap;
}
</style>
