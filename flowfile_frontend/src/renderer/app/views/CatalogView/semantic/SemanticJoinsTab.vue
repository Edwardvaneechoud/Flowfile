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
          <code>
            <button
              v-if="join.from_table_id !== null"
              class="table-link"
              type="button"
              :title="`Open ${join.from_table}`"
              @click="router.push(tablePageLocation(join.from_table_id))"
            >
              {{ join.from_table }}
            </button>
            <template v-else>{{ join.from_table }}</template
            >.{{ join.from_columns.join(", ") }}
          </code>
          <i class="fa-solid fa-arrow-right"></i>
          <code>
            <button
              v-if="join.to_table_id !== null"
              class="table-link"
              type="button"
              :title="`Open ${join.to_table}`"
              @click="router.push(tablePageLocation(join.to_table_id))"
            >
              {{ join.to_table }}
            </button>
            <template v-else>{{ join.to_table }}</template
            >.{{ join.to_columns.join(", ") }}
          </code>
        </div>
        <p v-if="join.description" class="join-desc">{{ join.description }}</p>
        <div
          v-if="checkOf(join) && checkOf(join)?.verdict !== 'ok'"
          class="join-check"
          :class="checkOf(join)?.verdict === 'unchecked' ? 'join-check-quiet' : 'join-check-warn'"
          data-testid="semantic-join-check"
        >
          <i
            v-if="checkOf(join)?.verdict !== 'unchecked'"
            class="fa-solid fa-triangle-exclamation"
          ></i>
          <span>{{ checkOf(join)?.message }}</span>
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
        </div>
      </li>
    </ul>

    <SemanticJoinDialog v-model="showDialog" :namespace-id="model.namespace_id" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { MarkerType, Position, VueFlow, type Edge, type Node } from "@vue-flow/core";
import type { JoinCheck, SemanticJoin, SemanticModel } from "../../../types/semantic.types";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import SemanticJoinDialog from "./SemanticJoinDialog.vue";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import { errorText, tablePageLocation } from "./semanticFormat";

const props = defineProps<{ model: SemanticModel }>();
const router = useRouter();
const store = useSemanticModelStore();
const showDialog = ref(false);
const fixing = ref<string | null>(null);

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

const nodes = computed<Node[]>(() => {
  const rows = new Map<number, number>();
  return tableNames.value.map((name) => {
    const col = columns.value.get(name) ?? 0;
    const row = rows.get(col) ?? 0;
    rows.set(col, row + 1);
    return {
      id: name,
      label: shortNames.value.get(name) ?? name,
      position: { x: col * 280, y: row * 80 },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      class: "semantic-diagram-node",
    };
  });
});

const edges = computed<Edge[]>(() =>
  props.model.joins.map((j) => ({
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

.table-link {
  padding: 0;
  border: none;
  background: transparent;
  color: var(--color-primary);
  font: inherit;
  cursor: pointer;
  text-decoration: underline;
  text-decoration-style: dotted;
}

.table-link:hover {
  text-decoration-style: solid;
}

.join-check {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-2);
  margin-top: var(--spacing-2);
  padding-left: var(--spacing-3);
  font-size: var(--font-size-xs);
}

.join-check-warn {
  border-left: 3px solid var(--color-warning-dark);
  color: var(--color-warning-dark);
}

.join-check-quiet {
  border-left: 3px solid var(--color-border-light);
  color: var(--color-text-muted);
}

.join-check span {
  flex: 1 1 240px;
}

.join-desc {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
</style>
