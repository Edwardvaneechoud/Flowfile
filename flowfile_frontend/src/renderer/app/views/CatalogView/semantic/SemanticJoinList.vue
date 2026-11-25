<template>
  <ul class="list">
    <li v-for="join in joins" :key="join.key" class="box" data-testid="semantic-join-row">
      <div class="line">
        <span class="name">{{ join.name }}</span>
        <span v-if="showLayer" class="layer-badge" :class="`layer-${join.layer}`">{{
          join.layer
        }}</span>
        <SemanticStatusChip v-if="needsReview(join)" :status="join.status" />
        <div class="line join-actions">
          <slot name="actions" :join="join" :removing="removing !== null" />
          <button
            v-if="join.can_remove"
            type="button"
            class="btn btn-ghost btn-danger btn-sm"
            data-testid="semantic-join-remove"
            :disabled="busy || removing !== null"
            @click="removeJoin(namespaceId, join)"
          >
            <i class="fa-solid fa-trash"></i>
            {{ removing === join.key ? "Removing…" : "Remove" }}
          </button>
        </div>
      </div>
      <div class="line join-sides">
        <template v-for="(side, i) in sidesOf(join)" :key="i">
          <i v-if="i > 0" class="fa-solid fa-arrow-right"></i>
          <code>
            <button
              v-if="side.id !== null"
              class="table-link"
              type="button"
              :title="`Open ${side.table}`"
              @click="router.push(tablePageLocation(side.id))"
            >
              {{ tableLabel(side.table) }}
            </button>
            <template v-else>{{ tableLabel(side.table) }}</template
            >.{{ side.columns.join(", ") }}
          </code>
        </template>
      </div>
      <p v-if="join.description" class="hint join-desc">{{ join.description }}</p>
      <div
        v-if="warningOf(join)"
        class="line join-check"
        :class="warningOf(join)?.verdict === 'unchecked' ? 'join-check-quiet' : 'warn-bar'"
        data-testid="semantic-join-check"
      >
        <i
          v-if="warningOf(join)?.verdict !== 'unchecked'"
          class="fa-solid fa-triangle-exclamation"
        ></i>
        <span>{{ warningOf(join)?.message }}</span>
        <slot name="check-actions" :join="join" />
      </div>
    </li>
  </ul>
</template>

<script setup lang="ts">
import { computed, watch } from "vue";
import { useRouter } from "vue-router";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import type { SemanticJoin } from "../../../types/semantic.types";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import { needsReview, tablePageLocation } from "./semanticFormat";
import { useSemanticRemoval } from "./semanticRemoval";

/** The open model's joins with their sides, the check against the data and Remove. */
const props = withDefaults(
  defineProps<{
    joins: SemanticJoin[];
    namespaceId: number;
    busy?: boolean;
    showLayer?: boolean;
    tableLabel?: (key: string) => string;
  }>(),
  { busy: false, showLayer: true, tableLabel: (key: string) => key },
);

const router = useRouter();
const store = useSemanticModelStore();
const { removing, removeJoin } = useSemanticRemoval();

const sidesOf = (j: SemanticJoin) => [
  { id: j.from_table_id, table: j.from_table, columns: j.from_columns },
  { id: j.to_table_id, table: j.to_table, columns: j.to_columns },
];

const warningOf = (join: SemanticJoin) => {
  const check = store.joinChecks[join.key];
  return check?.verdict === "ok" ? undefined : check;
};

// Measured when the list opens and whenever a join's ends, columns or status change.
const signature = computed(() =>
  JSON.stringify([props.namespaceId, props.joins.map((j) => [j.key, sidesOf(j), j.status])]),
);
watch(signature, () => void store.checkJoins(), { immediate: true });
</script>

<style scoped src="./semantic.css"></style>

<style scoped>
.join-actions {
  margin-left: auto;
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
  flex-wrap: wrap;
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
  flex-wrap: wrap;
  margin-top: var(--spacing-2);
  padding-left: var(--spacing-3);
  font-size: var(--font-size-xs);
}

.join-check-quiet {
  border-left: 3px solid var(--color-border-light);
  color: var(--color-text-muted);
}

.join-check span {
  flex: 1 1 240px;
}

.join-desc {
  margin-top: var(--spacing-2);
}
</style>
