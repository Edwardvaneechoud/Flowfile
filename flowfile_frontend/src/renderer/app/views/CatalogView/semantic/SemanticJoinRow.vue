<template>
  <li class="join-row" data-testid="semantic-join-row">
    <div class="join-main">
      <span class="join-name">{{ join.name }}</span>
      <span v-if="showLayer" class="layer-badge" :class="`layer-${join.layer}`">{{
        join.layer
      }}</span>
      <SemanticStatusChip v-if="pending" :status="join.status" />
      <div class="join-actions">
        <slot name="actions" />
      </div>
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
          {{ label(join.from_table) }}
        </button>
        <template v-else>{{ label(join.from_table) }}</template
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
          {{ label(join.to_table) }}
        </button>
        <template v-else>{{ label(join.to_table) }}</template
        >.{{ join.to_columns.join(", ") }}
      </code>
    </div>
    <p v-if="join.description" class="join-desc">{{ join.description }}</p>
    <div
      v-if="check && check.verdict !== 'ok'"
      class="join-check"
      :class="check.verdict === 'unchecked' ? 'join-check-quiet' : 'join-check-warn'"
      data-testid="semantic-join-check"
    >
      <i v-if="check.verdict !== 'unchecked'" class="fa-solid fa-triangle-exclamation"></i>
      <span>{{ check.message }}</span>
      <slot name="check-actions" />
    </div>
  </li>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useRouter } from "vue-router";
import type { JoinCheck, SemanticJoin } from "../../../types/semantic.types";
import SemanticStatusChip from "./SemanticStatusChip.vue";
import { chipState, tablePageLocation } from "./semanticFormat";

const props = withDefaults(
  defineProps<{
    join: SemanticJoin;
    check?: JoinCheck;
    showLayer?: boolean;
    tableLabel?: (key: string) => string;
  }>(),
  { check: undefined, showLayer: true, tableLabel: undefined },
);

const router = useRouter();

const label = (key: string) => (props.tableLabel ? props.tableLabel(key) : key);

// An accepted join shows no chip; one still waiting for a person does.
const pending = computed(() => {
  const state = chipState(props.join.status);
  return state === "pending" || state === "stale";
});
</script>

<style scoped>
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

.join-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
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
