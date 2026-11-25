<template>
  <el-tooltip
    v-if="shown"
    :content="tooltip"
    :disabled="!tooltip"
    placement="top"
    :show-after="300"
  >
    <span class="status-chip" :class="`is-${state}`">{{ state }}</span>
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { SemanticFact, SemanticFactStatus } from "../../../types/semantic.types";
import { chipState, needsReview, sourceHint } from "./semanticFormat";

/** The state of a `status`, or of a `fact` while it still needs a person. */
const props = defineProps<{ status?: SemanticFactStatus; fact?: SemanticFact }>();

const status = computed(() => props.status ?? props.fact?.status);
const state = computed(() => status.value && chipState(status.value));
const shown = computed(() => (props.status ? !!state.value : needsReview(props.fact)));
const tooltip = computed(() => {
  if (state.value === "pending" && props.fact) return sourceHint(props.fact);
  if (state.value === "stale")
    return "The column changed since this was accepted. Review it again.";
  return "";
});
</script>

<style scoped>
.status-chip {
  display: inline-flex;
  align-items: center;
  padding: 0 var(--spacing-2);
  border-radius: 999px;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  line-height: 18px;
  white-space: nowrap;
}

.is-accepted {
  background: var(--color-success-light);
  color: var(--color-success-dark);
}

.is-pending {
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
}

.is-stale {
  background: var(--color-danger-light);
  color: var(--color-danger-dark);
}
</style>
