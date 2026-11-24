<template>
  <el-tooltip :content="tooltip" :disabled="!tooltip" placement="top" :show-after="300">
    <span v-if="state" class="status-chip" :class="`is-${state}`">{{ label }}</span>
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { SemanticFact, SemanticFactStatus } from "../../../types/semantic.types";
import { chipState, sourceHint } from "./semanticFormat";

const props = defineProps<{ status: SemanticFactStatus; fact?: SemanticFact }>();

const state = computed(() => chipState(props.status));
const label = computed(() => state.value ?? "");
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
