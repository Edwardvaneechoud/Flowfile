<template>
  <div v-if="filtered.length === 0" class="empty-hint">
    {{ parameters.length ? "No matching parameters" : "No flow parameters defined" }}
  </div>
  <div v-for="param in filtered" :key="param.name" class="cool-button-container">
    <button class="cool-button" :title="paramTitle(param)" @click="handleClick(param)">
      <span class="col-name">{{ param.name }}</span>
      <span class="type-badge">{{ shortType(param.type) }}</span>
    </button>
  </div>
</template>

<script lang="ts" setup>
import { computed } from "vue";
import type { FlowParameter } from "../../../../types/flow.types";
import { paramInsertText, shortType, PARAM_INSERT_VARIANT } from "../paramTokens";

const props = defineProps<{ filterText?: string; parameters?: FlowParameter[] }>();

const emit = defineEmits<{
  (event: "value-selected", payload: string): void;
}>();

const parameters = computed<FlowParameter[]>(() => props.parameters ?? []);

const filtered = computed<FlowParameter[]>(() => {
  const q = props.filterText?.trim().toLowerCase() ?? "";
  if (!q) return parameters.value;
  return parameters.value.filter((p) => p.name.toLowerCase().includes(q));
});

const paramTitle = (param: FlowParameter): string => {
  const parts = [`${param.name} (${param.type ?? "string"})`];
  if (param.default_value !== "") parts.push(`default: ${param.default_value}`);
  if (param.description) parts.push(param.description);
  return parts.join(" — ");
};

const handleClick = (param: FlowParameter) => {
  emit("value-selected", paramInsertText(param, PARAM_INSERT_VARIANT));
};
</script>

<style scoped>
.cool-button-container {
  display: flex;
  flex-direction: column;
  align-items: stretch;
}

.cool-button {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
  width: 100%;
  border: none;
  color: var(--color-text-primary);
  background-color: transparent;
  padding: 3px 5px;
  text-align: left;
  margin: 1px 0;
  cursor: pointer;
  border-radius: 4px;
}

.cool-button:hover {
  background-color: var(--color-background-hover);
}

.col-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.type-badge {
  flex-shrink: 0;
  font-size: 10px;
  font-weight: 500;
  line-height: 1.6;
  padding: 0 6px;
  border-radius: 999px;
  white-space: nowrap;
  color: var(--color-accent);
  background-color: var(--color-accent-subtle);
}

.empty-hint {
  padding: 6px 5px;
  font-size: 12px;
  font-style: italic;
  color: var(--color-text-muted);
}
</style>
