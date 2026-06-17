<template>
  <div v-if="spec && spec.length" class="dynamic-params">
    <div v-for="param in spec" :key="param.name" class="param-row">
      <label :for="`param-${param.name}`" class="param-label">
        {{ param.label }}
      </label>

      <!-- boolean -->
      <el-switch
        v-if="param.type === 'boolean'"
        :id="`param-${param.name}`"
        size="small"
        :model-value="(modelValue[param.name] as boolean) ?? (param.default as boolean)"
        @update:model-value="updateValue(param.name, $event)"
      />

      <!-- number / integer -->
      <el-input-number
        v-else-if="param.type === 'number' || param.type === 'integer'"
        :id="`param-${param.name}`"
        :model-value="(modelValue[param.name] as number) ?? (param.default as number)"
        :min="param.min ?? undefined"
        :max="param.max ?? undefined"
        :step="param.step ?? (param.type === 'integer' ? 1 : 0.01)"
        :precision="param.type === 'integer' ? 0 : undefined"
        controls-position="right"
        @update:model-value="updateValue(param.name, $event)"
      />

      <!-- select -->
      <el-select
        v-else-if="param.type === 'select'"
        :id="`param-${param.name}`"
        :model-value="(modelValue[param.name] as string) ?? (param.default as string)"
        @update:model-value="updateValue(param.name, $event)"
      >
        <el-option v-for="opt in param.options ?? []" :key="opt" :label="opt" :value="opt" />
      </el-select>

      <span v-if="param.description" class="param-help">{{ param.description }}</span>
    </div>
  </div>
  <p v-else class="param-empty">This algorithm has no tunable hyperparameters.</p>
</template>

<script lang="ts" setup>
import type { MLParamSpec } from "../../../../../types/node.types";

interface Props {
  spec: MLParamSpec[];
  modelValue: Record<string, unknown>;
}

const props = defineProps<Props>();
const emit = defineEmits<(e: "update:modelValue", val: Record<string, unknown>) => void>();

function updateValue(name: string, value: unknown) {
  emit("update:modelValue", { ...props.modelValue, [name]: value });
}
</script>

<style scoped>
.dynamic-params {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}

.param-row {
  display: grid;
  grid-template-columns: 160px 1fr;
  align-items: center;
  gap: var(--spacing-2);
}

.param-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.param-help {
  grid-column: 1 / -1;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  margin-top: -2px;
}

.param-empty {
  color: var(--color-text-secondary);
  font-style: italic;
  font-size: var(--font-size-sm);
}
</style>
