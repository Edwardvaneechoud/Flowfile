<template>
  <div class="code-editor-section">
    <h4>Process Method</h4>
    <p class="code-hint">
      Write your data transformation logic. Access settings via
      <code>self.settings_schema.section_name.component_name.value</code>
    </p>
    <div class="code-editor-wrapper">
      <Codemirror
        :model-value="modelValue"
        placeholder="# Write your process logic here..."
        :style="{ height: '300px' }"
        :autofocus="false"
        :indent-with-tab="false"
        :tab-size="4"
        :extensions="extensions"
        @update:model-value="emit('update:modelValue', $event)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { Codemirror } from "vue-codemirror";
import type { Extension } from "@codemirror/state";

defineProps<{
  modelValue: string;
  extensions: Extension[];
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string): void;
}>();
</script>

<style scoped>
.code-editor-section {
  margin-top: 1rem;
  margin-bottom: 1.5rem;
  padding: 1.25rem;
  background: var(--bg-secondary, #f8f9fa);
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 8px;
}

.code-editor-section h4 {
  margin: 0 0 0.5rem 0;
  font-size: 1rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.code-hint {
  margin: 0 0 1rem 0;
  font-size: 0.8125rem;
  color: var(--text-secondary, #6c757d);
  line-height: 1.5;
}

.code-hint code {
  background: var(--card-bg, #ffffff);
  padding: 0.2rem 0.5rem;
  border-radius: 4px;
  font-size: 0.75rem;
  border: 1px solid var(--border-color, #e0e0e0);
  font-family: "Fira Code", "Monaco", monospace;
}

.code-editor-wrapper {
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 6px;
  overflow: hidden;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}
</style>
