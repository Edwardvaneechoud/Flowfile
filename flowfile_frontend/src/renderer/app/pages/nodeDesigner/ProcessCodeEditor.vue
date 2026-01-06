<template>
  <div class="code-editor-section">
    <div class="code-editor-header">
      <h4>Process Method</h4>
      <button class="help-btn" title="Show help" @click="showHelp = true">
        <i class="fa-solid fa-circle-question"></i>
        <span>Help</span>
      </button>
    </div>
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

    <ProcessCodeHelpModal :show="showHelp" @close="showHelp = false" />
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { Codemirror } from "vue-codemirror";
import type { Extension } from "@codemirror/state";
import ProcessCodeHelpModal from "./ProcessCodeHelpModal.vue";

defineProps<{
  modelValue: string;
  extensions: Extension[];
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string): void;
}>();

const showHelp = ref(false);
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

.code-editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.5rem;
}

.code-editor-header h4 {
  margin: 0;
  font-size: 1rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.help-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.75rem;
  background: var(--color-accent, #0891b2);
  border: none;
  border-radius: 4px;
  color: white;
  font-size: 0.8125rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.15s ease;
}

.help-btn:hover {
  background: var(--color-accent-hover, #0e7490);
}

.help-btn i {
  font-size: 0.875rem;
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
