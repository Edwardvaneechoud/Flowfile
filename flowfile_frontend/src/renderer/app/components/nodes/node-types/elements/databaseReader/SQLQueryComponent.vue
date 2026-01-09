// SQLQueryComponent.vue
<template>
  <div class="query-section">
    <h4 class="section-subtitle">SQL Query</h4>
    <div class="form-group">
      <textarea
        id="query"
        :value="modelValue"
        class="form-control textarea"
        placeholder="Enter SQL query"
        rows="4"
        @input="onInput"
      ></textarea>
    </div>
  </div>
</template>

<script lang="ts" setup>
// Props & Emits
defineProps<{
  modelValue: string;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string): void;
}>();

// Methods
const onInput = (event: Event) => {
  const target = event.target as HTMLTextAreaElement;
  emit("update:modelValue", target.value);
};
</script>

<style scoped>
.query-section {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border-light);
}

.section-subtitle {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.form-group {
  margin-bottom: 1rem;
}

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  font-size: 0.875rem;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
}

.textarea {
  resize: vertical;
  min-height: 100px;
  font-family: monospace;
}
</style>
