<template>
  <div class="inline-text">
    <el-input
      v-if="editing"
      ref="inputRef"
      v-model="draft"
      size="small"
      :type="multiline ? 'textarea' : 'text'"
      :autosize="multiline ? { minRows: 1, maxRows: 5 } : undefined"
      :placeholder="placeholder"
      @keydown.enter.exact.prevent="commit"
      @keydown.esc="editing = false"
      @blur="commit"
    />
    <button
      v-else
      type="button"
      class="inline-display"
      :class="{ empty: !value }"
      :disabled="disabled"
      :title="disabled ? undefined : 'Click to edit'"
      @click="start"
    >
      {{ value || placeholder }}
    </button>
  </div>
</template>

<script setup lang="ts">
import { nextTick, ref } from "vue";

const props = withDefaults(
  defineProps<{
    value: string;
    placeholder?: string;
    disabled?: boolean;
    multiline?: boolean;
  }>(),
  { placeholder: "Add a description", disabled: false, multiline: false },
);
const emit = defineEmits<(e: "commit", value: string) => void>();

const editing = ref(false);
const draft = ref("");
const inputRef = ref<{ focus: () => void } | null>(null);

async function start() {
  if (props.disabled) return;
  draft.value = props.value;
  editing.value = true;
  await nextTick();
  inputRef.value?.focus();
}

function commit() {
  if (!editing.value) return;
  editing.value = false;
  const next = draft.value.trim();
  if (next !== props.value.trim()) emit("commit", next);
}
</script>

<style scoped>
.inline-text {
  min-width: 0;
}

.inline-display {
  display: block;
  width: 100%;
  padding: 2px var(--spacing-2);
  border: 1px solid transparent;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-primary);
  font: inherit;
  font-size: var(--font-size-sm);
  text-align: left;
  cursor: text;
}

.inline-display:hover:not(:disabled) {
  border-color: var(--color-border-primary);
  background: var(--color-background-hover);
}

.inline-display.empty {
  color: var(--color-text-muted);
}

.inline-display:disabled {
  cursor: default;
}
</style>
