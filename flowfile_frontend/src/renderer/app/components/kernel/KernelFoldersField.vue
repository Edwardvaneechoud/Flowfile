<template>
  <div class="form-field kernel-folders">
    <label class="form-label">
      Folders this kernel can read
      <span class="form-label-hint">(optional)</span>
    </label>
    <div v-for="(folder, i) in modelValue" :key="i" class="kernel-folders__row">
      <input
        :value="folder"
        type="text"
        class="form-input"
        placeholder="/Users/me/data"
        :aria-label="`Folder ${i + 1}`"
        :disabled="disabled"
        @input="update(i, ($event.target as HTMLInputElement).value)"
      />
      <button
        type="button"
        class="btn btn-secondary btn-sm"
        aria-label="Remove folder"
        :disabled="disabled"
        @click="remove(i)"
      >
        <i class="fa-solid fa-xmark"></i>
      </button>
    </div>
    <button type="button" class="btn btn-secondary btn-sm" :disabled="disabled" @click="add">
      <i class="fa-solid fa-plus"></i> Add folder
    </button>
    <p class="form-help">
      Absolute paths on this machine, mounted read-only at the same path. Code on this kernel
      (notebook cells, Python Script nodes) can read them but never change them.
    </p>
  </div>
</template>

<script setup lang="ts">
const props = defineProps<{ modelValue: string[]; disabled?: boolean }>();
const emit = defineEmits<{ (e: "update:modelValue", value: string[]): void }>();

const update = (index: number, value: string) =>
  emit(
    "update:modelValue",
    props.modelValue.map((f, i) => (i === index ? value : f)),
  );
const remove = (index: number) =>
  emit(
    "update:modelValue",
    props.modelValue.filter((_, i) => i !== index),
  );
const add = () => emit("update:modelValue", [...props.modelValue, ""]);
</script>

<style scoped>
.kernel-folders__row {
  display: flex;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
}
.kernel-folders__row .form-input {
  flex: 1;
}
.kernel-folders > .btn {
  align-self: flex-start;
}
.form-label-hint {
  font-weight: var(--font-weight-normal);
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}
.form-help {
  margin: var(--spacing-1) 0 0;
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}
</style>
