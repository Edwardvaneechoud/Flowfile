<template>
  <div class="component-container">
    <label class="listbox-subtitle">{{ schema.label }}</label>
    <FilePathInput
      :model-value="modelValue ?? ''"
      :placeholder="schema.placeholder || 'Path to a file'"
      :mode="schema.mode ?? 'open'"
      :allowed-file-types="schema.file_types ?? []"
      :allow-directory-selection="schema.allow_directory ?? false"
      @update:model-value="$emit('update:modelValue', $event)"
    />
  </div>
</template>

<script setup lang="ts">
import type { PropType } from "vue";
import type { FilePickerComponent } from "../interface";
import FilePathInput from "../../../../../common/FileBrowser/FilePathInput.vue";

defineProps({
  schema: {
    type: Object as PropType<FilePickerComponent>,
    required: true,
  },
  modelValue: {
    type: String as PropType<string | null>,
    default: "",
  },
});

defineEmits(["update:modelValue"]);
</script>

<style scoped>
/* .component-item is a flex row whose children don't grow; fill it explicitly. */
.component-container {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  width: 100%;
  min-width: 0;
}
</style>
