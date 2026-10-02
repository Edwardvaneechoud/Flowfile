<template>
  <div class="form-field kernel-folders">
    <label class="form-label">
      Folders this kernel can read
      <span class="form-label-hint">(optional)</span>
    </label>
    <div v-for="(folder, i) in modelValue" :key="i" class="kernel-folders__row">
      <input
        :value="folderPath(folder)"
        type="text"
        class="form-input"
        placeholder="/Users/me/data"
        :aria-label="`Folder ${i + 1}`"
        :disabled="disabled"
        @input="
          update(i, folderEntry(($event.target as HTMLInputElement).value, isWritable(folder)))
        "
      />
      <label class="kernel-folders__writable">
        <input
          type="checkbox"
          :checked="isWritable(folder)"
          :aria-label="`Folder ${i + 1} writable`"
          :disabled="disabled"
          @change="
            update(i, folderEntry(folderPath(folder), ($event.target as HTMLInputElement).checked))
          "
        />
        Writable
      </label>
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
    <p v-if="anyWritable" class="warning-text">
      Code on this kernel can change or delete files in a writable folder.
    </p>
    <p class="form-help">
      Absolute paths on this machine. Each appears inside the kernel at the same path, or under
      /host/&lt;drive&gt;/ for a Windows path (C:\data is /host/c/data). The kernel can change files
      only in folders marked Writable.
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { MountedFolderEntry } from "../../types";
import { folderEntry, folderPath, isWritable } from "./kernelFolders";

const props = defineProps<{ modelValue: MountedFolderEntry[]; disabled?: boolean }>();
const emit = defineEmits<{ (e: "update:modelValue", value: MountedFolderEntry[]): void }>();

const anyWritable = computed(() => props.modelValue.some(isWritable));

const update = (index: number, value: MountedFolderEntry) =>
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
.kernel-folders__writable {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  white-space: nowrap;
  font-size: var(--font-size-sm);
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
