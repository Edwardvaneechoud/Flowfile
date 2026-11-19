<template>
  <el-dialog
    :model-value="modelValue"
    :title="hadNote ? 'Edit note' : 'Add note'"
    width="640px"
    append-to-body
    draggable
    class="tile-note-dialog"
    data-testid="tile-note-dialog"
    @update:model-value="emit('update:modelValue', $event)"
    @opened="textareaRef?.focus()"
  >
    <p class="note-hint">
      Readers open it from the ⓘ beside the tile's title: a definition, a source or a caveat.
      Markdown works as in a text block; an empty note removes it.
    </p>
    <div class="note-columns">
      <el-input
        ref="textareaRef"
        v-model="draft"
        type="textarea"
        :autosize="{ minRows: 8, maxRows: 14 }"
        :maxlength="NOTE_MAX_LENGTH"
        show-word-limit
        resize="none"
        placeholder="**Revenue** is net of refunds. [Definition](https://…)"
        data-testid="tile-note-input"
      />
      <div class="note-preview" data-testid="tile-note-preview">
        <div class="note-preview-label">Preview</div>
        <TileNoteBody v-if="draft.trim()" :markdown="draft" />
        <div v-else class="note-preview-empty">No note</div>
      </div>
    </div>
    <template #footer>
      <el-button @click="emit('update:modelValue', false)">Cancel</el-button>
      <el-button type="primary" data-testid="tile-note-save" @click="onSave">Save</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import type { InputInstance } from "element-plus";
import TileNoteBody from "./TileNoteBody.vue";
import { NOTE_MAX_LENGTH } from "./tileNote";

/** The editor's note dialog: a Markdown text area beside its live preview; Save hands back the draft. */
const props = defineProps<{
  modelValue: boolean;
  note: string | null;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "save", draft: string): void;
}>();

const draft = ref("");
const hadNote = ref(false);
const textareaRef = ref<InputInstance | null>(null);

watch(
  () => props.modelValue,
  (open) => {
    if (!open) return;
    draft.value = props.note ?? "";
    hadNote.value = !!props.note;
  },
  { immediate: true },
);

const onSave = () => {
  emit("save", draft.value);
  emit("update:modelValue", false);
};
</script>

<style scoped>
.note-hint {
  margin: 0 0 10px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.note-columns {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.note-columns :deep(textarea) {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 12.5px;
}
.note-preview {
  min-width: 0;
  max-height: 320px;
  overflow: auto;
  padding: 8px 12px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 4px;
  background: var(--el-fill-color-blank);
}
.note-preview-label {
  margin-bottom: 6px;
  font-size: 11px;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--el-text-color-secondary);
}
.note-preview-empty {
  font-size: 12px;
  color: var(--el-text-color-placeholder);
}
@media (max-width: 680px) {
  .note-columns {
    grid-template-columns: 1fr;
  }
}
</style>
