<template>
  <div class="save-version-form">
    <el-input
      v-model="message"
      type="textarea"
      :rows="rows"
      :placeholder="placeholder"
      resize="none"
      @keydown.meta.enter="onSave"
      @keydown.ctrl.enter="onSave"
    />
    <div class="save-version-form__actions">
      <span class="save-version-form__hint">{{ statusHint }}</span>
      <el-button type="primary" size="small" :loading="store.saving" @click="onSave">
        Save version
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import { ElMessage } from "element-plus";
import { useProjectStore } from "../../stores/project-store";

withDefaults(
  defineProps<{
    rows?: number;
    placeholder?: string;
  }>(),
  {
    rows: 2,
    placeholder: "What changed? e.g. 'Added monthly sales flow'",
  },
);

const emit = defineEmits<{ (e: "saved", sha: string | null): void }>();

const store = useProjectStore();
const message = ref("");

const statusHint = computed(() => {
  if (store.status === "external") return "Files also changed outside Flowfile.";
  if (store.status === "clean") return "Everything is already saved.";
  return "You have unsaved changes.";
});

const onSave = async () => {
  try {
    const sha = await store.saveVersion(message.value.trim() || "Update");
    message.value = "";
    ElMessage.success(sha ? "Version saved" : "Nothing new to save");
    emit("saved", sha);
  } catch (e: any) {
    ElMessage.error(e?.message || "Could not save version");
  }
};
</script>

<style scoped>
.save-version-form {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.save-version-form__actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.save-version-form__hint {
  font-size: 12px;
  color: var(--color-text-tertiary, #94a3b8);
}
</style>
