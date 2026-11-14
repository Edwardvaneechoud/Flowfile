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
import { ElMessage, ElMessageBox } from "element-plus";
import { ProjectFolderEdited } from "../../api/project.api";
import { useProjectStore } from "../../stores/project-store";
import { ProjectUnsavedChanges } from "../../views/ProjectView/unsavedChanges";

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

const save = async (text: string, overwrite = false) => {
  const sha = await store.saveVersion(text, overwrite);
  message.value = "";
  ElMessage.success(sha ? "Version saved" : "Nothing new to save");
  emit("saved", sha);
};

type FolderChoice = "apply" | "overwrite" | null;

const askApplyOrOverwrite = async (edited: ProjectFolderEdited): Promise<FolderChoice> => {
  const lines = edited.changes.map((c) => `${c.kind} “${c.label}”`).join("\n");
  try {
    await ElMessageBox.confirm(
      `${edited.message}\n\n${lines}\n\nApply makes Flowfile match the folder's files, then saves. Overwrite saves what Flowfile holds over those files.`,
      "Files changed outside Flowfile",
      {
        confirmButtonText: "Apply and save",
        cancelButtonText: "Overwrite",
        distinguishCancelAndClose: true,
        type: "warning",
        customStyle: { whiteSpace: "pre-line" },
      },
    );
    return "apply";
  } catch (action) {
    return action === "cancel" ? "overwrite" : null;
  }
};

const confirmReplaceUnsaved = async (unsaved: ProjectUnsavedChanges): Promise<boolean> => {
  const lines = unsaved.changes.map((c) => `${c.kind} “${c.label}”`).join("\n");
  const snapshot = unsaved.databaseSnapshot
    ? "A snapshot of the catalog database is also written to db_backups/ first."
    : "This catalog database is not snapshotted: the recovery point is the only copy, and it holds definitions, not stored passwords.";
  try {
    await ElMessageBox.confirm(
      `Applying the folder replaces these unsaved changes:\n\n${lines}\n\nA recovery point of them is kept first. ${snapshot}\n\nTo keep them instead, cancel and choose Overwrite.`,
      "Unsaved changes",
      {
        confirmButtonText: "Keep a recovery point and apply",
        cancelButtonText: "Cancel",
        type: "warning",
        customStyle: { whiteSpace: "pre-line" },
      },
    );
    return true;
  } catch {
    return false;
  }
};

const applyFolderAndSave = async (text: string) => {
  try {
    await store.reloadFromDisk();
  } catch (e) {
    if (!(e instanceof ProjectUnsavedChanges)) throw e;
    if (!(await confirmReplaceUnsaved(e))) return;
    await store.reloadFromDisk(true);
  }
  await save(text);
};

const onSave = async () => {
  const text = message.value.trim() || "Update";
  try {
    await save(text);
  } catch (e: any) {
    if (!(e instanceof ProjectFolderEdited)) {
      ElMessage.error(e?.message || "Could not save version");
      return;
    }
    const choice = await askApplyOrOverwrite(e);
    if (!choice) return;
    try {
      if (choice === "apply") {
        await applyFolderAndSave(text);
      } else {
        await save(text, true);
      }
    } catch (err: any) {
      ElMessage.error(err?.message || "Could not save version");
    }
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
