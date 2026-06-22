<template>
  <el-dialog
    :model-value="modelValue"
    width="520px"
    align-center
    :close-on-click-modal="false"
    :show-close="!restoring"
    @update:model-value="onVisibility"
    @closed="onClosed"
  >
    <template #header>
      <div class="restore-head">
        <i class="fa-solid fa-clock-rotate-left"></i>
        <span>Restore version</span>
      </div>
    </template>

    <div v-if="version" class="restore-body">
      <p class="restore-target">
        Restore to <strong>“{{ version.message }}”</strong>
      </p>

      <div v-if="loadingChanges" class="restore-loading">Checking what will change…</div>

      <template v-else>
        <div v-if="changes.length" class="restore-changes">
          <p class="restore-changes__caption">
            This will change the following, compared to your latest saved version:
          </p>
          <ChangeList :changes="changes" mode="restore" />
          <p class="restore-warning">
            <i class="fa-solid fa-triangle-exclamation"></i>
            Anything added since then will be removed. A new “Restored” version is saved, so you can
            always come back.
          </p>
        </div>

        <p v-else class="restore-empty">
          This version matches your current state — nothing would change.
        </p>
      </template>
    </div>

    <template #footer>
      <el-button :disabled="restoring" @click="close">Cancel</el-button>
      <el-button
        type="primary"
        :loading="restoring"
        :disabled="loadingChanges || !version"
        @click="confirm"
      >
        Restore
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { ElMessage, ElNotification } from "element-plus";
import ChangeList from "./ChangeList.vue";
import { useProjectStore } from "../../stores/project-store";
import type { ProjectVersion, ProjectVersionChange } from "../../types";

const props = defineProps<{ modelValue: boolean; version: ProjectVersion | null }>();
const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
  (e: "restored"): void;
}>();

const store = useProjectStore();
const changes = ref<ProjectVersionChange[]>([]);
const loadingChanges = ref(false);
const restoring = ref(false);

watch(
  () => props.modelValue,
  async (open) => {
    if (!open || !props.version) return;
    loadingChanges.value = true;
    changes.value = [];
    try {
      changes.value = await store.loadVersionChanges(props.version.sha);
    } catch (e: any) {
      ElMessage.error(e?.message || "Could not load changes");
    } finally {
      loadingChanges.value = false;
    }
  },
);

const confirm = async () => {
  if (!props.version) return;
  restoring.value = true;
  try {
    await store.restoreVersion(props.version.sha, props.version.message);
    ElNotification({
      title: "Version restored",
      message: "Reopen any flows you had open to see the restored version.",
      type: "success",
      position: "top-left",
      duration: 6000,
    });
    emit("restored");
    close();
  } catch (e: any) {
    ElMessage.error(e?.message || "Could not restore version");
  } finally {
    restoring.value = false;
  }
};

const onVisibility = (v: boolean) => {
  if (!restoring.value) emit("update:modelValue", v);
};
const close = () => emit("update:modelValue", false);
const onClosed = () => {
  changes.value = [];
};
</script>

<style scoped>
.restore-head {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
}

.restore-head i {
  color: var(--color-accent, #2563eb);
}

.restore-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.restore-target {
  margin: 0;
  font-size: 14px;
  color: var(--color-text-secondary, #475569);
}

.restore-loading,
.restore-empty {
  margin: 0;
  font-size: 13px;
  color: var(--color-text-tertiary, #94a3b8);
}

.restore-changes__caption {
  margin: 0 0 8px;
  font-size: 13px;
  color: var(--color-text-secondary, #475569);
}

.restore-warning {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  margin: 4px 0 0;
  padding: 8px 10px;
  border-radius: 6px;
  background: color-mix(in srgb, var(--color-warning, #d97706) 10%, transparent);
  font-size: 12px;
  line-height: 1.45;
  color: var(--color-text-secondary, #475569);
}

.restore-warning i {
  color: var(--color-warning, #d97706);
  margin-top: 2px;
}
</style>
