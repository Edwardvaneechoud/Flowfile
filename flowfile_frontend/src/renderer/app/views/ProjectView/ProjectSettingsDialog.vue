<template>
  <el-dialog
    :model-value="modelValue"
    width="520px"
    align-center
    :close-on-click-modal="false"
    :show-close="!saving"
    @update:model-value="onVisibility"
  >
    <template #header>
      <div class="settings-head">
        <i class="fa-solid fa-gear"></i>
        <span>Project settings</span>
      </div>
    </template>

    <div class="settings-body">
      <div class="setting-row">
        <div class="setting-text">
          <p class="setting-title">Track data artifacts</p>
          <p class="setting-desc">
            Version catalog tables, dashboards and ML models alongside your flows. When off, only
            flows, connections and schedules are saved, and existing data-artifact files are dropped
            from the project folder.
          </p>
        </div>
        <el-switch v-model="trackDataArtifacts" :disabled="saving" />
      </div>
    </div>

    <template #footer>
      <el-button :disabled="saving" @click="close">Cancel</el-button>
      <el-button type="primary" :loading="saving" :disabled="!dirty" @click="save">Save</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { useProjectStore } from "../../stores/project-store";

const props = defineProps<{ modelValue: boolean }>();
const emit = defineEmits<{ (e: "update:modelValue", value: boolean): void }>();

const store = useProjectStore();
const trackDataArtifacts = ref(true);
const saving = ref(false);

const dirty = computed(
  () => trackDataArtifacts.value !== (store.activeProject?.track_data_artifacts ?? true),
);

watch(
  () => props.modelValue,
  (open) => {
    if (open) trackDataArtifacts.value = store.activeProject?.track_data_artifacts ?? true;
  },
);

const save = async () => {
  saving.value = true;
  try {
    await store.updateSettings(trackDataArtifacts.value);
    ElMessage.success("Project settings updated");
    close();
  } catch (e: any) {
    ElMessage.error(e?.message || "Could not update project settings");
  } finally {
    saving.value = false;
  }
};

const onVisibility = (v: boolean) => {
  if (!saving.value) emit("update:modelValue", v);
};
const close = () => emit("update:modelValue", false);
</script>

<style scoped>
.settings-head {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
}

.settings-head i {
  color: var(--color-accent, #2563eb);
}

.setting-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-4, 16px);
}

.setting-text {
  flex: 1;
  min-width: 0;
}

.setting-title {
  margin: 0 0 4px;
  font-size: 14px;
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-primary, #0f172a);
}

.setting-desc {
  margin: 0;
  font-size: 13px;
  line-height: 1.5;
  color: var(--color-text-secondary, #475569);
}
</style>
