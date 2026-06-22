<template>
  <div class="project-view">
    <div v-if="!ready" class="project-loading">
      <div class="loading-spinner"></div>
    </div>
    <ProjectDisabled v-else-if="!projectsEnabled" />
    <ProjectDisabled v-else-if="store.featureUnavailable" admin-only />
    <template v-else>
      <ProjectIntro
        v-if="!store.isActive"
        @create="openPicker('create')"
        @open="openPicker('open')"
      />
      <ProjectManage v-else />

      <ProjectFolderPickerDialog v-model="pickerVisible" :mode="pickerMode" @done="onPicked" />
    </template>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from "vue";
import ProjectIntro from "./ProjectIntro.vue";
import ProjectManage from "./ProjectManage.vue";
import ProjectDisabled from "./ProjectDisabled.vue";
import ProjectFolderPickerDialog from "./ProjectFolderPickerDialog.vue";
import { useProjectStore } from "../../stores/project-store";
import { useMultiUser } from "../../composables/useMultiUser";

const store = useProjectStore();
const { projectsEnabled, refresh } = useMultiUser();
const ready = ref(false);
const pickerVisible = ref(false);
const pickerMode = ref<"create" | "open">("create");

onMounted(async () => {
  await refresh();
  if (projectsEnabled.value) {
    await store.refreshActive();
    if (store.isActive) store.loadVersions();
  }
  ready.value = true;
});

const openPicker = (mode: "create" | "open") => {
  pickerMode.value = mode;
  pickerVisible.value = true;
};

const onPicked = () => {
  store.loadVersions();
};
</script>

<style scoped>
.project-loading {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 240px;
}

.loading-spinner {
  width: 28px;
  height: 28px;
  border: 3px solid var(--color-border-primary, #e2e8f0);
  border-top-color: var(--color-accent, #2563eb);
  border-radius: 50%;
  animation: project-spin 0.8s linear infinite;
}

@keyframes project-spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
