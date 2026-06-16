<template>
  <div class="project-view">
    <ProjectIntro
      v-if="!store.isActive"
      @create="openPicker('create')"
      @open="openPicker('open')"
    />
    <ProjectManage v-else />

    <ProjectFolderPickerDialog v-model="pickerVisible" :mode="pickerMode" @done="onPicked" />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from "vue";
import ProjectIntro from "./ProjectIntro.vue";
import ProjectManage from "./ProjectManage.vue";
import ProjectFolderPickerDialog from "./ProjectFolderPickerDialog.vue";
import { useProjectStore } from "../../stores/project-store";

const store = useProjectStore();
const pickerVisible = ref(false);
const pickerMode = ref<"create" | "open">("create");

onMounted(() => {
  store.refreshActive().then(() => {
    if (store.isActive) store.loadVersions();
  });
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
.project-view {
  padding: var(--spacing-5, 24px);
  height: 100%;
  overflow-y: auto;
}
</style>
