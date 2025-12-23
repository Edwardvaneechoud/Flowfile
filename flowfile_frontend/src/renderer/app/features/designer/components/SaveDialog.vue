// saveDialog.vue

<template>
  <el-dialog
    v-model="isVisible"
    title="Select save location"
    width="70%"
    :close-on-click-modal="false"
    @closed="handleDialogClosed"
  >
    <file-browser
      ref="fileBrowserRef"
      :allowed-file-types="ALLOWED_SAVE_EXTENSIONS"
      mode="create"
      :initial-file-path="initialPath"
      @create-file="handleSaveFlow"
      @overwrite-file="handleSaveFlow"
    />
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import FileBrowser from "./fileBrowser/fileBrowser.vue";
import { FileInfo } from "./fileBrowser/types";
import { saveFlow } from "./HeaderButtons/utils";
import { getFlowSettings } from "../nodes/nodeLogic";
import { ALLOWED_SAVE_EXTENSIONS } from "./fileBrowser/constants";

const props = defineProps({
  visible: {
    type: Boolean,
    default: false,
  },
  flowId: {
    type: Number,
    required: true,
  },
});

const emit = defineEmits(["save-complete", "save-cancelled", "update:visible"]);

const isVisible = ref(props.visible);
const initialPath = ref("");

const fileBrowserRef = ref<{
  refresh: () => Promise<void>;
  handleInitialFileSelection: (filePath?: string) => Promise<void>;
  loadCurrentDirectory: () => Promise<void>;
  navigateUpDirectory: () => Promise<void>;
  selectedFile: FileInfo | null;
} | null>(null);
watch(
  () => props.visible,
  (newValue) => {
    isVisible.value = newValue;
  },
);

// When visibility changes, update the initialPath and file selection
watch(isVisible, async (newValue) => {
  // Emit update to parent for v-model binding
  if (newValue !== props.visible) {
    emit("update:visible", newValue);
  }

  if (newValue && props.flowId) {
    await updateInitialPath();
  }
});

// Update the two-way binding when local state changes
watch(isVisible, (newValue) => {
  if (!newValue) {
    // Only emit cancelled if the dialog was closed without saving
    emit("save-cancelled", props.flowId);
  }
});

// Handle dialog closure
const handleDialogClosed = () => {
  // Any cleanup logic can go here
};

// Update the initial path based on flow settings
const updateInitialPath = async () => {
  try {
    const settings = await getFlowSettings(props.flowId);
    if (settings?.path) {
      initialPath.value = settings.path;

      // Set the file selection if the file browser is available
      if (fileBrowserRef.value) {
        await fileBrowserRef.value.handleInitialFileSelection(settings.path);
      }
    }
  } catch (error) {
    console.error("Error getting flow settings:", error);
  }
};

const handleSaveFlow = async (flowPath: string) => {
  console.log("Saving flow to path:", flowPath);
  try {
    await saveFlow(props.flowId, flowPath);
    isVisible.value = false;
    emit("save-complete", props.flowId);
  } catch (error) {
    console.error("Error saving flow:", error);
  }
};

// Public methods
const open = async () => {
  await updateInitialPath();
  isVisible.value = true;
};

// Expose methods for external access
defineExpose({
  open,
  close: () => {
    isVisible.value = false;
  },
});
</script>
