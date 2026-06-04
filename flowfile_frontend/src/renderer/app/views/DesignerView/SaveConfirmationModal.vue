<template>
  <el-dialog
    v-model="isOpen"
    title="Save Flow"
    width="30%"
    destroy-on-close
    :append-to-body="true"
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @closed="handleDialogClosed"
  >
    <span>Do you want to save changes to this flow before closing?</span>
    <template #footer>
      <div class="dialog-footer">
        <el-button @click="handleDontSave">No</el-button>
        <el-button type="primary" @click="handleSave">Yes</el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref } from "vue";

const emit = defineEmits(["save", "dont-save"]);

const isOpen = ref(false);
const currentFlowId = ref<number | null>(null);
const pendingAction = ref<"save" | "dont-save" | null>(null);

const open = (flowId: number) => {
  currentFlowId.value = flowId;
  pendingAction.value = null;
  isOpen.value = true;
  return flowId;
};

const close = () => {
  isOpen.value = false;
};

const handleSave = () => {
  if (currentFlowId.value !== null) {
    pendingAction.value = "save";
    close();
  }
};

const handleDontSave = () => {
  if (currentFlowId.value !== null) {
    pendingAction.value = "dont-save";
    close();
  }
};

const handleDialogClosed = () => {
  if (currentFlowId.value === null) return;

  if (pendingAction.value === "save") {
    emit("save", currentFlowId.value);
  } else if (pendingAction.value === "dont-save") {
    emit("dont-save", currentFlowId.value);
  }

  pendingAction.value = null;
};

defineExpose({
  open,
  close,
  isOpen,
});
</script>

<style scoped>
.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}
</style>
