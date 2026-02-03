<template>
  <el-dialog
    v-model="isOpen"
    :title="title"
    width="500px"
    destroy-on-close
    :append-to-body="true"
    :close-on-click-modal="false"
    :close-on-press-escape="true"
    @closed="handleClosed"
  >
    <div class="warning-content">
      <div class="warning-icon">
        <i class="fa-solid fa-circle-info"></i>
      </div>
      <div class="warning-message">
        <slot>
          {{ message }}
        </slot>
      </div>
    </div>
    <template #footer>
      <div class="dialog-footer">
        <el-checkbox v-model="dontShowAgain" class="dont-show-checkbox">
          Don't show this again
        </el-checkbox>
        <el-button type="primary" @click="handleConfirm">Got it</el-button>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref } from "vue";

const props = defineProps<{
  storageKey: string;
  title: string;
  message?: string;
}>();

const emit = defineEmits(["confirm"]);

const isOpen = ref(false);
const dontShowAgain = ref(true);

// Check if the warning has been dismissed before
const shouldShow = (): boolean => {
  return localStorage.getItem(props.storageKey) !== "true";
};

// Show the modal if it hasn't been dismissed
const show = (): boolean => {
  if (shouldShow()) {
    isOpen.value = true;
    return true;
  }
  return false;
};

const handleConfirm = () => {
  if (dontShowAgain.value) {
    localStorage.setItem(props.storageKey, "true");
  }
  isOpen.value = false;
};

const handleClosed = () => {
  emit("confirm");
};

// Expose methods to parent component
defineExpose({
  show,
  shouldShow,
});
</script>

<style scoped>
.warning-content {
  display: flex;
  gap: var(--spacing-3, 12px);
  align-items: flex-start;
}

.warning-icon {
  font-size: 24px;
  color: var(--el-color-primary);
  flex-shrink: 0;
}

.warning-message {
  font-size: var(--font-size-sm, 14px);
  line-height: 1.6;
  color: var(--el-text-color-regular);
}

.warning-message :deep(p) {
  margin: 0 0 8px 0;
}

.warning-message :deep(p:last-child) {
  margin-bottom: 0;
}

.warning-message :deep(ul) {
  margin: 8px 0;
  padding-left: 20px;
}

.warning-message :deep(li) {
  margin: 4px 0;
}

.warning-message :deep(strong) {
  color: var(--el-text-color-primary);
}

.warning-message :deep(code) {
  background-color: var(--el-fill-color-light);
  padding: 2px 6px;
  border-radius: 4px;
  font-family: var(--font-family-mono, monospace);
  font-size: 12px;
}

.dialog-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

.dont-show-checkbox {
  font-size: var(--font-size-sm, 14px);
}
</style>
