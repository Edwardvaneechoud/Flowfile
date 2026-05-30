<template>
  <div class="kernel-row">
    <el-select
      :model-value="modelValue"
      placeholder="Select a kernel..."
      class="kernel-select"
      :size="size"
      :loading="loading"
      @update:model-value="onSelect"
    >
      <el-option
        v-for="kernel in kernels"
        :key="kernel.id"
        :value="kernel.id"
        :label="`${kernel.name} (${kernel.state})`"
      >
        <span class="kernel-option">
          <span class="kernel-state-dot" :class="`kernel-state-dot--${kernel.state}`"></span>
          <span>{{ kernel.name }}</span>
          <span class="kernel-state-label">({{ kernel.state }})</span>
        </span>
      </el-option>
    </el-select>
    <router-link v-if="showManageLink" :to="{ name: 'kernelManager' }" class="manage-kernels-link">
      Manage Kernels
    </router-link>
  </div>
</template>

<script lang="ts" setup>
import type { KernelInfo } from "../../types";

interface Props {
  modelValue: string | null;
  kernels: KernelInfo[];
  loading?: boolean;
  showManageLink?: boolean;
  size?: "small" | "default" | "large";
}

withDefaults(defineProps<Props>(), {
  loading: false,
  showManageLink: true,
  size: "small",
});

const emit = defineEmits<{
  (e: "update:modelValue", value: string | null): void;
  (e: "change", value: string | null): void;
}>();

const onSelect = (value: string | null) => {
  emit("update:modelValue", value);
  emit("change", value);
};
</script>

<style scoped>
.kernel-row {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.kernel-select {
  flex: 1;
}

.manage-kernels-link {
  font-size: 0.8rem;
  color: var(--el-color-primary);
  text-decoration: none;
  white-space: nowrap;
}

.manage-kernels-link:hover {
  text-decoration: underline;
}

.kernel-option {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.kernel-state-label {
  font-size: 0.8rem;
  color: var(--el-text-color-secondary);
}
</style>

<!-- Global (un-scoped) so the same state-dot styling applies wherever the
     `.kernel-state-dot` class is used (e.g. the kernel indicator badge in the
     Python script node's expanded editor), keeping the colors defined once. -->
<style>
.kernel-state-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  display: inline-block;
}

.kernel-state-dot--idle {
  background-color: #67c23a;
}

.kernel-state-dot--executing {
  background-color: #e6a23c;
}

.kernel-state-dot--starting {
  background-color: #409eff;
}

.kernel-state-dot--stopped {
  background-color: #909399;
}

.kernel-state-dot--error {
  background-color: #f56c6c;
}
</style>
