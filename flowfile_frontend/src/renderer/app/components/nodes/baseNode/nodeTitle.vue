<template>
  <div class="listbox-wrapper">
    <el-collapse v-if="props.intro" class="listbox-expandable">
      <el-collapse-item>
        <template #title>
          <div class="listbox-title">{{ props.title }}</div>
        </template>
        <!-- Replace v-html with a safer alternative -->
        <div class="intro-content">{{ props.intro }}</div>
      </el-collapse-item>
    </el-collapse>
    <div v-else class="title">{{ props.title }}</div>
  </div>
</template>

<script setup lang="ts">
import { defineProps } from "vue";

// Remove unused ref and function
const props = defineProps({
  title: {
    type: String,
    required: true,
  },
  intro: {
    type: String,
    required: false,
    default: "", // Add default value to resolve warning
  },
});
</script>

<style scoped>
.listbox-wrapper {
  color: var(--color-text-primary);
}

.listbox-title {
  color: var(--color-text-primary);
  font-weight: var(--font-weight-medium);
}

.title {
  color: var(--color-text-primary);
  font-weight: var(--font-weight-medium);
}

.intro-content {
  white-space: pre-wrap;
  color: var(--color-text-secondary);
}

.listbox-expandable {
  --el-collapse-header-background-color: transparent;
  --el-collapse-content-background-color: transparent;
  border: none;
}

.listbox-expandable :deep(.el-collapse-item__header) {
  background-color: transparent;
  border: none;
}

.listbox-expandable :deep(.el-collapse-item__wrap) {
  background-color: transparent;
  border: none;
}

.listbox-expandable :deep(.el-collapse-item__content) {
  background-color: transparent;
}
</style>
