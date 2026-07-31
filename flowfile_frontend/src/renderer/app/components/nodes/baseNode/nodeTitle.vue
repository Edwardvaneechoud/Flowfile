<template>
  <div class="listbox-wrapper">
    <el-collapse v-if="props.intro" class="listbox-expandable">
      <el-collapse-item>
        <template #title>
          <div class="listbox-title">{{ props.title }}</div>
        </template>
        <!-- Replace v-html with a safer alternative -->
        <div class="intro-content">{{ props.intro }}</div>
        <button v-if="props.docsUrl" type="button" class="node-docs-link" @click="openDocs">
          <i class="fa-solid fa-book"></i>
          Read more
        </button>
      </el-collapse-item>
    </el-collapse>
    <div v-else class="title-row">
      <div class="title">{{ props.title }}</div>
      <button v-if="props.docsUrl" type="button" class="node-docs-link" @click="openDocs">
        <i class="fa-solid fa-book"></i>
        Read more
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { defineProps } from "vue";
import { desktop } from "../../../../lib/desktop";

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
  docsUrl: {
    type: String,
    required: false,
    default: "",
  },
});

const openDocs = () => void desktop.openExternal(props.docsUrl);
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

.title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
}

.node-docs-link {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0;
  font-family: inherit;
  font-size: var(--font-size-xs);
  color: var(--color-accent, #0891b2);
  background: none;
  border: none;
  cursor: pointer;
}

.intro-content + .node-docs-link {
  margin-top: var(--spacing-1);
}

.node-docs-link:hover {
  text-decoration: underline;
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
