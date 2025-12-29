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
.intro-content {
  white-space: pre-wrap; /* Preserves whitespace and line breaks */
}
</style>
