<template>
  <!-- eslint-disable vue/no-v-html -- html is renderTileMarkdown's DOMPurify-sanitised output -->
  <div class="tile-note-body" @click="openTileLink" v-html="html" />
  <!-- eslint-enable vue/no-v-html -->
</template>

<script setup lang="ts">
import { computed } from "vue";
import { openTileLink } from "./tileLinks";
import { renderTileMarkdown } from "./tileMarkdown";

/** A tile note's Markdown, rendered as a text block renders its own; for the popover and the dialog's preview. */
const props = defineProps<{ markdown: string }>();

const html = computed(() => renderTileMarkdown(props.markdown));
</script>

<style scoped>
.tile-note-body {
  font-size: 13px;
  line-height: 1.5;
  color: var(--el-text-color-regular);
  overflow-wrap: anywhere;
}
.tile-note-body > :deep(:first-child) {
  margin-top: 0;
}
.tile-note-body > :deep(:last-child) {
  margin-bottom: 0;
}
.tile-note-body :deep(h1),
.tile-note-body :deep(h2),
.tile-note-body :deep(h3),
.tile-note-body :deep(h4),
.tile-note-body :deep(h5),
.tile-note-body :deep(h6) {
  font-size: 14px;
  font-weight: 600;
  margin: 8px 0 4px;
  color: var(--el-text-color-primary);
}
.tile-note-body :deep(p) {
  margin: 6px 0;
}
.tile-note-body :deep(ul),
.tile-note-body :deep(ol) {
  padding-left: 20px;
  margin: 6px 0;
}
.tile-note-body :deep(code) {
  background: var(--el-fill-color-light);
  padding: 1px 4px;
  border-radius: 3px;
  font-size: 12px;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
}
.tile-note-body :deep(pre) {
  background: var(--el-fill-color-light);
  padding: 6px 10px;
  border-radius: 4px;
  overflow: auto;
}
.tile-note-body :deep(pre code) {
  background: transparent;
  padding: 0;
}
.tile-note-body :deep(a) {
  color: var(--el-color-primary);
  text-decoration: none;
}
.tile-note-body :deep(a:hover) {
  text-decoration: underline;
}
.tile-note-body :deep(blockquote) {
  border-left: 3px solid var(--el-border-color);
  padding-left: 10px;
  margin: 6px 0;
  color: var(--el-text-color-secondary);
}
.tile-note-body :deep(img) {
  max-width: 100%;
}
.tile-note-body :deep(hr) {
  border: 0;
  border-top: 1px solid var(--el-border-color-lighter);
  margin: 8px 0;
}
.tile-note-body :deep(table) {
  border-collapse: collapse;
}
.tile-note-body :deep(th),
.tile-note-body :deep(td) {
  border: 1px solid var(--el-border-color-lighter);
  padding: 2px 6px;
  text-align: left;
}
</style>
