<template>
  <div class="response-tree" data-testid="response-tree">
    <div
      v-for="row in rows"
      :key="`${row.path}:${row.depth}`"
      class="tree-row"
      :class="{
        'is-pickable': row.pick === 'records',
        'is-records': row.pick === 'records' && row.path === recordPath,
        'is-cursor': isCursor(row),
      }"
      :style="{ paddingLeft: `${4 + row.depth * 14}px` }"
      :title="row.pick === 'records' ? 'Use this list as the rows' : undefined"
      @click="clickRow(row)"
    >
      <button
        v-if="row.expandable"
        type="button"
        class="tree-toggle"
        :aria-expanded="row.open"
        :aria-label="`${row.open ? 'Fold' : 'Unfold'} ${row.key}`"
        @click.stop="toggle(row)"
      >
        <i class="fa-solid fa-chevron-right" :class="{ 'is-open': row.open }"></i>
      </button>
      <span v-else class="tree-toggle"></span>
      <span class="tree-key">{{ row.key }}</span>
      <span v-if="row.detail" class="tree-detail">{{ row.detail }}</span>
      <span v-if="row.pick === 'records' && row.path === recordPath" class="tree-badge rows-badge"
        >used as rows</span
      >
      <span v-else-if="isCursor(row)" class="tree-badge cursor-badge">next-page token</span>
      <button
        v-else-if="pickCursor && row.pick === 'value'"
        type="button"
        class="tree-action"
        @click.stop="emit('pick-cursor', row.path)"
      >
        use as next-page token
      </button>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, ref } from "vue";
import { defaultOpenPaths, treeRows, type ResponseNode, type TreeRow } from "./restApiView";

const props = defineProps<{
  tree: ResponseNode;
  recordPath: string;
  cursorPath: string;
  pickCursor: boolean;
}>();

const emit = defineEmits<{
  (e: "pick-records", path: string): void;
  (e: "pick-cursor", path: string): void;
}>();

// What the reader folded or unfolded by hand; kept across test requests, never saved.
const toggled = ref<Record<string, boolean>>({});

const openPaths = computed(() => {
  const open = defaultOpenPaths(props.tree, props.pickCursor ? props.cursorPath : "");
  for (const [path, isOpen] of Object.entries(toggled.value)) {
    if (isOpen) open.add(path);
    else open.delete(path);
  }
  return open;
});

const rows = computed(() => treeRows(props.tree, openPaths.value));

const toggle = (row: TreeRow) => {
  toggled.value = { ...toggled.value, [row.path]: !row.open };
};

const clickRow = (row: TreeRow) => {
  if (row.pick === "records") emit("pick-records", row.path);
  else if (row.expandable) toggle(row);
};

const isCursor = (row: TreeRow) =>
  props.pickCursor && row.pick === "value" && row.path !== "" && row.path === props.cursorPath;
</script>

<style scoped>
.response-tree {
  border: 1px solid var(--color-border-primary, #d0d0d0);
  border-radius: 4px;
  padding: 4px 0;
  max-height: 260px;
  overflow: auto;
  font-family: var(--font-family-mono, monospace);
  font-size: 12px;
}

.tree-row {
  display: flex;
  align-items: baseline;
  gap: 6px;
  padding: 2px 8px;
  white-space: nowrap;
}

.tree-row.is-pickable,
.tree-row:has(> button.tree-toggle) {
  cursor: pointer;
}

.tree-toggle {
  flex: 0 0 14px;
  width: 14px;
  padding: 0;
  border: none;
  background: none;
  font-size: 9px;
  color: var(--color-text-secondary, #888);
}

.tree-toggle i {
  transition: transform 0.15s;
}

.tree-toggle i.is-open {
  transform: rotate(90deg);
}

.tree-row.is-pickable:hover {
  background: var(--color-background-hover);
}

.tree-row.is-records {
  background: var(--color-success-light);
}

.tree-row.is-cursor {
  background: var(--color-info-light);
}

.tree-key {
  color: var(--color-text-primary);
}

.tree-detail {
  color: var(--color-text-secondary, #888);
  overflow: hidden;
  text-overflow: ellipsis;
}

.tree-badge,
.tree-action {
  margin-left: auto;
  font-family: var(--font-family-base);
  font-size: 11px;
  flex-shrink: 0;
}

.rows-badge {
  color: var(--color-success-dark);
}

.cursor-badge {
  color: var(--color-info);
}

.tree-action {
  visibility: hidden;
  border: none;
  background: none;
  padding: 0;
  cursor: pointer;
  color: var(--color-accent, #3b82f6);
  text-decoration: underline;
}

.tree-row:hover .tree-action,
.tree-action:focus-visible {
  visibility: visible;
}
</style>
