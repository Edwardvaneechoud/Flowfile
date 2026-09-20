<template>
  <el-dropdown trigger="click" placement="bottom-end" :hide-on-click="true">
    <button type="button" class="nb-cell-menu" aria-label="Cell actions" title="Cell actions">
      <i class="fa-solid fa-ellipsis"></i>
    </button>
    <template #dropdown>
      <el-dropdown-menu>
        <el-dropdown-item
          data-action="insert-above"
          :disabled="disabled"
          @click="emit('insert-above')"
        >
          <i class="fa-solid fa-arrow-up nb-menu-icon"></i> Insert above
        </el-dropdown-item>
        <el-dropdown-item
          data-action="insert-below"
          :disabled="disabled"
          @click="emit('insert-below')"
        >
          <i class="fa-solid fa-arrow-down nb-menu-icon"></i> Insert below
        </el-dropdown-item>
        <el-dropdown-item data-action="duplicate" :disabled="disabled" @click="emit('duplicate')">
          <i class="fa-solid fa-clone nb-menu-icon"></i> Duplicate
        </el-dropdown-item>
        <el-dropdown-item data-action="toggle-code" @click="emit('toggle-code')">
          <i class="fa-solid fa-code nb-menu-icon"></i>
          {{ codeCollapsed ? "Expand code" : "Collapse code" }}
        </el-dropdown-item>
        <el-dropdown-item
          v-if="hasOutput"
          data-action="toggle-output"
          @click="emit('toggle-output')"
        >
          <i class="fa-solid fa-table-list nb-menu-icon"></i>
          {{ outputCollapsed ? "Expand output" : "Collapse output" }}
        </el-dropdown-item>
        <el-dropdown-item
          divided
          data-action="delete"
          :disabled="disabled || !canDelete"
          @click="emit('delete')"
        >
          <span class="nb-menu-danger" :class="{ 'is-muted': disabled || !canDelete }">
            <i class="fa-solid fa-trash nb-menu-icon"></i> Delete
          </span>
        </el-dropdown-item>
      </el-dropdown-menu>
    </template>
  </el-dropdown>
</template>

<script setup lang="ts">
withDefaults(
  defineProps<{
    /** Structural items only; the collapse items stay usable while a notebook runs. */
    disabled?: boolean;
    codeCollapsed: boolean;
    outputCollapsed: boolean;
    hasOutput?: boolean;
    canDelete?: boolean;
  }>(),
  { disabled: false, hasOutput: false, canDelete: true },
);

const emit = defineEmits<{
  (e: "insert-above"): void;
  (e: "insert-below"): void;
  (e: "duplicate"): void;
  (e: "toggle-code"): void;
  (e: "toggle-output"): void;
  (e: "delete"): void;
}>();
</script>

<style scoped>
.nb-cell-menu {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  border: none;
  border-radius: 5px;
  background: transparent;
  color: var(--el-text-color-secondary, #909399);
  cursor: pointer;
  font-size: 12px;
  transition:
    background 0.12s,
    color 0.12s;
}
.nb-cell-menu:hover {
  background: var(--el-fill-color, #f0f2f5);
  color: var(--el-text-color-primary, #303133);
}
.nb-menu-icon {
  width: 16px;
  margin-right: 6px;
  text-align: center;
}
.nb-menu-danger {
  color: var(--el-color-danger, #f56c6c);
}
/* The colour sits on the span, so it has to yield to the item's disabled grey. */
.nb-menu-danger.is-muted {
  color: inherit;
}
</style>
