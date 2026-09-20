<template>
  <el-dropdown
    trigger="click"
    placement="bottom-end"
    :hide-on-click="true"
    popper-class="nb-cell-menu-popper"
  >
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
          <i class="fa-regular fa-clone nb-menu-icon"></i> Duplicate
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
            <i class="fa-regular fa-trash-can nb-menu-icon"></i> Delete
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
.nb-cell-menu:hover,
.nb-cell-menu[aria-expanded="true"] {
  background: var(--el-fill-color, #f0f2f5);
  color: var(--el-text-color-primary, #303133);
}
</style>

<!-- Not scoped: the menu is teleported to <body>; every rule is namespaced under the popper class. -->
<style>
.el-dropdown__popper.el-popper.nb-cell-menu-popper {
  min-width: 200px;
  padding: 6px;
  border: 1px solid var(--el-border-color-lighter, #ebeef5);
  border-radius: 8px;
  /* Not -overlay: dark mode maps that onto the same colour as the item hover fill. */
  background: var(--el-bg-color, #fff);
  box-shadow: var(--el-box-shadow-light);
}
.nb-cell-menu-popper .el-popper__arrow {
  display: none;
}
.nb-cell-menu-popper .el-dropdown-menu {
  padding: 0;
  border: none;
  background: transparent;
}
/* Out-specifies .el-dropdown-menu--small, stamped by the app-wide `size: "small"` option. */
.nb-cell-menu-popper .el-dropdown-menu .el-dropdown-menu__item {
  gap: 8px;
  padding: 6px 10px;
  border-radius: 6px;
  color: var(--el-text-color-primary, #303133);
  font-size: 13px;
  line-height: 20px;
}
.nb-cell-menu-popper .el-dropdown-menu .el-dropdown-menu__item.is-disabled {
  color: var(--el-text-color-disabled, #c0c4cc);
}
.nb-cell-menu-popper .el-dropdown-menu .el-dropdown-menu__item:not(.is-disabled):hover,
.nb-cell-menu-popper .el-dropdown-menu .el-dropdown-menu__item:not(.is-disabled):focus {
  background-color: var(--el-fill-color-light, #f5f7fa);
  color: var(--el-text-color-primary, #303133);
}
.nb-cell-menu-popper .el-dropdown-menu .el-dropdown-menu__item--divided {
  margin: 4px 0;
  border-top: 1px solid var(--el-border-color-lighter, #ebeef5);
}
.nb-cell-menu-popper .nb-menu-icon {
  width: 18px;
  margin-right: 0;
  color: var(--el-text-color-secondary, #909399);
  font-size: 13px;
  text-align: center;
}
.nb-cell-menu-popper .nb-menu-danger {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--el-color-danger, #f56c6c);
}
.nb-cell-menu-popper .nb-menu-danger .nb-menu-icon {
  color: inherit;
}
.nb-cell-menu-popper
  .el-dropdown-menu
  .el-dropdown-menu__item[data-action="delete"]:not(.is-disabled):hover,
.nb-cell-menu-popper
  .el-dropdown-menu
  .el-dropdown-menu__item[data-action="delete"]:not(.is-disabled):focus {
  background-color: var(--el-color-danger-light-9, #fef0f0);
}
/* The colour sits on the span, so it has to yield to the item's disabled grey. */
.nb-cell-menu-popper .nb-menu-danger.is-muted {
  color: inherit;
}
</style>
