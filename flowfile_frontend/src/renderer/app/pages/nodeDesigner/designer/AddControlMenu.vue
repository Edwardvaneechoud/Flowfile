<template>
  <el-popover
    v-model:visible="open"
    placement="bottom-start"
    trigger="click"
    :width="240"
    popper-class="add-control-popper"
  >
    <template #reference>
      <button class="btn btn-sm btn-secondary" type="button" data-testid="add-control-trigger">
        <i class="fa-solid fa-plus"></i>
        {{ label }}
      </button>
    </template>
    <div class="control-menu">
      <button
        v-for="ctrl in AVAILABLE_CONTROLS"
        :key="ctrl.type"
        class="control-menu-item"
        type="button"
        :data-testid="`add-control-${ctrl.type}`"
        @click="pick(ctrl.type)"
      >
        <i :class="ctrl.icon"></i>
        <span>{{ ctrl.label }}</span>
      </button>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { ref } from "vue";
import { ElPopover } from "element-plus";
import type { ComponentType } from "../designerState";
import { componentDocs } from "../componentDocs";

const AVAILABLE_CONTROLS = componentDocs.map((c) => ({
  type: c.type,
  label: c.label,
  icon: c.icon,
}));

withDefaults(defineProps<{ label?: string }>(), { label: "Add a control" });

const emit = defineEmits<{ (e: "add", type: ComponentType): void }>();

const open = ref(false);

function pick(type: ComponentType) {
  emit("add", type);
  open.value = false;
}
</script>

<style scoped>
.control-menu {
  display: flex;
  flex-direction: column;
  gap: 0.125rem;
}

.control-menu-item {
  display: flex;
  align-items: center;
  gap: 0.625rem;
  width: 100%;
  padding: 0.5rem 0.625rem;
  background: transparent;
  border: none;
  border-radius: var(--border-radius-sm, 4px);
  font-size: 0.875rem;
  color: var(--color-text-primary, #374151);
  cursor: pointer;
  text-align: left;
}

.control-menu-item:hover {
  background: var(--color-background-secondary, #f3f4f6);
}

.control-menu-item i {
  width: 16px;
  text-align: center;
  color: var(--color-text-secondary, #6b7280);
}
</style>
