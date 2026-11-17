<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    width="92vw"
    top="4vh"
    append-to-body
    destroy-on-close
    class="tile-expand-dialog"
    data-testid="tile-expand-dialog"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div class="tile-expand-body" :style="bodyStyle">
      <slot v-if="modelValue" />
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
/** One tile drawn large: the caller puts the tile's own component, with the same filters, in the slot. */
defineProps<{
  modelValue: boolean;
  title: string;
  /** The tile's own background, so a coloured KPI keeps its colour. */
  bodyStyle?: Record<string, string>;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
}>();
</script>

<style scoped>
.tile-expand-body {
  height: calc(92vh - 110px);
  min-height: 240px;
  display: flex;
  flex-direction: column;
  border-radius: 6px;
  overflow: hidden;
}
</style>
