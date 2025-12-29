<template>
  <div class="context-menu" :style="{ top: position.y + 'px', left: position.x + 'px' }">
    <ul>
      <li
        v-for="option in options"
        :key="option.action"
        :class="{ disabled: option.disabled }"
        @click="!option.disabled && selectOption(option.action)"
      >
        {{ option.label }}
      </li>
    </ul>
  </div>
</template>

<script lang="ts" setup>
import { defineProps, defineEmits } from "vue";

const props = defineProps({
  position: { type: Object as () => { x: number; y: number }, required: true },
  options: {
    type: Array as () => { label: string; action: string; disabled: boolean }[],
    required: true,
  },
});

const emit = defineEmits(["select", "close"]);

const selectOption = (action: string) => {
  emit("select", action);
  emit("close");
};
</script>

<style scoped>
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: white;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  border-radius: 4px;
  user-select: none;
}

.context-menu ul {
  list-style: none;
  padding: 0;
  margin: 0;
}

.context-menu li {
  padding: 8px 16px;
  cursor: pointer;
}

.context-menu li.disabled {
  color: #ccc;
  cursor: not-allowed;
}

.context-menu li:hover:not(.disabled) {
  background-color: #f0f0f0;
}
</style>
