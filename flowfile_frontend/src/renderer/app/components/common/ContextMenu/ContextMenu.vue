<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";

export interface ContextMenuOption {
  label: string;
  action: string;
  disabled?: boolean;
  danger?: boolean;
}

const props = defineProps<{
  position: { x: number; y: number };
  options: ContextMenuOption[];
}>();

const emit = defineEmits<{
  (e: "select", action: string): void;
  (e: "close"): void;
}>();

const menuRef = ref<HTMLElement | null>(null);

const selectOption = (option: ContextMenuOption) => {
  if (option.disabled) return;
  emit("select", option.action);
  emit("close");
};

// Capture-phase pointerdown: VueFlow's pane preventDefaults pointerdown, which
// suppresses the compatibility mousedown — a bubble mousedown listener never
// fires for canvas clicks, leaving the menu stuck open.
const handlePointerDownOutside = (event: PointerEvent) => {
  if (menuRef.value && !menuRef.value.contains(event.target as Node)) {
    emit("close");
  }
};

const handleKeyDown = (event: KeyboardEvent) => {
  if (event.key === "Escape") {
    emit("close");
  }
};

onMounted(() => {
  document.addEventListener("pointerdown", handlePointerDownOutside, true);
  document.addEventListener("keydown", handleKeyDown);
});

onUnmounted(() => {
  document.removeEventListener("pointerdown", handlePointerDownOutside, true);
  document.removeEventListener("keydown", handleKeyDown);
});
</script>

<template>
  <div
    ref="menuRef"
    class="context-menu"
    :style="{ top: position.y + 'px', left: position.x + 'px' }"
  >
    <ul>
      <li
        v-for="option in options"
        :key="option.action"
        :class="{ disabled: option.disabled, danger: option.danger }"
        @click="selectOption(option)"
      >
        {{ option.label }}
      </li>
    </ul>
  </div>
</template>
