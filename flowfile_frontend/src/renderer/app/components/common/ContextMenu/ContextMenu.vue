<script setup lang="ts">
import { ref, watch, nextTick, onMounted, onUnmounted } from "vue";

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

// The page itself never scrolls (html/body/#app are overflow:hidden), so a menu
// opened near an edge would be unreachable rather than scrolled into view.
const coords = ref({ ...props.position });

const clampToViewport = () => {
  const el = menuRef.value;
  if (!el) return;
  const { width, height } = el.getBoundingClientRect();
  coords.value = {
    x: Math.max(10, Math.min(props.position.x, window.innerWidth - width - 10)),
    y: Math.max(10, Math.min(props.position.y, window.innerHeight - height - 10)),
  };
};

watch(
  () => props.position,
  (next) => {
    coords.value = { ...next };
    void nextTick(clampToViewport);
  },
  { deep: true },
);

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
  void nextTick(clampToViewport);
  document.addEventListener("pointerdown", handlePointerDownOutside, true);
  document.addEventListener("keydown", handleKeyDown);
});

onUnmounted(() => {
  document.removeEventListener("pointerdown", handlePointerDownOutside, true);
  document.removeEventListener("keydown", handleKeyDown);
});
</script>

<template>
  <div ref="menuRef" class="context-menu" :style="{ top: coords.y + 'px', left: coords.x + 'px' }">
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
