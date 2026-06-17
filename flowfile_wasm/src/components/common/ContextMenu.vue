<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'

export interface ContextMenuOption {
  label: string
  action: string
  disabled?: boolean
  danger?: boolean
}

defineProps<{
  position: { x: number; y: number }
  options: ContextMenuOption[]
}>()

const emit = defineEmits<{
  (e: 'select', action: string): void
  (e: 'close'): void
}>()

const menuRef = ref<HTMLElement | null>(null)

const selectOption = (option: ContextMenuOption) => {
  if (option.disabled) return
  emit('select', option.action)
  emit('close')
}

const handleClickOutside = (event: MouseEvent) => {
  if (menuRef.value && !menuRef.value.contains(event.target as Node)) {
    emit('close')
  }
}

const handleKeyDown = (event: KeyboardEvent) => {
  if (event.key === 'Escape') {
    emit('close')
  }
}

onMounted(() => {
  document.addEventListener('mousedown', handleClickOutside)
  document.addEventListener('keydown', handleKeyDown)
})

onUnmounted(() => {
  document.removeEventListener('mousedown', handleClickOutside)
  document.removeEventListener('keydown', handleKeyDown)
})
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

<style scoped>
.context-menu {
  position: fixed;
  min-width: 150px;
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--radius-md);
  box-shadow: var(--shadow-lg);
  z-index: var(--z-index-context-menu);
  overflow: hidden;
  user-select: none;
}

.context-menu ul {
  list-style: none;
  padding: 0;
  margin: 0;
}

.context-menu li {
  padding: var(--spacing-2) var(--spacing-3);
  cursor: pointer;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  transition: background-color var(--transition-fast);
}

.context-menu li:hover:not(.disabled) {
  background-color: var(--color-background-tertiary);
}

.context-menu li.disabled {
  color: var(--color-text-muted);
  cursor: not-allowed;
}

.context-menu li.danger {
  color: var(--color-danger);
}

.context-menu li.danger:hover:not(.disabled) {
  background-color: var(--color-danger-light);
}
</style>
