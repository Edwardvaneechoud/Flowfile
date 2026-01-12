<template>
  <div class="listbox-wrapper">
    <div class="listbox-row">
      <div class="listbox-subtitle" :style="{ fontSize: titleFontSize }">
        {{ title }}
      </div>
      <div
        class="items-container"
        :class="{ droppable: droppable }"
        @dragover.prevent="onDragOver"
        @dragleave="onDragLeave"
        @drop="onDrop"
      >
        <div v-for="(item, index) in items" :key="index" class="item-box">
          <template v-if="item !== ''">
            {{ item }}
            <span class="remove-btn" @click="emitRemove(item)">&times;</span>
          </template>
        </div>
        <div v-if="items.length === 0 || (items.length === 1 && items[0] === '')" class="placeholder">
          {{ placeholder }}
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref } from 'vue'

const props = defineProps({
  title: { type: String, required: true },
  titleFontSize: { type: String, default: '14px' },
  items: { type: Array as () => string[], required: true },
  droppable: { type: Boolean, default: false },
  placeholder: { type: String, default: 'Drag columns here or right-click to add' }
})

const emit = defineEmits(['removeItem', 'drop'])

const isDragOver = ref(false)

const emitRemove = (item: string) => {
  emit('removeItem', item)
}

const onDragOver = () => {
  if (props.droppable) {
    isDragOver.value = true
  }
}

const onDragLeave = () => {
  isDragOver.value = false
}

const onDrop = (event: DragEvent) => {
  isDragOver.value = false
  emit('drop', event)
}
</script>

<style scoped>
.listbox-row {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.items-container {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  min-height: 32px;
  padding: var(--spacing-2);
  background-color: var(--color-background-tertiary);
  border-radius: var(--radius-sm);
  border: 1px solid var(--color-border-light);
  transition: all var(--transition-fast);
}

.items-container.droppable {
  border-style: dashed;
}

.items-container.droppable:hover {
  border-color: var(--color-accent);
  background-color: var(--color-accent-subtle);
}

.item-box {
  display: flex;
  align-items: center;
  padding: 4px 10px;
  background-color: var(--color-background-selected);
  border-radius: var(--radius-sm);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.remove-btn {
  margin-left: 8px;
  cursor: pointer;
  color: var(--color-text-secondary);
  font-weight: bold;
  font-size: var(--font-size-base);
  line-height: 1;
}

.remove-btn:hover {
  color: var(--color-danger);
}

.placeholder {
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  font-style: italic;
}
</style>
