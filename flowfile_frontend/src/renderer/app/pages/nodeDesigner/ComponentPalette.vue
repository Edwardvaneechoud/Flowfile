<template>
  <div class="panel component-palette">
    <div class="panel-header">
      <h3>Components</h3>
    </div>
    <div class="panel-content">
      <div
        v-for="comp in availableComponents"
        :key="comp.type"
        class="component-item"
        draggable="true"
        @dragstart="handleDragStart($event, comp)"
      >
        <i :class="comp.icon"></i>
        <span>{{ comp.label }}</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { availableComponents } from './constants';
import type { AvailableComponent } from './types';

const emit = defineEmits<{
  (e: 'dragstart', event: DragEvent, component: AvailableComponent): void;
}>();

function handleDragStart(event: DragEvent, component: AvailableComponent) {
  event.dataTransfer?.setData('component_type', component.type);
  emit('dragstart', event, component);
}
</script>

<style scoped>
.component-palette .panel-content {
  padding: 0.5rem;
}

.component-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  margin-bottom: 0.25rem;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: grab;
  transition: all 0.2s;
}

.component-item:hover {
  background: var(--bg-hover);
  border-color: var(--primary-color);
}

.component-item i {
  width: 20px;
  text-align: center;
  color: var(--primary-color);
}

.component-item span {
  font-size: 0.8125rem;
}
</style>
