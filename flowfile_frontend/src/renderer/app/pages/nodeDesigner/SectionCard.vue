<template>
  <div class="section-card" :class="{ selected: isSelected }" @click="emit('select')">
    <div class="section-header">
      <div class="section-fields">
        <div class="section-field">
          <label>Variable Name</label>
          <input
            :value="section.name"
            type="text"
            class="section-name-input"
            placeholder="section_name"
            @click.stop
            @input="handleNameChange"
          />
        </div>
        <div class="section-field">
          <label>Display Title</label>
          <input
            :value="section.title"
            type="text"
            class="section-title-input"
            placeholder="Section Title"
            @click.stop
            @input="handleTitleChange"
          />
        </div>
      </div>
      <button class="btn-icon" @click.stop="emit('remove')">
        <i class="fa-solid fa-trash"></i>
      </button>
    </div>

    <div class="section-components" @dragover.prevent @drop="handleDrop">
      <div
        v-for="(component, compIndex) in section.components"
        :key="compIndex"
        class="component-card"
        :class="{ selected: isSelected && selectedComponentIndex === compIndex }"
        @click.stop="emit('selectComponent', compIndex)"
      >
        <div class="component-preview">
          <i :class="getComponentIcon(component.component_type)"></i>
          <span class="component-label">{{ component.label || component.component_type }}</span>
          <span class="component-type">({{ component.component_type }})</span>
        </div>
        <button class="btn-icon btn-remove" @click.stop="emit('removeComponent', compIndex)">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div v-if="section.components.length === 0" class="drop-zone">
        <i class="fa-solid fa-plus"></i>
        <span>Drop components here</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { DesignerSection } from "./types";
import { getComponentIcon } from "./constants";

const props = defineProps<{
  section: DesignerSection;
  isSelected: boolean;
  selectedComponentIndex: number | null;
}>();

const emit = defineEmits<{
  (e: "select"): void;
  (e: "remove"): void;
  (e: "selectComponent", compIndex: number): void;
  (e: "removeComponent", compIndex: number): void;
  (e: "drop", event: DragEvent): void;
  (e: "updateName", name: string): void;
  (e: "updateTitle", title: string): void;
}>();

function handleNameChange(event: Event) {
  const target = event.target as HTMLInputElement;
  emit("updateName", target.value);
}

function handleTitleChange(event: Event) {
  const target = event.target as HTMLInputElement;
  emit("updateTitle", target.value);
}

function handleDrop(event: DragEvent) {
  emit("drop", event);
}
</script>

<style scoped>
.section-card {
  border: 1px solid var(--border-color);
  border-radius: 6px;
  margin-bottom: 0.75rem;
  background: var(--bg-secondary);
}

.section-card.selected {
  border-color: var(--primary-color);
  box-shadow: 0 0 0 2px rgba(var(--primary-rgb), 0.1);
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  padding: 0.75rem;
  border-bottom: 1px solid var(--border-color);
  gap: 0.5rem;
}

.section-fields {
  display: flex;
  gap: 0.75rem;
  flex: 1;
}

.section-field {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  flex: 1;
}

.section-field label {
  font-size: 0.6875rem;
  font-weight: 500;
  color: var(--text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.02em;
}

.section-name-input,
.section-title-input {
  padding: 0.375rem 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--input-bg, #fff);
  font-size: 0.8125rem;
  color: var(--text-primary);
  width: 100%;
}

.section-name-input {
  font-family: "Fira Code", "Monaco", monospace;
  font-size: 0.75rem;
}

.section-title-input {
  font-weight: 500;
}

.section-name-input:focus,
.section-title-input:focus {
  outline: none;
  border-color: var(--primary-color, #4a6cf7);
  box-shadow: 0 0 0 2px rgba(74, 108, 247, 0.1);
}

.btn-icon {
  background: transparent;
  border: none;
  color: var(--text-secondary);
  cursor: pointer;
  padding: 0.25rem;
  border-radius: 4px;
}

.btn-icon:hover {
  background: var(--bg-hover);
  color: var(--danger-color);
}

.section-components {
  padding: 0.5rem;
  min-height: 60px;
}

.component-card {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.5rem 0.75rem;
  margin-bottom: 0.25rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: pointer;
}

.component-card:hover {
  border-color: var(--primary-color);
}

.component-card.selected {
  border-color: var(--primary-color);
  background: rgba(var(--primary-rgb), 0.05);
}

.component-preview {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.component-preview i {
  color: var(--primary-color);
}

.component-label {
  font-weight: 500;
}

.component-type {
  font-size: 0.75rem;
  color: var(--text-secondary);
}

.btn-remove {
  opacity: 0;
  transition: opacity 0.2s;
}

.component-card:hover .btn-remove {
  opacity: 1;
}

.drop-zone {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 1rem;
  border: 2px dashed var(--border-color);
  border-radius: 4px;
  color: var(--text-secondary);
  font-size: 0.8125rem;
}

.drop-zone i {
  margin-bottom: 0.25rem;
}
</style>
