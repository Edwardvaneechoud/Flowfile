<template>
  <div class="form-canvas">
    <div class="form-canvas-inner">
      <EmptyState
        v-if="store.sections.length === 0"
        icon="fa-solid fa-layer-group"
        title="No controls yet"
        description="Add a group to start building this node's settings form."
      >
        <template #actions>
          <button
            class="btn btn-secondary"
            type="button"
            data-testid="add-group"
            @click="store.addSection()"
          >
            <i class="fa-solid fa-plus"></i>
            Add group
          </button>
        </template>
      </EmptyState>

      <template v-else>
        <CustomNodeForm
          :schema="store.frontendSchema"
          :form-data="store.previewValues"
          :incoming-columns="incomingColumns"
          :column-types="columnTypes"
          :edit-mode="true"
          @update:form-data="onPreviewUpdate"
        >
          <template #component-chrome="{ sectionKey, componentKey }">
            <div
              class="control-chrome"
              :class="{ selected: isSelected(sectionKey, componentKey) }"
              @click.stop="select(sectionKey, componentKey)"
            >
              <div class="control-chrome-actions">
                <button
                  class="chrome-btn chrome-btn-select"
                  type="button"
                  title="Edit this control"
                  @click.stop="select(sectionKey, componentKey)"
                >
                  <i class="fa-solid fa-pen"></i>
                </button>
                <button
                  class="chrome-btn"
                  type="button"
                  title="Move up"
                  @click.stop="move(sectionKey, componentKey, -1)"
                >
                  <i class="fa-solid fa-chevron-up"></i>
                </button>
                <button
                  class="chrome-btn"
                  type="button"
                  title="Move down"
                  @click.stop="move(sectionKey, componentKey, 1)"
                >
                  <i class="fa-solid fa-chevron-down"></i>
                </button>
                <button
                  class="chrome-btn chrome-btn-danger"
                  type="button"
                  title="Remove control"
                  @click.stop="remove(sectionKey, componentKey)"
                >
                  <i class="fa-solid fa-xmark"></i>
                </button>
              </div>
            </div>
          </template>

          <template #section-footer="{ sectionKey }">
            <div class="section-footer">
              <AddControlMenu @add="addControl(sectionKey, $event)" />
              <button
                v-if="store.sections.length > 1"
                class="remove-group-btn"
                type="button"
                title="Remove group"
                @click="removeGroup(sectionKey)"
              >
                <i class="fa-solid fa-trash"></i>
                Remove group
              </button>
            </div>
          </template>
        </CustomNodeForm>

        <button
          class="btn btn-secondary add-group-trailing"
          type="button"
          @click="store.addSection()"
        >
          <i class="fa-solid fa-plus"></i>
          Add group
        </button>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { ComponentType } from "../designerState";
import type { FileColumn } from "../../../components/nodes/baseNode/nodeInterfaces";
import CustomNodeForm from "../../../components/nodes/node-types/elements/customNode/CustomNodeForm.vue";
import EmptyState from "../../../components/common/EmptyState/EmptyState.vue";
import AddControlMenu from "./AddControlMenu.vue";

const store = useNodeDesignerStore();

// Live preview has no upstream connection, so column-driven controls show samples
// pulled from the Test-tab sample data (first input's columns) when present.
const incomingColumns = computed<string[]>(() => {
  const first = store.designerState.example_inputs?.[0];
  return first ? Object.keys(first.data) : [];
});
const columnTypes = computed<FileColumn[]>(() =>
  incomingColumns.value.map((name) => ({ name, data_type: "String" }) as FileColumn),
);

function sectionIndex(sectionKey: string): number {
  return store.sections.findIndex((s) => s.name === sectionKey);
}

function componentIndex(sectionKey: string, componentKey: string): number {
  const sIdx = sectionIndex(sectionKey);
  if (sIdx < 0) return -1;
  return store.sections[sIdx].components.findIndex((c) => c.name === componentKey);
}

function isSelected(sectionKey: string, componentKey: string): boolean {
  return (
    store.selectedSectionIndex === sectionIndex(sectionKey) &&
    store.selectedComponentIndex === componentIndex(sectionKey, componentKey)
  );
}

function select(sectionKey: string, componentKey: string) {
  const sIdx = sectionIndex(sectionKey);
  const cIdx = componentIndex(sectionKey, componentKey);
  if (sIdx >= 0 && cIdx >= 0) store.selectComponent(sIdx, cIdx);
}

function move(sectionKey: string, componentKey: string, delta: number) {
  const sIdx = sectionIndex(sectionKey);
  const cIdx = componentIndex(sectionKey, componentKey);
  if (sIdx >= 0 && cIdx >= 0) {
    store.selectComponent(sIdx, cIdx);
    store.moveComponent(sIdx, cIdx, cIdx + delta);
  }
}

function remove(sectionKey: string, componentKey: string) {
  const sIdx = sectionIndex(sectionKey);
  const cIdx = componentIndex(sectionKey, componentKey);
  if (sIdx >= 0 && cIdx >= 0) store.removeComponent(sIdx, cIdx);
}

function addControl(sectionKey: string, type: ComponentType) {
  const sIdx = sectionIndex(sectionKey);
  if (sIdx >= 0) store.addComponent(sIdx, type);
}

function removeGroup(sectionKey: string) {
  const sIdx = sectionIndex(sectionKey);
  if (sIdx >= 0) store.removeSection(sIdx);
}

function onPreviewUpdate(value: Record<string, Record<string, unknown>>) {
  store.setPreviewValues(value);
}
</script>

<style scoped>
.form-canvas {
  overflow-y: auto;
  min-height: 0;
  height: 100%;
}

.form-canvas-inner {
  padding: 0.5rem 0.25rem 1.5rem;
}

.control-chrome {
  position: absolute;
  inset: -0.375rem;
  border: 1px dashed transparent;
  border-radius: var(--border-radius-sm, 4px);
  pointer-events: none;
}

.control-chrome.selected {
  border-color: var(--color-accent, #0891b2);
  background: var(--color-focus-ring-accent, rgba(8, 145, 178, 0.06));
}

.control-chrome-actions {
  position: absolute;
  top: -0.5rem;
  right: 0;
  display: flex;
  gap: 0.125rem;
  opacity: 0;
  pointer-events: auto;
  transition: opacity 0.15s;
}

/* wrapper for a component in edit mode makes the whole item hoverable */
:deep(.component-item) {
  position: relative;
  cursor: pointer;
}

:deep(.component-item:hover) .control-chrome-actions,
.control-chrome.selected .control-chrome-actions {
  opacity: 1;
}

.chrome-btn {
  width: 22px;
  height: 22px;
  padding: 0;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: 4px;
  background: var(--color-background-primary, #fff);
  color: var(--color-text-secondary, #6b7280);
  font-size: 0.6875rem;
  cursor: pointer;
}

.chrome-btn:hover {
  border-color: var(--color-accent, #0891b2);
  color: var(--color-accent, #0891b2);
}

.chrome-btn-danger:hover {
  border-color: var(--color-text-danger, #dc2626);
  color: var(--color-text-danger, #dc2626);
}

.section-footer {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  margin: 0.25rem 0 1.5rem;
}

.remove-group-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.625rem;
  background: transparent;
  color: var(--color-text-secondary, #6b7280);
  border: none;
  border-radius: var(--border-radius-sm, 4px);
  font-size: 0.75rem;
  cursor: pointer;
}

.remove-group-btn:hover {
  color: var(--color-text-danger, #dc2626);
}

.add-group-trailing {
  margin-top: 0.5rem;
}
</style>
