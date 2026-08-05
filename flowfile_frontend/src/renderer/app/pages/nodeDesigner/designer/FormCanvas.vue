<template>
  <div class="form-canvas">
    <div class="form-canvas-inner">
      <EmptyState
        v-if="store.sections.length === 0"
        icon="fa-solid fa-puzzle-piece"
        title="Build a custom node"
        description="Design its settings form here, write the Python transform in the Code tab, then try it on sample data in Test. Saved nodes show up in your palette."
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
          :selected-section-key="selectedSectionName"
          @update:form-data="onPreviewUpdate"
          @select-component="select"
        >
          <template #section-header="{ sectionKey, section }">
            <div
              class="section-header-edit"
              :class="{
                selected: editingSectionName === sectionKey,
                'drop-before': dropHint(sectionKey) === 'before',
                'drop-into': dropHint(sectionKey) === 'into',
              }"
              role="button"
              tabindex="0"
              title="Edit this group"
              draggable="true"
              :data-testid="`group-header-${sectionKey}`"
              @click="selectSectionByKey(sectionKey)"
              @keydown.enter="selectSectionByKey(sectionKey)"
              @dragstart="onSectionDragStart(sectionKey, $event)"
              @dragover.prevent="onHeaderDragOver(sectionKey, $event)"
              @dragleave="onHeaderDragLeave(sectionKey)"
              @drop.prevent="onHeaderDrop(sectionKey)"
              @dragend="endDrag"
            >
              <i class="fa-solid fa-grip-vertical section-grip"></i>
              <span class="section-header-title">
                {{ section.title || sectionKey.replace(/_/g, " ") }}
              </span>
              <span class="section-header-name">{{ sectionKey }}</span>
              <span class="section-header-actions">
                <button
                  class="chrome-btn"
                  type="button"
                  title="Move group up"
                  :data-testid="`group-up-${sectionKey}`"
                  @click.stop="moveGroup(sectionKey, -1)"
                >
                  <i class="fa-solid fa-chevron-up"></i>
                </button>
                <button
                  class="chrome-btn"
                  type="button"
                  title="Move group down"
                  :data-testid="`group-down-${sectionKey}`"
                  @click.stop="moveGroup(sectionKey, 1)"
                >
                  <i class="fa-solid fa-chevron-down"></i>
                </button>
              </span>
              <i class="fa-solid fa-sliders section-header-icon"></i>
            </div>
          </template>

          <template #component-chrome="{ sectionKey, componentKey }">
            <div class="control-chrome" :class="{ selected: isSelected(sectionKey, componentKey) }">
              <div class="control-chrome-actions">
                <span
                  class="chrome-btn chrome-grip"
                  title="Drag onto another group's header to move it there"
                  draggable="true"
                  @dragstart.stop="onControlDragStart(sectionKey, componentKey, $event)"
                  @dragend="endDrag"
                >
                  <i class="fa-solid fa-grip-vertical"></i>
                </span>
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

        <div
          class="add-group-row"
          :class="{ 'drop-before': endDropActive }"
          @dragover.prevent="onEndDragOver"
          @dragleave="onEndDragLeave"
          @drop.prevent="onEndDrop"
        >
          <button
            class="btn btn-secondary add-group-trailing"
            type="button"
            data-testid="add-group-trailing"
            @click="store.addSection()"
          >
            <i class="fa-solid fa-plus"></i>
            Add group
          </button>
        </div>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { ComponentType } from "../designerState";
import type { FileColumn } from "../../../components/nodes/baseNode/nodeInterfaces";
import { columnDataToTable, sampleColumnsToFileColumns } from "../sampleData";
import CustomNodeForm from "../../../components/nodes/node-types/elements/customNode/CustomNodeForm.vue";
import EmptyState from "../../../components/common/EmptyState/EmptyState.vue";
import AddControlMenu from "./AddControlMenu.vue";

const store = useNodeDesignerStore();

// Live preview has no upstream connection, so column-driven controls show samples
// pulled from the Test-tab sample data (first input's columns) when present.
const columnTypes = computed<FileColumn[]>(() => {
  const first = store.designerState.example_inputs?.[0];
  return first ? sampleColumnsToFileColumns(columnDataToTable(first.data).columns) : [];
});
const incomingColumns = computed<string[]>(() => columnTypes.value.map((c) => c.name));

// The section that contains the current selection (section or one of its
// controls) — frames the whole card so you can see which group you're in.
const selectedSectionName = computed<string | null>(() => {
  const idx = store.selectedSectionIndex;
  if (idx === null) return null;
  return store.sections[idx]?.name ?? null;
});

// The section that is itself the edit target (no control selected) — drives the
// header's strong "editing this group" fill, kept distinct from control editing.
const editingSectionName = computed<string | null>(() => {
  const idx = store.selectedSectionIndex;
  if (idx === null || store.selectedComponentIndex !== null) return null;
  return store.sections[idx]?.name ?? null;
});

function sectionIndex(sectionKey: string): number {
  return store.sections.findIndex((s) => s.name === sectionKey);
}

function selectSectionByKey(sectionKey: string) {
  const idx = sectionIndex(sectionKey);
  if (idx >= 0) store.selectSection(idx);
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

function moveGroup(sectionKey: string, delta: number) {
  const idx = sectionIndex(sectionKey);
  if (idx >= 0) store.moveSection(idx, idx + delta);
}

// --- drag reorder ---
// Dropping on a header always means "land at that group's position"; the row under
// the last group is the only way to land at the end. One indicator, no ambiguity.
// dataTransfer.getData() is unreadable during dragover, so the payload lives here.
type DragPayload =
  | { kind: "section"; index: number }
  | { kind: "control"; sectionIndex: number; componentIndex: number };

const dragging = ref<DragPayload | null>(null);
const hoveredSection = ref<string | null>(null);
const endDropActive = ref(false);

function dropHint(sectionKey: string): "before" | "into" | null {
  if (!dragging.value || hoveredSection.value !== sectionKey) return null;
  return dragging.value.kind === "control" ? "into" : "before";
}

function beginDrag(payload: DragPayload, label: string, event: DragEvent) {
  dragging.value = payload;
  // Firefox refuses to start a drag without data on the transfer.
  event.dataTransfer?.setData("text/plain", label);
  if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
}

function endDrag() {
  dragging.value = null;
  hoveredSection.value = null;
  endDropActive.value = false;
}

function onSectionDragStart(sectionKey: string, event: DragEvent) {
  const idx = sectionIndex(sectionKey);
  if (idx < 0) return;
  beginDrag({ kind: "section", index: idx }, sectionKey, event);
}

function onControlDragStart(sectionKey: string, componentKey: string, event: DragEvent) {
  const sIdx = sectionIndex(sectionKey);
  const cIdx = componentIndex(sectionKey, componentKey);
  if (sIdx < 0 || cIdx < 0) return;
  beginDrag({ kind: "control", sectionIndex: sIdx, componentIndex: cIdx }, componentKey, event);
}

function onHeaderDragOver(sectionKey: string, event: DragEvent) {
  if (!dragging.value) return;
  if (event.dataTransfer) event.dataTransfer.dropEffect = "move";
  hoveredSection.value = sectionKey;
  endDropActive.value = false;
}

function onHeaderDragLeave(sectionKey: string) {
  if (hoveredSection.value === sectionKey) hoveredSection.value = null;
}

function onHeaderDrop(sectionKey: string) {
  const drag = dragging.value;
  endDrag();
  const target = sectionIndex(sectionKey);
  if (!drag || target < 0) return;
  if (drag.kind === "control") {
    store.moveComponentToSection(
      drag.sectionIndex,
      drag.componentIndex,
      target,
      store.sections[target].components.length,
    );
    return;
  }
  store.moveSection(drag.index, target > drag.index ? target - 1 : target);
}

function onEndDragOver() {
  if (dragging.value?.kind !== "section") return;
  hoveredSection.value = null;
  endDropActive.value = true;
}

function onEndDragLeave() {
  endDropActive.value = false;
}

function onEndDrop() {
  const drag = dragging.value;
  endDrag();
  if (drag?.kind !== "section") return;
  store.moveSection(drag.index, store.sections.length - 1);
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

/* Each section renders as a distinct card so it's clear where one group ends and
   the next begins; the selected group gets an accent frame. */
.form-canvas-inner :deep(.listbox-wrapper) {
  border: 1px solid var(--color-border-light, #e5e7eb);
  border-radius: var(--border-radius-lg, 8px);
  padding: 0.75rem;
  margin-bottom: 1rem;
  background: var(--color-background-primary, #fff);
  transition:
    border-color 0.15s,
    box-shadow 0.15s;
}

.form-canvas-inner :deep(.listbox-wrapper.section-selected) {
  border-color: var(--color-accent, #0891b2);
  box-shadow: 0 0 0 1px var(--color-accent, #0891b2);
}

/* Full-bleed title band: sits at the top of the card, its content left-aligned
   with the controls below, closed off from the body by a thin divider. */
.section-header-edit {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin: -0.75rem -0.75rem 0.75rem;
  padding: 0.5rem 0.75rem;
  border-top-left-radius: var(--border-radius-lg, 8px);
  border-top-right-radius: var(--border-radius-lg, 8px);
  border-bottom: 1px solid var(--color-border-light, #e5e7eb);
  cursor: pointer;
}

.section-header-edit:hover {
  background: var(--color-background-secondary, #f3f4f6);
}

/* Section edit-target: a solid accent fill, deliberately unlike the control's
   subtle dashed tint so "editing the group" reads differently from "editing a
   control". */
.section-header-edit.selected {
  background: var(--color-accent, #0891b2);
  border-bottom-color: var(--color-accent, #0891b2);
}

.section-header-title {
  font-size: var(--font-size-md, 13px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
}

.section-header-name {
  font-size: 0.7rem;
  font-family: var(--font-family-mono, monospace);
  color: var(--color-text-secondary, #6b7280);
}

.section-header-edit.selected .section-header-title {
  color: #fff;
}

.section-header-edit.selected .section-header-name {
  color: rgba(255, 255, 255, 0.85);
}

.section-grip {
  font-size: 0.7rem;
  color: var(--color-text-tertiary, #9ca3af);
  cursor: grab;
}

.section-header-edit.selected .section-grip {
  color: rgba(255, 255, 255, 0.85);
}

.section-header-actions {
  display: flex;
  gap: 0.125rem;
  margin-left: auto;
  opacity: 0;
  transition: opacity 0.15s;
}

.section-header-edit:hover .section-header-actions,
.section-header-edit.selected .section-header-actions {
  opacity: 1;
}

/* Insert-here line: dropping on a header lands the dragged group at its position. */
.section-header-edit.drop-before {
  box-shadow: inset 0 3px 0 0 var(--color-accent, #0891b2);
}

.section-header-edit.drop-into {
  background: var(--color-focus-ring-accent, rgba(8, 145, 178, 0.12));
  outline: 2px dashed var(--color-accent, #0891b2);
  outline-offset: -2px;
}

.section-header-icon {
  font-size: 0.75rem;
  color: var(--color-text-tertiary, #9ca3af);
  opacity: 0;
  transition: opacity 0.15s;
}

.section-header-edit:hover .section-header-icon {
  opacity: 1;
  color: var(--color-accent, #0891b2);
}

.section-header-edit.selected .section-header-icon {
  opacity: 1;
  color: #fff;
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

.chrome-grip {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: grab;
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

.add-group-row {
  padding-top: 0.5rem;
  border-top: 3px solid transparent;
}

.add-group-row.drop-before {
  border-top-color: var(--color-accent, #0891b2);
}

.add-group-trailing {
  margin-top: 0.5rem;
}
</style>
