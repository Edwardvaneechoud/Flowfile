<template>
  <div class="panel section-inspector">
    <div class="panel-header">
      <h3>Section</h3>
    </div>
    <div class="panel-content">
      <div v-if="section" class="inspector-form">
        <div class="type-badge-row">
          <div class="type-badge">
            <i class="fa-solid fa-layer-group"></i>
            <span>{{ section.name }}</span>
          </div>
        </div>

        <div class="field-group">
          <div class="field-group-title">Basic</div>
          <div class="field-row">
            <label>Title</label>
            <input
              :value="section.title ?? ''"
              type="text"
              class="ins-input"
              placeholder="Group title"
              @input="updateTitle(($event.target as HTMLInputElement).value)"
            />
          </div>
          <div class="field-row">
            <label>Python name <span class="req">*</span></label>
            <input
              :value="section.name"
              type="text"
              class="ins-input ins-mono"
              title="Attribute used in self.settings_schema.<name>"
              placeholder="section_name"
              @input="updateName(($event.target as HTMLInputElement).value)"
              @change="commitName"
            />
          </div>
          <div class="field-row">
            <label>Description</label>
            <textarea
              :value="section.description ?? ''"
              class="ins-input ins-textarea"
              rows="2"
              placeholder="Optional helper text shown under the title"
              @input="updateDescription(($event.target as HTMLTextAreaElement).value)"
            ></textarea>
          </div>
        </div>

        <div class="field-group">
          <div class="field-group-title">Layout</div>
          <div class="field-row">
            <label>Control arrangement</label>
            <div class="layout-switch" role="group" aria-label="Section layout">
              <button
                type="button"
                class="layout-btn"
                :class="{ active: section.layout !== 'horizontal' }"
                title="Stack controls vertically"
                @click="setLayout('vertical')"
              >
                <i class="fa-solid fa-bars"></i>
                Vertical
              </button>
              <button
                type="button"
                class="layout-btn"
                :class="{ active: section.layout === 'horizontal' }"
                title="Flow controls side by side"
                @click="setLayout('horizontal')"
              >
                <i class="fa-solid fa-table-columns"></i>
                Horizontal
              </button>
            </div>
            <span class="field-hint">
              Horizontal flows controls side by side; wide controls wrap to their own row.
            </span>
          </div>
        </div>
      </div>

      <EmptyState
        v-else
        icon="fa-solid fa-arrow-pointer"
        description="Select a group to edit it, or a control to edit its settings."
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { SectionLayout, SectionState } from "../designerState";
import EmptyState from "../../../components/common/EmptyState/EmptyState.vue";

const store = useNodeDesignerStore();

const section = computed<SectionState | null>(() => {
  const idx = store.selectedSectionIndex;
  if (idx === null) return null;
  return store.sections[idx] ?? null;
});

function updateTitle(value: string) {
  if (store.selectedSectionIndex === null) return;
  store.updateSectionTitle(store.selectedSectionIndex, value);
}

function updateName(value: string) {
  if (store.selectedSectionIndex === null) return;
  store.sections[store.selectedSectionIndex].name = value;
}

function commitName() {
  if (store.selectedSectionIndex === null) return;
  store.sanitizeSectionName(store.selectedSectionIndex);
}

function updateDescription(value: string) {
  if (store.selectedSectionIndex === null) return;
  store.updateSectionDescription(store.selectedSectionIndex, value);
}

function setLayout(layout: SectionLayout) {
  if (store.selectedSectionIndex === null) return;
  store.setSectionLayout(store.selectedSectionIndex, layout);
}
</script>

<style scoped>
.section-inspector {
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background: var(--color-background-primary);
  overflow-y: auto;
  min-height: 0;
}

.section-inspector .panel-content {
  padding: 1rem;
}

.inspector-form {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.type-badge-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 0.5rem;
}

.type-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  background: var(--color-background-secondary, #f3f4f6);
  border-radius: var(--border-radius-md, 6px);
  font-size: 0.875rem;
  font-weight: 500;
}

.type-badge i {
  color: var(--color-text-secondary, #6b7280);
}

.field-group {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.field-group-title {
  font-size: 0.75rem;
  font-weight: 600;
  color: var(--color-text-secondary, #6b7280);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid var(--color-border-light, #e5e7eb);
}

.field-row {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.field-row label {
  font-size: 0.8125rem;
  font-weight: 500;
  color: var(--color-text-secondary, #6b7280);
}

.ins-input {
  width: 100%;
  padding: 0.5rem;
  font-size: 0.875rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-md, 6px);
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary, #374151);
}

.ins-input:focus {
  outline: none;
  border-color: var(--color-accent);
}

.ins-textarea {
  resize: vertical;
  font-family: inherit;
  line-height: 1.4;
}

.ins-mono {
  font-family: var(--font-family-mono, monospace);
  font-size: 0.8125rem;
}

.req {
  color: var(--color-danger, #ef4444);
}

.layout-switch {
  display: flex;
  gap: 0;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-md, 6px);
  overflow: hidden;
}

.layout-btn {
  flex: 1;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.375rem;
  padding: 0.4rem 0.5rem;
  border: none;
  background: var(--color-background-primary, #fff);
  color: var(--color-text-secondary, #6b7280);
  font-size: 0.8125rem;
  cursor: pointer;
}

.layout-btn + .layout-btn {
  border-left: 1px solid var(--color-border-primary, #d1d5db);
}

.layout-btn:hover {
  background: var(--color-background-secondary, #f3f4f6);
}

.layout-btn.active {
  background: var(--color-accent, #0891b2);
  color: #fff;
}

.field-hint {
  font-size: 0.75rem;
  color: var(--color-text-secondary, #6b7280);
}
</style>
