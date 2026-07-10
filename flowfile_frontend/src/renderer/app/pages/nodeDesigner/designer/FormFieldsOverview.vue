<template>
  <div class="fields-overview">
    <div class="fields-header">
      <i class="fa-solid fa-sliders"></i>
      <span>Form fields</span>
    </div>
    <p class="fields-hint">Click a field to insert its accessor into your code.</p>

    <div v-if="!hasFields" class="fields-empty">
      No form controls yet — add them in the <b>Form</b> tab.
    </div>

    <div v-for="(section, sIndex) in sectionsWithFields" :key="sIndex" class="fields-section">
      <div class="fields-section-title">{{ section.title || section.name }}</div>
      <button
        v-for="comp in section.components"
        :key="comp.name"
        class="field-row"
        :title="accessor(section, comp)"
        @click="insert(section, comp)"
      >
        <span class="field-name">{{ toSnakeCase(comp.name) }}</span>
        <span class="field-type">{{ comp.component_type }}</span>
        <code class="field-accessor">{{ accessor(section, comp) }}</code>
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import { toSnakeCase } from "../mappers";
import type { ComponentState, SectionState } from "../designerState";

const emit = defineEmits<{
  (e: "insert", code: string): void;
}>();

const store = useNodeDesignerStore();

const sectionsWithFields = computed(() => store.sections.filter((s) => s.components.length > 0));
const hasFields = computed(() => sectionsWithFields.value.length > 0);

function accessor(section: SectionState, comp: ComponentState): string {
  const attribute = comp.component_type === "SecretSelector" ? "secret_value" : "value";
  return `self.settings_schema.${toSnakeCase(section.name)}.${toSnakeCase(comp.name)}.${attribute}`;
}

function insert(section: SectionState, comp: ComponentState) {
  const fieldName = toSnakeCase(comp.name);
  emit("insert", `    ${fieldName} = ${accessor(section, comp)}`);
}
</script>

<style scoped>
.fields-overview {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  overflow-y: auto;
  min-height: 0;
}

.fields-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.fields-hint {
  margin: 0;
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
}

.fields-empty {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  padding: var(--spacing-2) 0;
}

.fields-section-title {
  font-size: var(--font-size-2xs);
  font-weight: var(--font-weight-semibold);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary);
  margin: var(--spacing-2) 0 var(--spacing-1);
}

.field-row {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  width: 100%;
  padding: var(--spacing-2);
  margin-bottom: var(--spacing-1);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-primary);
  cursor: pointer;
  text-align: left;
  transition: border-color var(--transition-normal);
}

.field-row:hover {
  border-color: var(--color-accent);
}

.field-name {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.field-type {
  font-size: var(--font-size-2xs);
  color: var(--color-text-secondary);
}

.field-accessor {
  font-size: var(--font-size-2xs);
  color: var(--color-accent);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 100%;
}
</style>
