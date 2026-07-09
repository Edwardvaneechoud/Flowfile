<template>
  <div
    v-for="(section, sectionKey) in schema"
    v-show="!section.hidden"
    :key="sectionKey"
    class="listbox-wrapper"
  >
    <div class="section-title">
      {{ section.title || sectionKey.toString().replace(/_/g, " ") }}
    </div>
    <p v-if="section.description" class="section-description">{{ section.description }}</p>

    <div class="components-container">
      <div
        v-for="(component, componentKey) in section.components"
        :key="componentKey"
        class="component-item"
      >
        <slot
          v-if="editMode"
          name="component-chrome"
          :section-key="sectionKey.toString()"
          :component-key="componentKey.toString()"
          :component="component"
        ></slot>

        <TextInput
          v-if="component.component_type === 'TextInput'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <NumericInput
          v-else-if="component.component_type === 'NumericInput'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <SliderInput
          v-else-if="component.component_type === 'SliderInput'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <MultiSelect
          v-else-if="component.component_type === 'MultiSelect'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          :incoming-columns="incomingColumns"
          :available-artifacts="artifactOptions"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <SingleSelect
          v-else-if="component.component_type === 'SingleSelect'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          :incoming-columns="incomingColumns"
          :available-artifacts="artifactOptions"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <ToggleSwitch
          v-else-if="component.component_type === 'ToggleSwitch'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <ColumnSelector
          v-else-if="component.component_type === 'ColumnSelector'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          :incoming-columns="columnTypes"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <SecretSelector
          v-else-if="component.component_type === 'SecretSelector'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <ColumnActionInput
          v-else-if="component.component_type === 'ColumnActionInput'"
          :model-value="formData[sectionKey]?.[componentKey]"
          :schema="component"
          :incoming-columns="columnTypes"
          @update:model-value="setValue(sectionKey.toString(), componentKey.toString(), $event)"
        />

        <div v-else class="text-red-500 text-xs">
          Unknown component type: {{ (component as any).component_type }}
        </div>
      </div>
    </div>

    <slot
      v-if="editMode"
      name="section-footer"
      :section-key="sectionKey.toString()"
      :section="section"
    ></slot>
  </div>
</template>

<script setup lang="ts">
// Shared renderer for custom-node settings forms: used by the settings drawer
// (CustomNode.vue) and, in editMode with chrome slots, by the Node Designer.
import type { SettingsSchema } from "./interface";
import type { FileColumn } from "../../../baseNode/nodeInterfaces";
import MultiSelect from "./components/MultiSelect.vue";
import ToggleSwitch from "./components/ToggleSwitch.vue";
import TextInput from "./components/TextInput.vue";
import NumericInput from "./components/NumericInput.vue";
import SliderInput from "./components/SliderInput.vue";
import SingleSelect from "./components/SingleSelect.vue";
import ColumnSelector from "./components/ColumnSelector.vue";
import SecretSelector from "./components/SecretSelector.vue";
import ColumnActionInput from "./components/ColumnActionInput.vue";

// Inner values are `any`: the nine child inputs each declare their own modelValue type
// and the drawer has always fed them untyped saved-settings values.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type CustomNodeFormData = Record<string, Record<string, any>>;

const props = withDefaults(
  defineProps<{
    schema: SettingsSchema;
    formData: CustomNodeFormData;
    incomingColumns?: string[];
    columnTypes?: FileColumn[];
    artifactOptions?: string[];
    editMode?: boolean;
  }>(),
  {
    incomingColumns: () => [],
    columnTypes: () => [],
    artifactOptions: () => [],
    editMode: false,
  },
);

const emit = defineEmits<{
  (e: "update:formData", value: CustomNodeFormData): void;
}>();

// Never mutates the prop: emits a shallow-updated copy so hosts bind v-model:form-data.
function setValue(sectionKey: string, componentKey: string, value: unknown) {
  emit("update:formData", {
    ...props.formData,
    [sectionKey]: { ...(props.formData[sectionKey] ?? {}), [componentKey]: value },
  });
}
</script>

<style scoped>
.section-description {
  font-size: var(--font-size-sm, 12px);
  color: var(--color-text-secondary);
  margin-top: 0.25rem;
  margin-bottom: var(--spacing-3, 12px);
  padding-left: var(--spacing-2, 8px);
}

.components-container {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3, 12px);
}

.section-title {
  font-size: var(--font-size-md, 13px);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-primary);
  padding: var(--spacing-1, 4px) 0 var(--spacing-1, 4px) var(--spacing-2, 8px);
  margin-bottom: var(--spacing-3, 12px);
  border-left: 3px solid var(--color-accent, #0891b2);
}

/* Normalize the field label: the global .listbox-subtitle renders a heavy gray
   bar; here it becomes a light modern label. Targets the <label> only, so
   ColumnActionInput's inner <div class="listbox-subtitle"> sub-headings are
   untouched. */
:deep(label.listbox-subtitle) {
  display: block;
  height: auto;
  padding: 0;
  background: none;
  border: none;
  font-size: var(--font-size-sm, 12px);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-secondary);
  margin-bottom: var(--spacing-1, 4px);
}
</style>
