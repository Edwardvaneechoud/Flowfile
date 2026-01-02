<template>
  <div class="panel property-editor">
    <div class="panel-header">
      <h3>Properties</h3>
    </div>
    <div class="panel-content">
      <div v-if="component" class="property-form">
        <h4>{{ component.component_type }} Properties</h4>

        <!-- Common Properties -->
        <div class="form-field">
          <label>Field Name *</label>
          <input
            :value="component.field_name"
            type="text"
            class="form-input"
            placeholder="field_name"
            @input="updateField('field_name', ($event.target as HTMLInputElement).value)"
          />
        </div>
        <div class="form-field">
          <label>Label</label>
          <input
            :value="component.label"
            type="text"
            class="form-input"
            placeholder="Display Label"
            @input="updateField('label', ($event.target as HTMLInputElement).value)"
          />
        </div>

        <!-- TextInput Properties -->
        <template v-if="component.component_type === 'TextInput'">
          <div class="form-field">
            <label>Default Value</label>
            <input
              :value="component.default"
              type="text"
              class="form-input"
              placeholder="Default value"
              @input="updateField('default', ($event.target as HTMLInputElement).value)"
            />
          </div>
          <div class="form-field">
            <label>Placeholder</label>
            <input
              :value="component.placeholder"
              type="text"
              class="form-input"
              placeholder="Placeholder text"
              @input="updateField('placeholder', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </template>

        <!-- NumericInput Properties -->
        <template v-if="component.component_type === 'NumericInput'">
          <div class="form-field">
            <label>Default Value</label>
            <input
              :value="component.default"
              type="number"
              class="form-input"
              @input="updateField('default', Number(($event.target as HTMLInputElement).value))"
            />
          </div>
          <div class="form-field">
            <label>Min Value</label>
            <input
              :value="component.min_value"
              type="number"
              class="form-input"
              @input="updateField('min_value', Number(($event.target as HTMLInputElement).value))"
            />
          </div>
          <div class="form-field">
            <label>Max Value</label>
            <input
              :value="component.max_value"
              type="number"
              class="form-input"
              @input="updateField('max_value', Number(($event.target as HTMLInputElement).value))"
            />
          </div>
        </template>

        <!-- ToggleSwitch Properties -->
        <template v-if="component.component_type === 'ToggleSwitch'">
          <div class="form-field">
            <label>Default Value</label>
            <input
              :checked="component.default"
              type="checkbox"
              class="form-checkbox"
              @change="updateField('default', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="form-field">
            <label>Description</label>
            <input
              :value="component.description"
              type="text"
              class="form-input"
              placeholder="Toggle description"
              @input="updateField('description', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </template>

        <!-- SingleSelect Properties -->
        <template v-if="component.component_type === 'SingleSelect'">
          <div class="form-field">
            <label>Options Source</label>
            <select
              :value="component.options_source"
              class="form-input"
              @change="updateField('options_source', ($event.target as HTMLSelectElement).value)"
            >
              <option value="static">Static Options</option>
              <option value="incoming_columns">Incoming Columns</option>
            </select>
          </div>
          <div v-if="component.options_source === 'static'" class="form-field">
            <label>Options (comma-separated)</label>
            <input
              :value="component.options_string"
              type="text"
              class="form-input"
              placeholder="option1, option2, option3"
              @input="updateField('options_string', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </template>

        <!-- MultiSelect Properties -->
        <template v-if="component.component_type === 'MultiSelect'">
          <div class="form-field">
            <label>Options Source</label>
            <select
              :value="component.options_source"
              class="form-input"
              @change="updateField('options_source', ($event.target as HTMLSelectElement).value)"
            >
              <option value="static">Static Options</option>
              <option value="incoming_columns">Incoming Columns</option>
            </select>
          </div>
          <div v-if="component.options_source === 'static'" class="form-field">
            <label>Options (comma-separated)</label>
            <input
              :value="component.options_string"
              type="text"
              class="form-input"
              placeholder="option1, option2, option3"
              @input="updateField('options_string', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </template>

        <!-- ColumnSelector Properties -->
        <template v-if="component.component_type === 'ColumnSelector'">
          <div class="form-field">
            <label>Required</label>
            <input
              :checked="component.required"
              type="checkbox"
              class="form-checkbox"
              @change="updateField('required', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="form-field">
            <label>Multiple Selection</label>
            <input
              :checked="component.multiple"
              type="checkbox"
              class="form-checkbox"
              @change="updateField('multiple', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="form-field">
            <label>Data Types Filter</label>
            <select
              :value="component.data_types"
              class="form-input"
              @change="updateField('data_types', ($event.target as HTMLSelectElement).value)"
            >
              <option value="ALL">All Types</option>
              <option value="numeric">Numeric</option>
              <option value="string">String</option>
              <option value="temporal">Temporal</option>
            </select>
          </div>
        </template>

        <!-- SliderInput Properties -->
        <template v-if="component.component_type === 'SliderInput'">
          <div class="form-field">
            <label>Min Value *</label>
            <input
              :value="component.min_value"
              type="number"
              class="form-input"
              @input="updateField('min_value', Number(($event.target as HTMLInputElement).value))"
            />
          </div>
          <div class="form-field">
            <label>Max Value *</label>
            <input
              :value="component.max_value"
              type="number"
              class="form-input"
              @input="updateField('max_value', Number(($event.target as HTMLInputElement).value))"
            />
          </div>
          <div class="form-field">
            <label>Step</label>
            <input
              :value="component.step"
              type="number"
              class="form-input"
              @input="updateField('step', Number(($event.target as HTMLInputElement).value))"
            />
          </div>
        </template>
      </div>

      <div v-else class="no-selection">
        <i class="fa-solid fa-mouse-pointer"></i>
        <p>Select a component to edit its properties</p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { DesignerComponent } from "./types";

const props = defineProps<{
  component: DesignerComponent | null;
}>();

const emit = defineEmits<{
  (e: "update", field: string, value: any): void;
}>();

function updateField(field: string, value: any) {
  emit("update", field, value);
}
</script>

<style scoped>
.property-form h4 {
  margin: 0 0 1rem 0;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid var(--border-color);
}

.property-form .form-field {
  margin-bottom: 0.75rem;
}

.form-field {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.form-field label {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--text-secondary);
}

.form-input {
  padding: 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--input-bg);
  color: var(--text-primary);
  font-size: 0.875rem;
}

.form-input:focus {
  outline: none;
  border-color: var(--primary-color);
}

.form-checkbox {
  width: 18px;
  height: 18px;
}

.no-selection {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--text-secondary);
  text-align: center;
}

.no-selection i {
  font-size: 2rem;
  margin-bottom: 0.5rem;
}
</style>
