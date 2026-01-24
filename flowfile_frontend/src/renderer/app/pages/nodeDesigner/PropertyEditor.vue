<template>
  <div class="panel property-editor">
    <div class="panel-header">
      <h3>Properties</h3>
    </div>
    <div class="panel-content">
      <div v-if="component" class="property-form">
        <!-- Component Type Badge -->
        <div class="component-type-badge">
          <i :class="getComponentIcon(component.component_type)"></i>
          <span>{{ component.component_type }}</span>
        </div>

        <!-- Common Properties -->
        <div class="property-group">
          <div class="property-group-title">Basic</div>
          <div class="property-row">
            <label class="property-label">Field Name <span class="required">*</span></label>
            <input
              :value="component.field_name"
              type="text"
              class="property-input"
              placeholder="field_name"
              @input="updateField('field_name', ($event.target as HTMLInputElement).value)"
            />
          </div>
          <div class="property-row">
            <label class="property-label">Label</label>
            <input
              :value="component.label"
              type="text"
              class="property-input"
              placeholder="Display Label"
              @input="updateField('label', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </div>

        <!-- TextInput Properties -->
        <template v-if="component.component_type === 'TextInput'">
          <div class="property-group">
            <div class="property-group-title">Text Options</div>
            <div class="property-row">
              <label class="property-label">Default Value</label>
              <input
                :value="component.default"
                type="text"
                class="property-input"
                placeholder="Default value"
                @input="updateField('default', ($event.target as HTMLInputElement).value)"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Placeholder</label>
              <input
                :value="component.placeholder"
                type="text"
                class="property-input"
                placeholder="Placeholder text"
                @input="updateField('placeholder', ($event.target as HTMLInputElement).value)"
              />
            </div>
          </div>
        </template>

        <!-- NumericInput Properties -->
        <template v-if="component.component_type === 'NumericInput'">
          <div class="property-group">
            <div class="property-group-title">Number Options</div>
            <div class="property-row">
              <label class="property-label">Default Value</label>
              <input
                :value="component.default"
                type="number"
                class="property-input"
                @input="updateField('default', Number(($event.target as HTMLInputElement).value))"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Min Value</label>
              <input
                :value="component.min_value"
                type="number"
                class="property-input"
                @input="updateField('min_value', Number(($event.target as HTMLInputElement).value))"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Max Value</label>
              <input
                :value="component.max_value"
                type="number"
                class="property-input"
                @input="updateField('max_value', Number(($event.target as HTMLInputElement).value))"
              />
            </div>
          </div>
        </template>

        <!-- ToggleSwitch Properties -->
        <template v-if="component.component_type === 'ToggleSwitch'">
          <div class="property-group">
            <div class="property-group-title">Toggle Options</div>
            <div class="property-row checkbox-row">
              <label class="property-label">Default Value</label>
              <input
                :checked="component.default"
                type="checkbox"
                class="property-checkbox"
                @change="updateField('default', ($event.target as HTMLInputElement).checked)"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Description</label>
              <input
                :value="component.description"
                type="text"
                class="property-input"
                placeholder="Toggle description"
                @input="updateField('description', ($event.target as HTMLInputElement).value)"
              />
            </div>
          </div>
        </template>

        <!-- SingleSelect Properties -->
        <template v-if="component.component_type === 'SingleSelect'">
          <div class="property-group">
            <div class="property-group-title">Select Options</div>
            <div class="property-row">
              <label class="property-label">Options Source</label>
              <select
                :value="component.options_source"
                class="property-input"
                @change="updateField('options_source', ($event.target as HTMLSelectElement).value)"
              >
                <option value="static">Static Options</option>
                <option value="incoming_columns">Incoming Columns</option>
              </select>
            </div>
            <div v-if="component.options_source === 'static'" class="property-row">
              <label class="property-label">Options (comma-separated)</label>
              <input
                :value="component.options_string"
                type="text"
                class="property-input"
                placeholder="option1, option2, option3"
                @input="updateField('options_string', ($event.target as HTMLInputElement).value)"
              />
            </div>
          </div>
        </template>

        <!-- MultiSelect Properties -->
        <template v-if="component.component_type === 'MultiSelect'">
          <div class="property-group">
            <div class="property-group-title">Select Options</div>
            <div class="property-row">
              <label class="property-label">Options Source</label>
              <select
                :value="component.options_source"
                class="property-input"
                @change="updateField('options_source', ($event.target as HTMLSelectElement).value)"
              >
                <option value="static">Static Options</option>
                <option value="incoming_columns">Incoming Columns</option>
              </select>
            </div>
            <div v-if="component.options_source === 'static'" class="property-row">
              <label class="property-label">Options (comma-separated)</label>
              <input
                :value="component.options_string"
                type="text"
                class="property-input"
                placeholder="option1, option2, option3"
                @input="updateField('options_string', ($event.target as HTMLInputElement).value)"
              />
            </div>
          </div>
        </template>

        <!-- ColumnSelector Properties -->
        <template v-if="component.component_type === 'ColumnSelector'">
          <div class="property-group">
            <div class="property-group-title">Column Options</div>
            <div class="property-row checkbox-row">
              <label class="property-label">Required</label>
              <input
                :checked="component.required"
                type="checkbox"
                class="property-checkbox"
                @change="updateField('required', ($event.target as HTMLInputElement).checked)"
              />
            </div>
            <div class="property-row checkbox-row">
              <label class="property-label">Multiple Selection</label>
              <input
                :checked="component.multiple"
                type="checkbox"
                class="property-checkbox"
                @change="updateField('multiple', ($event.target as HTMLInputElement).checked)"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Data Types Filter</label>
              <select
                :value="component.data_types"
                class="property-input"
                @change="updateField('data_types', ($event.target as HTMLSelectElement).value)"
              >
                <option value="ALL">All Types</option>
                <option value="numeric">Numeric</option>
                <option value="string">String</option>
                <option value="temporal">Temporal</option>
              </select>
            </div>
          </div>
        </template>

        <!-- SliderInput Properties -->
        <template v-if="component.component_type === 'SliderInput'">
          <div class="property-group">
            <div class="property-group-title">Slider Options</div>
            <div class="property-row">
              <label class="property-label">Min Value <span class="required">*</span></label>
              <input
                :value="component.min_value"
                type="number"
                class="property-input"
                @input="updateField('min_value', Number(($event.target as HTMLInputElement).value))"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Max Value <span class="required">*</span></label>
              <input
                :value="component.max_value"
                type="number"
                class="property-input"
                @input="updateField('max_value', Number(($event.target as HTMLInputElement).value))"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Step</label>
              <input
                :value="component.step"
                type="number"
                class="property-input"
                @input="updateField('step', Number(($event.target as HTMLInputElement).value))"
              />
            </div>
          </div>
        </template>

        <!-- ColumnActionInput Properties -->
        <template v-if="component.component_type === 'ColumnActionInput'">
          <div class="property-group">
            <div class="property-group-title">Column Action Options</div>
            <div class="property-row">
              <label class="property-label"
                >Actions (comma-separated) <span class="required">*</span></label
              >
              <input
                :value="component.actions_string"
                type="text"
                class="property-input"
                placeholder="sum, mean, min, max"
                @input="updateField('actions_string', ($event.target as HTMLInputElement).value)"
              />
              <span class="field-hint"
                >Actions available in dropdown (e.g., sum, mean, min, max)</span
              >
            </div>
            <div class="property-row">
              <label class="property-label">Output Name Template</label>
              <input
                :value="component.output_name_template"
                type="text"
                class="property-input"
                placeholder="{column}_{action}"
                @input="
                  updateField('output_name_template', ($event.target as HTMLInputElement).value)
                "
              />
              <span class="field-hint">Use {column} and {action} placeholders</span>
            </div>
            <div class="property-row">
              <label class="property-label">Data Types Filter</label>
              <select
                :value="component.data_types"
                class="property-input"
                @change="updateField('data_types', ($event.target as HTMLSelectElement).value)"
              >
                <option value="ALL">All Types</option>
                <option value="Numeric">Numeric</option>
                <option value="String">String</option>
                <option value="Date">Date/Time</option>
              </select>
            </div>
            <div class="property-row checkbox-row">
              <label class="property-label">Show Group By</label>
              <input
                :checked="component.show_group_by"
                type="checkbox"
                class="property-checkbox"
                @change="updateField('show_group_by', ($event.target as HTMLInputElement).checked)"
              />
            </div>
            <div class="property-row checkbox-row">
              <label class="property-label">Show Order By</label>
              <input
                :checked="component.show_order_by"
                type="checkbox"
                class="property-checkbox"
                @change="updateField('show_order_by', ($event.target as HTMLInputElement).checked)"
              />
            </div>
          </div>
        </template>

        <!-- SecretSelector Properties -->
        <template v-if="component.component_type === 'SecretSelector'">
          <div class="property-group">
            <div class="property-group-title">Secret Options</div>
            <div class="property-row checkbox-row">
              <label class="property-label">Required</label>
              <input
                :checked="component.required"
                type="checkbox"
                class="property-checkbox"
                @change="updateField('required', ($event.target as HTMLInputElement).checked)"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Description</label>
              <input
                :value="component.description"
                type="text"
                class="property-input"
                placeholder="Help text for the user"
                @input="updateField('description', ($event.target as HTMLInputElement).value)"
              />
            </div>
            <div class="property-row">
              <label class="property-label">Name Prefix Filter</label>
              <input
                :value="component.name_prefix"
                type="text"
                class="property-input"
                placeholder="e.g. API_KEY_"
                @input="updateField('name_prefix', ($event.target as HTMLInputElement).value)"
              />
              <span class="field-hint">Only show secrets starting with this prefix</span>
            </div>
          </div>
        </template>

        <!-- Insert Variable Button -->
        <div class="action-section">
          <button class="action-btn" @click="insertVariable">
            <i class="fa-solid fa-code"></i>
            Insert Variable
          </button>
          <span class="field-hint">Add typed variable to process method</span>
        </div>
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
import { toSnakeCase } from "./composables/useCodeGeneration";
import { getComponentIcon } from "./constants";

const props = defineProps<{
  component: DesignerComponent | null;
  sectionName: string;
}>();

const emit = defineEmits<{
  (e: "update", field: string, value: any): void;
  (e: "insert-variable", code: string): void;
}>();

function updateField(field: string, value: any) {
  emit("update", field, value);
}

function getTypeForComponent(componentType: string, multiple?: boolean): string {
  switch (componentType) {
    case "TextInput":
      return "str";
    case "NumericInput":
    case "SliderInput":
      return "float";
    case "ToggleSwitch":
      return "bool";
    case "SingleSelect":
      return "str";
    case "MultiSelect":
      return "list[str]";
    case "ColumnSelector":
      return multiple ? "list[str]" : "str";
    case "ColumnActionInput":
      return "dict";
    case "SecretSelector":
      return "SecretStr";
    default:
      return "Any";
  }
}

function insertVariable() {
  if (!props.component || !props.sectionName) return;

  const fieldName = toSnakeCase(props.component.field_name);
  const sectionName = toSnakeCase(props.sectionName);
  const pyType = getTypeForComponent(props.component.component_type, props.component.multiple);

  let code: string;
  if (props.component.component_type === "SecretSelector") {
    code = `    ${fieldName}: ${pyType} = self.settings_schema.${sectionName}.${fieldName}.secret_value`;
  } else {
    code = `    ${fieldName}: ${pyType} = self.settings_schema.${sectionName}.${fieldName}.value`;
  }

  emit("insert-variable", code);
}
</script>

<style scoped>
.property-form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.component-type-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  background: var(--color-background-secondary, #f3f4f6);
  border-radius: var(--border-radius-md, 6px);
  font-size: var(--font-size-sm, 0.875rem);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-primary, #374151);
}

.component-type-badge i {
  color: var(--color-text-secondary, #6b7280);
  font-size: 0.875rem;
}

.property-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3, 0.75rem);
}

.property-editor {
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 8px;
  padding: 1rem;
  background: var(--bg-secondary, #f8f9fa);
}

.property-group-title {
  font-size: var(--font-size-xs, 0.75rem);
  font-weight: var(--font-weight-semibold, 600);
  color: var(--color-text-secondary, #6b7280);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  padding-bottom: var(--spacing-2, 0.5rem);
  border-bottom: 1px solid var(--color-border-light, #e5e7eb);
}

.property-row {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1, 0.25rem);
}

.property-row.checkbox-row {
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
}

.property-label {
  font-size: var(--font-size-sm, 0.875rem);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-secondary, #6b7280);
}

.required {
  color: var(--color-danger, #ef4444);
}

.property-input {
  width: 100%;
  padding: var(--spacing-2, 0.5rem);
  font-size: var(--font-size-sm, 0.875rem);
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-md, 6px);
  background-color: var(--color-background-primary, #ffffff);
  color: var(--color-text-primary, #374151);
  transition: border-color var(--transition-fast, 0.15s);
}

.property-input:focus {
  outline: none;
  border-color: var(--input-border-focus, #4a6cf7);
}

.property-checkbox {
  width: 18px;
  height: 18px;
  cursor: pointer;
  accent-color: var(--primary-color, #4a6cf7);
}

.no-selection {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 200px;
  color: var(--color-text-secondary, #6b7280);
  text-align: center;
}

.no-selection i {
  font-size: 2rem;
  margin-bottom: 0.75rem;
  opacity: 0.5;
}

.no-selection p {
  margin: 0;
  font-size: var(--font-size-sm, 0.875rem);
}

.field-hint {
  font-size: var(--font-size-xs, 0.75rem);
  color: var(--color-text-secondary, #6b7280);
  margin-top: 0.25rem;
}

.action-section {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  padding-top: var(--spacing-4, 1rem);
  border-top: 1px solid var(--color-border-light, #e5e7eb);
}

.action-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
  padding: 0.625rem 1rem;
  background: var(--primary-color, #4a6cf7);
  color: white;
  border: none;
  border-radius: var(--border-radius-md, 6px);
  font-size: var(--font-size-sm, 0.875rem);
  font-weight: var(--font-weight-medium, 500);
  cursor: pointer;
  transition: background var(--transition-fast, 0.15s);
}

.action-btn:hover {
  background: var(--primary-color-hover, #3d5bd9);
}

.action-btn i {
  font-size: 0.875rem;
}
</style>
