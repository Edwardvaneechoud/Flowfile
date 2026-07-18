<template>
  <div class="panel control-inspector">
    <div class="panel-header">
      <h3>Control</h3>
    </div>
    <div class="panel-content">
      <div v-if="comp" class="inspector-form">
        <div class="type-badge-row">
          <div class="type-badge">
            <i :class="typeIcon"></i>
            <span>{{ comp.component_type }}</span>
          </div>
          <button
            class="type-help-btn"
            type="button"
            title="What does this component do?"
            data-testid="control-help"
            @click="showHelp = true"
          >
            <i class="fa-solid fa-circle-question"></i>
          </button>
        </div>

        <!-- Common -->
        <div class="field-group">
          <div class="field-group-title">Basic</div>
          <div class="field-row">
            <label>Field Name <span class="req">*</span></label>
            <input
              :value="comp.name"
              type="text"
              class="ins-input"
              placeholder="field_name"
              @focus="captureName"
              @input="updateName(($event.target as HTMLInputElement).value)"
              @change="commitName"
            />
          </div>
          <div class="field-row">
            <label>Label</label>
            <input
              :value="comp.label ?? ''"
              type="text"
              class="ins-input"
              placeholder="Display Label"
              @input="update('label', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </div>

        <!-- TextInput -->
        <div v-if="comp.component_type === 'TextInput'" class="field-group">
          <div class="field-group-title">Text</div>
          <div class="field-row">
            <label>Default Value</label>
            <input
              :value="comp.default ?? ''"
              type="text"
              class="ins-input"
              @input="update('default', ($event.target as HTMLInputElement).value)"
            />
          </div>
          <div class="field-row">
            <label>Placeholder</label>
            <input
              :value="comp.placeholder ?? ''"
              type="text"
              class="ins-input"
              @input="update('placeholder', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </div>

        <!-- NumericInput -->
        <div v-else-if="comp.component_type === 'NumericInput'" class="field-group">
          <div class="field-group-title">Number</div>
          <div class="field-row">
            <label>Default Value</label>
            <input
              :value="comp.default ?? ''"
              type="number"
              class="ins-input"
              @input="update('default', toNum(($event.target as HTMLInputElement).value))"
            />
          </div>
          <div class="field-row">
            <label>Min Value</label>
            <input
              :value="comp.min_value ?? ''"
              type="number"
              class="ins-input"
              @input="update('min_value', toNum(($event.target as HTMLInputElement).value))"
            />
          </div>
          <div class="field-row">
            <label>Max Value</label>
            <input
              :value="comp.max_value ?? ''"
              type="number"
              class="ins-input"
              @input="update('max_value', toNum(($event.target as HTMLInputElement).value))"
            />
          </div>
        </div>

        <!-- SliderInput -->
        <div v-else-if="comp.component_type === 'SliderInput'" class="field-group">
          <div class="field-group-title">Slider</div>
          <div class="field-row">
            <label>Min Value <span class="req">*</span></label>
            <input
              :value="comp.min_value"
              type="number"
              class="ins-input"
              @input="update('min_value', toNum(($event.target as HTMLInputElement).value) ?? 0)"
            />
          </div>
          <div class="field-row">
            <label>Max Value <span class="req">*</span></label>
            <input
              :value="comp.max_value"
              type="number"
              class="ins-input"
              @input="update('max_value', toNum(($event.target as HTMLInputElement).value) ?? 100)"
            />
          </div>
          <div class="field-row">
            <label>Step</label>
            <input
              :value="comp.step"
              type="number"
              class="ins-input"
              @input="update('step', toNum(($event.target as HTMLInputElement).value) ?? 1)"
            />
          </div>
        </div>

        <!-- ToggleSwitch -->
        <div v-else-if="comp.component_type === 'ToggleSwitch'" class="field-group">
          <div class="field-group-title">Toggle</div>
          <div class="field-row checkbox-row">
            <label>Default On</label>
            <input
              :checked="comp.default"
              type="checkbox"
              class="ins-checkbox"
              @change="update('default', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="field-row">
            <label>Description</label>
            <input
              :value="comp.description ?? ''"
              type="text"
              class="ins-input"
              @input="update('description', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </div>

        <!-- Single / MultiSelect -->
        <div
          v-else-if="
            comp.component_type === 'SingleSelect' || comp.component_type === 'MultiSelect'
          "
          class="field-group"
        >
          <div class="field-group-title">Options</div>
          <div class="field-row">
            <label>Options Source</label>
            <select
              :value="comp.options_source"
              class="ins-input"
              @change="update('options_source', ($event.target as HTMLSelectElement).value)"
            >
              <option value="static">Static Options</option>
              <option value="incoming_columns">Incoming Columns</option>
              <option
                v-if="isKernel || comp.options_source === 'available_artifacts'"
                value="available_artifacts"
              >
                Available Artifacts
              </option>
            </select>
            <span
              v-if="!isKernel && comp.options_source === 'available_artifacts'"
              class="field-hint"
            >
              Artifact selectors only work for kernel nodes.
            </span>
          </div>
          <div v-if="comp.options_source === 'static'" class="field-row">
            <label>Options (comma-separated)</label>
            <input
              :value="optionsCsv"
              type="text"
              class="ins-input"
              placeholder="a, b, c"
              @input="updateOptions(($event.target as HTMLInputElement).value)"
            />
          </div>
          <template v-if="comp.options_source === 'available_artifacts'">
            <div class="field-row">
              <label>Scope</label>
              <select
                :value="comp.artifact_scope"
                class="ins-input"
                @change="update('artifact_scope', ($event.target as HTMLSelectElement).value)"
              >
                <option value="upstream">Upstream (lineage)</option>
                <option value="global">Global (catalog)</option>
                <option value="all">All (upstream + global)</option>
              </select>
            </div>
            <div class="field-row">
              <label>Type Filter (comma-separated)</label>
              <input
                :value="artifactTypeFilterCsv"
                type="text"
                class="ins-input"
                placeholder="e.g. xgboost.*, *.Booster"
                @input="updateArtifactTypeFilter(($event.target as HTMLInputElement).value)"
              />
            </div>
          </template>
        </div>

        <!-- ColumnSelector -->
        <div v-else-if="comp.component_type === 'ColumnSelector'" class="field-group">
          <div class="field-group-title">Column</div>
          <div class="field-row checkbox-row">
            <label>Required</label>
            <input
              :checked="comp.required"
              type="checkbox"
              class="ins-checkbox"
              @change="update('required', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="field-row checkbox-row">
            <label>Multiple Selection</label>
            <input
              :checked="comp.multiple"
              type="checkbox"
              class="ins-checkbox"
              @change="update('multiple', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="field-row">
            <label>Data Types Filter</label>
            <el-select
              :model-value="dataTypesValue"
              multiple
              filterable
              collapse-tags
              collapse-tags-tooltip
              placeholder="All types"
              size="small"
              style="width: 100%"
              @update:model-value="updateDataTypes"
            >
              <el-option-group label="Groups">
                <el-option v-for="g in DATA_TYPE_GROUP_OPTIONS" :key="g" :label="g" :value="g" />
              </el-option-group>
              <el-option-group label="Specific types">
                <el-option v-for="t in DATA_TYPE_SPECIFIC_OPTIONS" :key="t" :label="t" :value="t" />
              </el-option-group>
            </el-select>
          </div>
        </div>

        <!-- SecretSelector -->
        <div v-else-if="comp.component_type === 'SecretSelector'" class="field-group">
          <div class="field-group-title">Secret</div>
          <div class="field-row checkbox-row">
            <label>Required</label>
            <input
              :checked="comp.required"
              type="checkbox"
              class="ins-checkbox"
              @change="update('required', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="field-row">
            <label>Description</label>
            <input
              :value="comp.description ?? ''"
              type="text"
              class="ins-input"
              @input="update('description', ($event.target as HTMLInputElement).value)"
            />
          </div>
          <div class="field-row">
            <label>Name Prefix Filter</label>
            <input
              :value="comp.name_prefix ?? ''"
              type="text"
              class="ins-input"
              placeholder="e.g. API_KEY_"
              @input="update('name_prefix', ($event.target as HTMLInputElement).value)"
            />
          </div>
        </div>

        <!-- ColumnActionInput -->
        <div v-else-if="comp.component_type === 'ColumnActionInput'" class="field-group">
          <div class="field-group-title">Column Action</div>
          <div class="field-row">
            <label>Actions (comma-separated) <span class="req">*</span></label>
            <input
              :value="actionsCsv"
              type="text"
              class="ins-input"
              placeholder="sum, mean, min, max"
              @input="updateActions(($event.target as HTMLInputElement).value)"
            />
          </div>
          <div class="field-row">
            <label>Output Name Template</label>
            <input
              :value="comp.output_name_template"
              type="text"
              class="ins-input"
              placeholder="{column}_{action}"
              @input="update('output_name_template', ($event.target as HTMLInputElement).value)"
            />
          </div>
          <div class="field-row">
            <label>Data Types Filter</label>
            <el-select
              :model-value="dataTypesValue"
              multiple
              filterable
              collapse-tags
              collapse-tags-tooltip
              placeholder="All types"
              size="small"
              style="width: 100%"
              @update:model-value="updateDataTypes"
            >
              <el-option-group label="Groups">
                <el-option v-for="g in DATA_TYPE_GROUP_OPTIONS" :key="g" :label="g" :value="g" />
              </el-option-group>
              <el-option-group label="Specific types">
                <el-option v-for="t in DATA_TYPE_SPECIFIC_OPTIONS" :key="t" :label="t" :value="t" />
              </el-option-group>
            </el-select>
          </div>
          <div class="field-row checkbox-row">
            <label>Show Group By</label>
            <input
              :checked="comp.show_group_by"
              type="checkbox"
              class="ins-checkbox"
              @change="update('show_group_by', ($event.target as HTMLInputElement).checked)"
            />
          </div>
          <div class="field-row checkbox-row">
            <label>Show Order By</label>
            <input
              :checked="comp.show_order_by"
              type="checkbox"
              class="ins-checkbox"
              @change="update('show_order_by', ($event.target as HTMLInputElement).checked)"
            />
          </div>
        </div>

        <div class="action-section">
          <button class="btn btn-primary" type="button" @click="insertVariable">
            <i class="fa-solid fa-code"></i>
            Insert Variable
          </button>
          <span class="field-hint">Add a typed accessor to your process method</span>
        </div>
      </div>

      <EmptyState
        v-else
        icon="fa-solid fa-arrow-pointer"
        description="Select a control in the form to edit it."
      />
    </div>

    <Teleport to="body">
      <ComponentReferenceModal
        :show="showHelp"
        :focus-type="comp?.component_type ?? null"
        @close="showHelp = false"
      />
    </Teleport>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { ComponentState, SelectOption } from "../designerState";
import { toSnakeCase } from "../mappers";
import {
  DATA_TYPE_GROUP_OPTIONS,
  DATA_TYPE_SPECIFIC_OPTIONS,
  getComponentIcon,
} from "../constants";
import { pyValueType } from "../componentDocs";
import EmptyState from "../../../components/common/EmptyState/EmptyState.vue";
import ComponentReferenceModal from "../ComponentReferenceModal.vue";

const emit = defineEmits<{ (e: "insert-variable", code: string): void }>();

const store = useNodeDesignerStore();

const comp = computed<ComponentState | null>(() => store.selectedComponent);

const isKernel = computed(() => store.environment.kind === "kernel");

const showHelp = ref(false);

const typeIcon = computed(() => getComponentIcon(comp.value?.component_type ?? ""));

function toNum(v: string): number | null {
  if (v.trim() === "") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

function update(field: string, value: unknown) {
  if (!comp.value) return;
  (comp.value as unknown as Record<string, unknown>)[field] = value;
}

const nameBeforeEdit = ref("");

function captureName() {
  nameBeforeEdit.value = comp.value?.name ?? "";
}

function updateName(value: string) {
  if (!comp.value) return;
  comp.value.name = toSnakeCase(value);
  store.syncPreviewValues();
}

// On blur, retarget any visible_when rule that pointed at this control's old name
// (no-op unless a section actually gates on it — only toggles are ever referenced).
function commitName() {
  const sectionIndex = store.selectedSectionIndex;
  if (!comp.value || sectionIndex === null) return;
  store.retargetVisibleWhenForComponent(
    store.sections[sectionIndex].name,
    nameBeforeEdit.value,
    comp.value.name,
  );
}

const optionsCsv = computed(() => {
  const c = comp.value;
  if (c && (c.component_type === "SingleSelect" || c.component_type === "MultiSelect")) {
    return c.options.map((o) => o.value).join(", ");
  }
  return "";
});

function updateOptions(raw: string) {
  const opts: SelectOption[] = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((v) => ({ value: v, label: v }));
  update("options", opts);
}

const artifactTypeFilterCsv = computed(() => {
  const c = comp.value;
  if (c && (c.component_type === "SingleSelect" || c.component_type === "MultiSelect")) {
    return (c.artifact_type_filter ?? []).join(", ");
  }
  return "";
});

function updateArtifactTypeFilter(raw: string) {
  const filters = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  update("artifact_type_filter", filters);
}

const actionsCsv = computed(() => {
  const c = comp.value;
  if (c && c.component_type === "ColumnActionInput") {
    return c.actions.map((a) => a.value).join(", ");
  }
  return "";
});

function updateActions(raw: string) {
  const acts: SelectOption[] = raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((v) => ({ value: v, label: v }));
  update("actions", acts);
}

const dataTypesValue = computed<string[]>(() => {
  const c = comp.value;
  if (c && (c.component_type === "ColumnSelector" || c.component_type === "ColumnActionInput")) {
    return c.data_types === "ALL" ? [] : c.data_types;
  }
  return [];
});

function updateDataTypes(values: string[]) {
  update("data_types", values.length ? values : "ALL");
}

function insertVariable() {
  const c = comp.value;
  if (!c || store.selectedSectionIndex === null) return;
  const section = store.sections[store.selectedSectionIndex];
  if (!section) return;
  const fieldName = toSnakeCase(c.name);
  const sectionName = toSnakeCase(section.name);
  let pyType = pyValueType(c.component_type);
  if (c.component_type === "ColumnSelector") pyType = c.multiple ? "list[str]" : "str";
  else if (c.component_type === "ColumnActionInput") pyType = `nd.${pyType}`;
  const accessor = c.component_type === "SecretSelector" ? "secret_value" : "value";
  emit(
    "insert-variable",
    `    ${fieldName}: ${pyType} = self.settings_schema.${sectionName}.${fieldName}.${accessor}`,
  );
}
</script>

<style scoped>
.control-inspector {
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
  background: var(--color-background-primary);
  overflow-y: auto;
  min-height: 0;
}

.control-inspector .panel-content {
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

.type-help-btn {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 1.75rem;
  height: 1.75rem;
  padding: 0;
  border: none;
  border-radius: var(--border-radius-md, 6px);
  background: transparent;
  color: var(--color-text-tertiary, #718096);
  cursor: pointer;
}

.type-help-btn:hover {
  background: var(--color-background-secondary, #f3f4f6);
  color: var(--color-accent, #0891b2);
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

.field-row.checkbox-row {
  flex-direction: row;
  align-items: center;
  justify-content: space-between;
}

.field-row label {
  font-size: 0.8125rem;
  font-weight: 500;
  color: var(--color-text-secondary, #6b7280);
}

.req {
  color: var(--color-danger, #ef4444);
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

.ins-checkbox {
  width: 18px;
  height: 18px;
  cursor: pointer;
  accent-color: var(--color-accent);
}

.action-section {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border-light, #e5e7eb);
}

.field-hint {
  font-size: 0.75rem;
  color: var(--color-text-secondary, #6b7280);
}
</style>
