<template>
  <div class="node-designer-container">
    <!-- Header -->
    <div class="page-header">
      <div class="header-left">
        <h2 class="page-title">Node Designer</h2>
        <p class="page-description">Design custom nodes visually</p>
      </div>
      <div class="header-actions">
        <button class="btn btn-secondary" @click="showHelpModal = true">
          <i class="fa-solid fa-circle-question"></i>
          Help
        </button>
        <button class="btn btn-secondary" @click="nodeBrowser.openNodeBrowser()">
          <i class="fa-solid fa-folder-open"></i>
          Browse
        </button>
        <button class="btn btn-secondary" @click="handleNew">
          <i class="fa-solid fa-file"></i>
          New
        </button>
        <button class="btn btn-secondary" @click="handlePreview">
          <i class="fa-solid fa-code"></i>
          Preview
        </button>
        <button class="btn btn-primary" @click="handleSave">
          <i class="fa-solid fa-save"></i>
          Save
        </button>
      </div>
    </div>

    <!-- Main Content -->
    <div class="designer-layout">
      <!-- Left Panel: Component Palette -->
      <ComponentPalette />

      <!-- Center Panel: Design Canvas -->
      <div class="panel design-canvas">
        <div class="panel-header">
          <h3>Design Canvas</h3>
        </div>
        <div class="panel-content">
          <!-- Node Metadata -->
          <div class="metadata-section">
            <h4>Node Metadata</h4>
            <div class="form-grid">
              <div class="form-field">
                <label for="node-name">Node Name *</label>
                <input
                  id="node-name"
                  v-model="nodeMetadata.node_name"
                  type="text"
                  class="form-input"
                  placeholder="My Custom Node"
                />
              </div>
              <div class="form-field">
                <label for="node-category">Category *</label>
                <input
                  id="node-category"
                  v-model="nodeMetadata.node_category"
                  type="text"
                  class="form-input"
                  placeholder="Custom"
                />
              </div>
              <div class="form-field">
                <label for="node-title">Title</label>
                <input
                  id="node-title"
                  v-model="nodeMetadata.title"
                  type="text"
                  class="form-input"
                  placeholder="My Custom Node"
                />
              </div>
              <div class="form-field">
                <label for="node-intro">Description</label>
                <input
                  id="node-intro"
                  v-model="nodeMetadata.intro"
                  type="text"
                  class="form-input"
                  placeholder="A custom node for data processing"
                />
              </div>
              <div class="form-field">
                <label for="node-inputs">Number of Inputs</label>
                <input
                  id="node-inputs"
                  v-model.number="nodeMetadata.number_of_inputs"
                  type="number"
                  min="0"
                  max="10"
                  class="form-input"
                />
              </div>
              <div class="form-field">
                <label for="node-outputs">Number of Outputs</label>
                <input
                  id="node-outputs"
                  v-model.number="nodeMetadata.number_of_outputs"
                  type="number"
                  min="1"
                  max="10"
                  class="form-input"
                  :disabled="nodeMetadata.requires_kernel"
                />
              </div>
              <div class="form-field">
                <label class="checkbox-label">
                  <input
                    v-model="nodeMetadata.requires_kernel"
                    type="checkbox"
                    class="form-checkbox"
                  />
                  Require Kernel Execution
                </label>
              </div>
              <div class="form-field icon-field">
                <IconSelector v-model="nodeMetadata.node_icon" />
              </div>
            </div>
          </div>

          <!-- Kernel Execution -->
          <div v-if="nodeMetadata.requires_kernel" class="metadata-section kernel-section">
            <h4>Execution</h4>
            <div class="form-grid">
              <div class="form-field" style="grid-column: span 2">
                <label for="kernel-select">Kernel</label>
                <select
                  id="kernel-select"
                  v-model="nodeMetadata.kernel_id"
                  class="form-input"
                  @change="handleKernelChange"
                >
                  <option :value="null">Local (default)</option>
                  <option v-for="kernel in availableKernels" :key="kernel.id" :value="kernel.id">
                    {{ kernel.name }}
                    <template v-if="kernel.packages.length">
                      ({{ kernel.packages.join(", ") }})
                    </template>
                  </option>
                </select>
              </div>
              <div v-if="nodeMetadata.kernel_id" class="form-field" style="grid-column: span 2">
                <label>Output Names</label>
                <div class="output-names-list">
                  <div
                    v-for="(name, index) in nodeMetadata.output_names"
                    :key="index"
                    class="output-name-row"
                  >
                    <span class="output-handle-label">output-{{ index }}</span>
                    <input
                      :value="name"
                      type="text"
                      class="form-input"
                      placeholder="output name"
                      @input="updateOutputName(index, ($event.target as HTMLInputElement).value)"
                    />
                    <button
                      v-if="nodeMetadata.output_names.length > 1"
                      class="btn-icon-sm"
                      title="Remove output"
                      @click="removeOutputName(index)"
                    >
                      <i class="fa-solid fa-xmark"></i>
                    </button>
                  </div>
                  <button class="add-output-btn" @click="addOutputName">
                    <i class="fa-solid fa-plus"></i>
                    Add Output
                  </button>
                </div>
              </div>
              <div
                v-else-if="kernelSelectionRequired"
                class="form-field"
                style="grid-column: span 2"
              >
                <div class="kernel-error">
                  Kernel execution is required for this node. Select a kernel to enable it.
                </div>
              </div>
            </div>
          </div>

          <!-- Sections -->
          <div class="sections-area">
            <div class="sections-header">
              <h4>UI Sections</h4>
              <button class="add-section-btn" @click="addSection()">
                <i class="fa-solid fa-plus"></i>
                Add Section
              </button>
            </div>

            <SectionCard
              v-for="(section, sectionIndex) in sections"
              :key="sectionIndex"
              :section="section"
              :is-selected="selectedSectionIndex === sectionIndex"
              :selected-component-index="
                selectedSectionIndex === sectionIndex ? selectedComponentIndex : null
              "
              @select="selectSection(sectionIndex)"
              @remove="removeSection(sectionIndex)"
              @select-component="selectComponent(sectionIndex, $event)"
              @remove-component="removeComponent(sectionIndex, $event)"
              @drop="handleDrop($event, sectionIndex)"
              @update-name="
                section.name = $event;
                sanitizeSectionName(sectionIndex);
              "
              @update-title="section.title = $event"
            />

            <div v-if="sections.length === 0" class="empty-sections">
              <i class="fa-solid fa-layer-group"></i>
              <p>No sections yet. Add a section to start designing your node UI.</p>
            </div>
          </div>

          <!-- Python Code Editor -->
          <ProcessCodeEditor v-model="processCode" :extensions="autocompletion.extensions" />
        </div>
      </div>

      <!-- Right Panel: Property Editor -->
      <PropertyEditor
        :component="selectedComponent"
        :section-name="selectedSectionName"
        @update="handlePropertyUpdate"
        @insert-variable="handleInsertVariable"
      />
    </div>

    <!-- Modals -->
    <CodePreviewModal
      :show="codeGen.showPreviewModal.value"
      :code="codeGen.generatedCode.value"
      @close="codeGen.closePreview()"
    />

    <ValidationModal
      :show="validation.showValidationModal.value"
      :errors="validation.validationErrors.value"
      @close="validation.closeValidationModal()"
    />

    <NodeBrowserModal
      :show="nodeBrowser.showNodeBrowser.value"
      :nodes="nodeBrowser.customNodes.value"
      :loading="nodeBrowser.loadingNodes.value"
      :viewing-node-code="nodeBrowser.viewingNodeCode.value"
      :viewing-node-name="nodeBrowser.viewingNodeName.value"
      :show-delete-confirm="nodeBrowser.showDeleteConfirm.value"
      :read-only-extensions="autocompletion.readOnlyExtensions"
      @close="nodeBrowser.closeNodeBrowser()"
      @view-node="nodeBrowser.viewCustomNode($event)"
      @back="nodeBrowser.backToNodeList()"
      @confirm-delete="nodeBrowser.confirmDeleteNode()"
      @cancel-delete="nodeBrowser.showDeleteConfirm.value = false"
      @delete="nodeBrowser.deleteNode()"
    />

    <NodeDesignerHelpModal :show="showHelpModal" @close="showHelpModal = false" />
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted, computed } from "vue";
import axios from "axios";

// Child components
import ComponentPalette from "./nodeDesigner/ComponentPalette.vue";
import SectionCard from "./nodeDesigner/SectionCard.vue";
import PropertyEditor from "./nodeDesigner/PropertyEditor.vue";
import ProcessCodeEditor from "./nodeDesigner/ProcessCodeEditor.vue";
import CodePreviewModal from "./nodeDesigner/CodePreviewModal.vue";
import ValidationModal from "./nodeDesigner/ValidationModal.vue";
import NodeBrowserModal from "./nodeDesigner/NodeBrowserModal.vue";
import NodeDesignerHelpModal from "./nodeDesigner/NodeDesignerHelpModal.vue";
import IconSelector from "./nodeDesigner/IconSelector.vue";

// Composables
import {
  useNodeDesignerState,
  useSessionStorage,
  useNodeValidation,
  useCodeGeneration,
  useNodeBrowser,
  usePolarsAutocompletion,
  toSnakeCase,
} from "./nodeDesigner/composables";
import type { DesignerComponent, KernelInfo } from "./nodeDesigner/types";

// Initialize composables - destructure for proper TypeScript support
const {
  nodeMetadata,
  sections,
  selectedSectionIndex,
  selectedComponentIndex,
  processCode,
  selectedComponent,
  addSection,
  removeSection,
  selectSection,
  sanitizeSectionName,
  selectComponent,
  removeComponent,
  addComponentToSection,
  resetState,
  getState,
  setState,
} = useNodeDesignerState();

const validation = useNodeValidation();
const codeGen = useCodeGeneration();
const nodeBrowser = useNodeBrowser();
const autocompletion = usePolarsAutocompletion(() => sections.value);

const kernelSelectionRequired = computed(
  () => nodeMetadata.requires_kernel && !nodeMetadata.kernel_id,
);

watch(
  () => nodeMetadata.requires_kernel,
  (requiresKernel) => {
    if (requiresKernel && !nodeMetadata.kernel_id) {
      nodeMetadata.kernel_id = null;
      nodeMetadata.output_names = ["main"];
      nodeMetadata.number_of_outputs = nodeMetadata.output_names.length;
    }
  },
);

const storage = useSessionStorage(getState, setState, resetState);

// Help modal state
const showHelpModal = ref(false);

// Kernel state
const availableKernels = ref<KernelInfo[]>([]);

async function fetchKernels() {
  try {
    const response = await axios.get("/kernels/");
    availableKernels.value = response.data || [];
  } catch {
    // Kernels endpoint may not be available (no Docker), silently ignore
    availableKernels.value = [];
  }
}

function handleKernelChange() {
  // Sync number_of_outputs with output_names length when kernel mode changes
  if (nodeMetadata.kernel_id) {
    nodeMetadata.number_of_outputs = nodeMetadata.output_names.length;
  }
}

function addOutputName() {
  if (kernelSelectionRequired.value) {
    return;
  }
  const nextIndex = nodeMetadata.output_names.length;
  nodeMetadata.output_names.push(`output_${nextIndex}`);
  nodeMetadata.number_of_outputs = nodeMetadata.output_names.length;
}

function removeOutputName(index: number) {
  if (kernelSelectionRequired.value) {
    return;
  }
  if (nodeMetadata.output_names.length > 1) {
    nodeMetadata.output_names.splice(index, 1);
    nodeMetadata.number_of_outputs = nodeMetadata.output_names.length;
  }
}

function updateOutputName(index: number, value: string) {
  nodeMetadata.output_names[index] = value;
}

// Setup auto-save and load on mount
watch([() => ({ ...nodeMetadata }), sections, processCode], () => storage.saveToSessionStorage(), {
  deep: true,
});

onMounted(() => {
  storage.loadFromSessionStorage();
  fetchKernels();
});

// Event handlers
function handleNew() {
  storage.clearSessionStorage();
}

function handlePreview() {
  codeGen.previewCode(nodeMetadata, sections.value, processCode.value);
}

async function handleSave() {
  const errors = validation.validateSettings(nodeMetadata, sections.value, processCode.value);

  if (errors.length > 0) {
    validation.showErrors(errors);
    return;
  }

  const code = codeGen.generateCode(nodeMetadata, sections.value, processCode.value);
  const fileName = toSnakeCase(nodeMetadata.node_name) + ".py";

  try {
    await axios.post("/user_defined_components/save-custom-node", {
      file_name: fileName,
      code: code,
    });
    alert(`Node "${nodeMetadata.node_name}" saved successfully!`);
  } catch (error: any) {
    const errorMsg = error.response?.data?.detail || error.message || "Failed to save node";
    alert(`Error saving node: ${errorMsg}`);
  }
}

function handleDrop(event: DragEvent, sectionIndex: number) {
  const componentType = event.dataTransfer?.getData("component_type");
  if (!componentType) return;

  const compCount = sections.value[sectionIndex].components.length + 1;
  const newComponent: DesignerComponent = {
    component_type: componentType,
    field_name: `${toSnakeCase(componentType)}_${compCount}`,
    label: `${componentType} ${compCount}`,
    options_source: "static",
    options_string: "",
  };

  // Set defaults based on type
  if (componentType === "TextInput") {
    newComponent.default = "";
    newComponent.placeholder = "";
  } else if (componentType === "NumericInput") {
    newComponent.default = 0;
  } else if (componentType === "ToggleSwitch") {
    newComponent.default = false;
  } else if (componentType === "ColumnSelector") {
    newComponent.required = false;
    newComponent.multiple = false;
    newComponent.data_types = "ALL";
  } else if (componentType === "ColumnActionInput") {
    newComponent.actions_string = "sum, mean, min, max";
    newComponent.output_name_template = "{column}_{action}";
    newComponent.show_group_by = false;
    newComponent.show_order_by = false;
    newComponent.data_types = "ALL";
  } else if (componentType === "SliderInput") {
    newComponent.min_value = 0;
    newComponent.max_value = 100;
    newComponent.step = 1;
  }

  addComponentToSection(sectionIndex, newComponent);
}

function handlePropertyUpdate(field: string, value: any) {
  if (selectedComponent.value) {
    (selectedComponent.value as any)[field] = value;
  }
}

// Computed property for selected section name
const selectedSectionName = computed(() => {
  if (selectedSectionIndex.value === null) return "";
  const section = sections.value[selectedSectionIndex.value];
  return section?.name || section?.title || "";
});

// Insert variable into process code
function handleInsertVariable(code: string) {
  // Find the position after "def process..." line to insert the variable
  const lines = processCode.value.split("\n");
  let insertIndex = 1; // Default to after first line

  // Find the first non-empty, non-comment line after the def statement
  for (let i = 0; i < lines.length; i++) {
    const trimmed = lines[i].trim();
    if (trimmed.startsWith("def process")) {
      insertIndex = i + 1;
      // Skip any immediate comments after def
      while (
        insertIndex < lines.length &&
        (lines[insertIndex].trim().startsWith("#") || lines[insertIndex].trim() === "")
      ) {
        insertIndex++;
      }
      break;
    }
  }

  // Insert the variable assignment
  lines.splice(insertIndex, 0, code);
  processCode.value = lines.join("\n");
}
</script>

<style scoped>
@import "./nodeDesigner/nodeDesigner.css";

.node-designer-container {
  padding: 1rem;
  height: 100vh;
  max-height: 100vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  box-sizing: border-box;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
  flex-shrink: 0;
}

.header-left {
  display: flex;
  flex-direction: column;
}

.page-title {
  margin: 0;
  font-size: 1.5rem;
  font-weight: 600;
}

.page-description {
  margin: 0.25rem 0 0 0;
  color: var(--text-secondary);
  font-size: 0.875rem;
}

.header-actions {
  display: flex;
  gap: 0.5rem;
}

.designer-layout {
  display: grid;
  grid-template-columns: 200px 1fr 280px;
  gap: 1rem;
  flex: 1;
  min-height: 0;
  height: 0;
  overflow: hidden;
}

/* Design Canvas */
.design-canvas .panel-content {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  padding-bottom: 3rem;
}

.metadata-section h4 {
  margin: 0 0 0.75rem 0;
  font-size: 0.9375rem;
  font-weight: 600;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 0.75rem;
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

.icon-field {
  grid-column: span 2;
}

/* Sections */
.sections-area {
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 8px;
  padding: 1rem;
  background: var(--bg-secondary, #f8f9fa);
}

.sections-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.75rem;
  padding-bottom: 0.75rem;
  border-bottom: 1px solid var(--border-color, #e0e0e0);
}

.sections-header h4 {
  margin: 0;
  font-size: 0.9375rem;
  font-weight: 600;
}

.add-section-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.5rem 1rem;
  background: var(--color-button-primary);
  color: var(--color-text-inverse);
  border: none;
  border-radius: var(--border-radius-sm);
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: background var(--transition-fast);
}

.add-section-btn:hover {
  background: var(--color-button-primary-hover);
}

.add-section-btn i {
  font-size: 0.75rem;
}

.empty-sections {
  text-align: center;
  padding: 2rem;
  color: var(--text-secondary);
}

.empty-sections i {
  font-size: 2rem;
  margin-bottom: 0.5rem;
}

.empty-sections p {
  margin: 0;
}

/* Kernel section */
.kernel-section {
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 8px;
  padding: 1rem;
  background: var(--bg-secondary, #f8f9fa);
}

.output-names-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.output-name-row {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.output-handle-label {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--text-secondary);
  white-space: nowrap;
  min-width: 60px;
}

.btn-icon-sm {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  padding: 0;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: transparent;
  color: var(--text-secondary);
  cursor: pointer;
  flex-shrink: 0;
}

.btn-icon-sm:hover {
  background: var(--color-background-danger, #fee2e2);
  color: var(--color-text-danger, #dc2626);
  border-color: var(--color-text-danger, #dc2626);
}

.add-output-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.75rem;
  background: transparent;
  color: var(--color-button-primary);
  border: 1px dashed var(--color-button-primary);
  border-radius: var(--border-radius-sm);
  font-size: 0.8125rem;
  cursor: pointer;
  transition: background var(--transition-fast);
  align-self: flex-start;
}

.add-output-btn:hover {
  background: var(--color-button-primary);
  color: var(--color-text-inverse);
}

.add-output-btn i {
  font-size: 0.6875rem;
}

.kernel-error {
  font-size: 0.8125rem;
  color: var(--color-text-danger, #dc2626);
}

.form-checkbox {
  width: 18px;
  height: 18px;
  margin-right: 0.5rem;
  accent-color: var(--primary-color, #4a6cf7);
}

.checkbox-label {
  display: flex;
  align-items: center;
  font-size: var(--font-size-sm, 0.875rem);
  font-weight: var(--font-weight-medium, 500);
  color: var(--color-text-secondary, #6b7280);
}
</style>
