<template>
  <div class="node-designer-container">
    <!-- Header -->
    <div class="page-header">
      <div class="header-left">
        <h2 class="page-title">Node Designer</h2>
        <p class="page-description">Design custom nodes visually</p>
      </div>
      <div class="header-actions">
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
                />
              </div>
              <div class="form-field icon-field">
                <IconSelector v-model="nodeMetadata.node_icon" />
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
  </div>
</template>

<script setup lang="ts">
import { watch, onMounted, computed } from "vue";
import axios from "axios";

// Child components
import ComponentPalette from "./nodeDesigner/ComponentPalette.vue";
import SectionCard from "./nodeDesigner/SectionCard.vue";
import PropertyEditor from "./nodeDesigner/PropertyEditor.vue";
import ProcessCodeEditor from "./nodeDesigner/ProcessCodeEditor.vue";
import CodePreviewModal from "./nodeDesigner/CodePreviewModal.vue";
import ValidationModal from "./nodeDesigner/ValidationModal.vue";
import NodeBrowserModal from "./nodeDesigner/NodeBrowserModal.vue";
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
import type { DesignerComponent } from "./nodeDesigner/types";

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

const storage = useSessionStorage(getState, setState, resetState);

// Setup auto-save and load on mount
watch([() => ({ ...nodeMetadata }), sections, processCode], () => storage.saveToSessionStorage(), {
  deep: true,
});

onMounted(() => {
  storage.loadFromSessionStorage();
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
  background: #4a6cf7;
  color: white;
  border: none;
  border-radius: 4px;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: background 0.2s;
}

.add-section-btn:hover {
  background: #3d5bd9;
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
</style>
