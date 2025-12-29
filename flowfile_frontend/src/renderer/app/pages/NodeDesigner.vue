<template>
  <div class="node-designer-container">
    <!-- Header -->
    <div class="page-header">
      <div class="header-left">
        <h2 class="page-title">Node Designer</h2>
        <p class="page-description">Design custom nodes visually and generate Python code</p>
      </div>
      <div class="header-actions">
        <button class="btn btn-secondary" @click="previewCode">
          <i class="fa-solid fa-code"></i>
          Preview Code
        </button>
        <button class="btn btn-primary" :disabled="!canSave" @click="saveNode">
          <i class="fa-solid fa-save"></i>
          Save Node
        </button>
      </div>
    </div>

    <!-- Main Content -->
    <div class="designer-layout">
      <!-- Left Panel: Component Palette -->
      <div class="panel component-palette">
        <div class="panel-header">
          <h3>Components</h3>
        </div>
        <div class="panel-content">
          <div
            v-for="comp in availableComponents"
            :key="comp.type"
            class="component-item"
            draggable="true"
            @dragstart="handleDragStart($event, comp)"
          >
            <i :class="comp.icon"></i>
            <span>{{ comp.label }}</span>
          </div>
        </div>
      </div>

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
            </div>
          </div>

          <!-- Sections -->
          <div class="sections-area">
            <div class="sections-header">
              <h4>UI Sections</h4>
              <button class="add-section-btn" @click="addSection">
                <i class="fa-solid fa-plus"></i>
                Add Section
              </button>
            </div>

            <div
              v-for="(section, sectionIndex) in sections"
              :key="sectionIndex"
              class="section-card"
              :class="{ 'selected': selectedSectionIndex === sectionIndex }"
              @click="selectSection(sectionIndex)"
            >
              <div class="section-header">
                <div class="section-fields">
                  <div class="section-field">
                    <label>Variable Name</label>
                    <input
                      v-model="section.name"
                      type="text"
                      class="section-name-input"
                      placeholder="section_name"
                      @click.stop
                      @input="sanitizeSectionName(sectionIndex)"
                    />
                  </div>
                  <div class="section-field">
                    <label>Display Title</label>
                    <input
                      v-model="section.title"
                      type="text"
                      class="section-title-input"
                      placeholder="Section Title"
                      @click.stop
                    />
                  </div>
                </div>
                <button class="btn-icon" @click.stop="removeSection(sectionIndex)">
                  <i class="fa-solid fa-trash"></i>
                </button>
              </div>

              <div
                class="section-components"
                @dragover.prevent
                @drop="handleDrop($event, sectionIndex)"
              >
                <div
                  v-for="(component, compIndex) in section.components"
                  :key="compIndex"
                  class="component-card"
                  :class="{ 'selected': selectedSectionIndex === sectionIndex && selectedComponentIndex === compIndex }"
                  @click.stop="selectComponent(sectionIndex, compIndex)"
                >
                  <div class="component-preview">
                    <i :class="getComponentIcon(component.component_type)"></i>
                    <span class="component-label">{{ component.label || component.component_type }}</span>
                    <span class="component-type">({{ component.component_type }})</span>
                  </div>
                  <button class="btn-icon btn-remove" @click.stop="removeComponent(sectionIndex, compIndex)">
                    <i class="fa-solid fa-times"></i>
                  </button>
                </div>
                <div v-if="section.components.length === 0" class="drop-zone">
                  <i class="fa-solid fa-plus"></i>
                  <span>Drop components here</span>
                </div>
              </div>
            </div>

            <div v-if="sections.length === 0" class="empty-sections">
              <i class="fa-solid fa-layer-group"></i>
              <p>No sections yet. Add a section to start designing your node UI.</p>
            </div>
          </div>

          <!-- Python Code Editor -->
          <div class="code-editor-section">
            <h4>Process Method</h4>
            <p class="code-hint">Write your data transformation logic. Access settings via <code>self.settings_schema.section_name.component_name.value</code></p>
            <div class="code-editor-wrapper">
              <codemirror
                v-model="processCode"
                placeholder="# Write your process logic here..."
                :style="{ height: '300px' }"
                :autofocus="false"
                :indent-with-tab="false"
                :tab-size="4"
                :extensions="extensions"
              />
            </div>
          </div>
        </div>
      </div>

      <!-- Right Panel: Property Editor -->
      <div class="panel property-editor">
        <div class="panel-header">
          <h3>Properties</h3>
        </div>
        <div class="panel-content">
          <div v-if="selectedComponent" class="property-form">
            <h4>{{ selectedComponent.component_type }} Properties</h4>

            <!-- Common Properties -->
            <div class="form-field">
              <label>Field Name *</label>
              <input
                v-model="selectedComponent.field_name"
                type="text"
                class="form-input"
                placeholder="field_name"
              />
            </div>
            <div class="form-field">
              <label>Label</label>
              <input
                v-model="selectedComponent.label"
                type="text"
                class="form-input"
                placeholder="Display Label"
              />
            </div>

            <!-- TextInput Properties -->
            <template v-if="selectedComponent.component_type === 'TextInput'">
              <div class="form-field">
                <label>Default Value</label>
                <input
                  v-model="selectedComponent.default"
                  type="text"
                  class="form-input"
                  placeholder="Default value"
                />
              </div>
              <div class="form-field">
                <label>Placeholder</label>
                <input
                  v-model="selectedComponent.placeholder"
                  type="text"
                  class="form-input"
                  placeholder="Placeholder text"
                />
              </div>
            </template>

            <!-- NumericInput Properties -->
            <template v-if="selectedComponent.component_type === 'NumericInput'">
              <div class="form-field">
                <label>Default Value</label>
                <input
                  v-model.number="selectedComponent.default"
                  type="number"
                  class="form-input"
                />
              </div>
              <div class="form-field">
                <label>Min Value</label>
                <input
                  v-model.number="selectedComponent.min_value"
                  type="number"
                  class="form-input"
                />
              </div>
              <div class="form-field">
                <label>Max Value</label>
                <input
                  v-model.number="selectedComponent.max_value"
                  type="number"
                  class="form-input"
                />
              </div>
            </template>

            <!-- ToggleSwitch Properties -->
            <template v-if="selectedComponent.component_type === 'ToggleSwitch'">
              <div class="form-field">
                <label>Default Value</label>
                <input
                  v-model="selectedComponent.default"
                  type="checkbox"
                  class="form-checkbox"
                />
              </div>
              <div class="form-field">
                <label>Description</label>
                <input
                  v-model="selectedComponent.description"
                  type="text"
                  class="form-input"
                  placeholder="Toggle description"
                />
              </div>
            </template>

            <!-- SingleSelect Properties -->
            <template v-if="selectedComponent.component_type === 'SingleSelect'">
              <div class="form-field">
                <label>Options Source</label>
                <select v-model="selectedComponent.options_source" class="form-input">
                  <option value="static">Static Options</option>
                  <option value="incoming_columns">Incoming Columns</option>
                </select>
              </div>
              <div v-if="selectedComponent.options_source === 'static'" class="form-field">
                <label>Options (comma-separated)</label>
                <input
                  v-model="selectedComponent.options_string"
                  type="text"
                  class="form-input"
                  placeholder="option1, option2, option3"
                />
              </div>
            </template>

            <!-- MultiSelect Properties -->
            <template v-if="selectedComponent.component_type === 'MultiSelect'">
              <div class="form-field">
                <label>Options Source</label>
                <select v-model="selectedComponent.options_source" class="form-input">
                  <option value="static">Static Options</option>
                  <option value="incoming_columns">Incoming Columns</option>
                </select>
              </div>
              <div v-if="selectedComponent.options_source === 'static'" class="form-field">
                <label>Options (comma-separated)</label>
                <input
                  v-model="selectedComponent.options_string"
                  type="text"
                  class="form-input"
                  placeholder="option1, option2, option3"
                />
              </div>
            </template>

            <!-- ColumnSelector Properties -->
            <template v-if="selectedComponent.component_type === 'ColumnSelector'">
              <div class="form-field">
                <label>Required</label>
                <input
                  v-model="selectedComponent.required"
                  type="checkbox"
                  class="form-checkbox"
                />
              </div>
              <div class="form-field">
                <label>Multiple Selection</label>
                <input
                  v-model="selectedComponent.multiple"
                  type="checkbox"
                  class="form-checkbox"
                />
              </div>
              <div class="form-field">
                <label>Data Types Filter</label>
                <select v-model="selectedComponent.data_types" class="form-input">
                  <option value="ALL">All Types</option>
                  <option value="numeric">Numeric</option>
                  <option value="string">String</option>
                  <option value="temporal">Temporal</option>
                </select>
              </div>
            </template>

            <!-- SliderInput Properties -->
            <template v-if="selectedComponent.component_type === 'SliderInput'">
              <div class="form-field">
                <label>Min Value *</label>
                <input
                  v-model.number="selectedComponent.min_value"
                  type="number"
                  class="form-input"
                />
              </div>
              <div class="form-field">
                <label>Max Value *</label>
                <input
                  v-model.number="selectedComponent.max_value"
                  type="number"
                  class="form-input"
                />
              </div>
              <div class="form-field">
                <label>Step</label>
                <input
                  v-model.number="selectedComponent.step"
                  type="number"
                  class="form-input"
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
    </div>

    <!-- Code Preview Modal -->
    <div v-if="showPreviewModal" class="modal-overlay" @click="closePreview">
      <div class="modal-container modal-large" @click.stop>
        <div class="modal-header">
          <h3 class="modal-title">Generated Python Code</h3>
          <button class="modal-close" @click="closePreview">
            <i class="fa-solid fa-times"></i>
          </button>
        </div>
        <div class="modal-content">
          <div class="code-preview">
            <pre><code>{{ generatedCode }}</code></pre>
          </div>
        </div>
        <div class="modal-actions">
          <button class="btn btn-secondary" @click="copyCode">
            <i class="fa-solid fa-copy"></i>
            Copy Code
          </button>
          <button class="btn btn-primary" @click="closePreview">
            Close
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, reactive, watch } from "vue";
import { EditorView, keymap } from "@codemirror/view";
import { EditorState, Extension, Compartment } from "@codemirror/state";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { oneDark } from "@codemirror/theme-one-dark";
import { autocompletion, CompletionContext, CompletionResult, acceptCompletion } from "@codemirror/autocomplete";
import { indentMore } from "@codemirror/commands";
import axios from "axios";

// Available component types
const availableComponents = [
  { type: "TextInput", label: "Text Input", icon: "fa-solid fa-font" },
  { type: "NumericInput", label: "Numeric Input", icon: "fa-solid fa-hashtag" },
  { type: "ToggleSwitch", label: "Toggle Switch", icon: "fa-solid fa-toggle-on" },
  { type: "SingleSelect", label: "Single Select", icon: "fa-solid fa-list" },
  { type: "MultiSelect", label: "Multi Select", icon: "fa-solid fa-list-check" },
  { type: "ColumnSelector", label: "Column Selector", icon: "fa-solid fa-table-columns" },
  { type: "SliderInput", label: "Slider", icon: "fa-solid fa-sliders" },
];

// Node metadata
const nodeMetadata = reactive({
  node_name: "",
  node_category: "Custom",
  title: "",
  intro: "",
  number_of_inputs: 1,
  number_of_outputs: 1,
});

// Component interface
interface DesignerComponent {
  component_type: string;
  field_name: string;
  label: string;
  default?: any;
  placeholder?: string;
  min_value?: number;
  max_value?: number;
  step?: number;
  description?: string;
  required?: boolean;
  multiple?: boolean;
  data_types?: string;
  options_source?: string;
  options_string?: string;
}

// Section interface
interface DesignerSection {
  name: string;
  title: string;
  components: DesignerComponent[];
}

// Sections state
const sections = ref<DesignerSection[]>([]);
const selectedSectionIndex = ref<number | null>(null);
const selectedComponentIndex = ref<number | null>(null);

// Python code for process method
const processCode = ref(`def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
    # Get the first input LazyFrame
    lf = inputs[0]

    # Access settings values like this:
    # value = self.settings_schema.section_name.field_name.value

    # Your transformation logic here
    # Example: lf = lf.filter(pl.col("column") > 0)

    return lf`);

// Dynamic autocompletion based on schema
function schemaCompletions(context: CompletionContext): CompletionResult | null {
  // Match patterns like "self.settings_schema." or "self.settings_schema.section_name."
  const beforeCursor = context.state.doc.sliceString(0, context.pos);

  // Check for "self.settings_schema."
  const settingsMatch = beforeCursor.match(/self\.settings_schema\.(\w*)$/);
  if (settingsMatch) {
    const typed = settingsMatch[1];
    const sectionOptions = sections.value.map(section => {
      // Use the sanitized variable name directly
      const sectionName = section.name || toSnakeCase(section.title || "section");
      const sectionTitle = section.title || section.name || "Section";
      return {
        label: sectionName,
        type: "property",
        info: `Section: ${sectionTitle}`,
        detail: "Section",
      };
    });

    return {
      from: context.pos - typed.length,
      options: sectionOptions,
      validFor: /^\w*$/,
    };
  }

  // Check for "self.settings_schema.section_name."
  for (const section of sections.value) {
    // Use the sanitized variable name directly
    const sectionName = section.name || toSnakeCase(section.title || "section");
    const sectionMatch = beforeCursor.match(new RegExp(`self\\.settings_schema\\.${sectionName}\\.([\\w]*)$`));

    if (sectionMatch) {
      const typed = sectionMatch[1];
      const componentOptions = section.components.map(comp => {
        const fieldName = toSnakeCase(comp.field_name);
        return {
          label: fieldName,
          type: "property",
          info: `${comp.component_type}: ${comp.label}`,
          detail: comp.component_type,
          apply: fieldName + ".value",
        };
      });

      return {
        from: context.pos - typed.length,
        options: componentOptions,
        validFor: /^\w*$/,
      };
    }
  }

  // Common Polars completions
  const wordMatch = context.matchBefore(/\w+/);
  if (!wordMatch && !context.explicit) return null;

  const polarsCompletions = [
    // Settings access
    { label: "self.settings_schema", type: "property", info: "Access node settings", apply: "self.settings_schema." },

    // Input dataframes
    { label: "inputs[0]", type: "variable", info: "First input LazyFrame" },
    { label: "inputs[1]", type: "variable", info: "Second input LazyFrame" },

    // Polars expressions
    { label: "pl.col", type: "function", info: "Select a column by name", apply: 'pl.col("")' },
    { label: "pl.lit", type: "function", info: "Create a literal value", apply: 'pl.lit()' },
    { label: "pl.all", type: "function", info: "Select all columns", apply: 'pl.all()' },
    { label: "pl.exclude", type: "function", info: "Select all except specified", apply: 'pl.exclude("")' },
    { label: "pl.when", type: "function", info: "Start conditional expression", apply: 'pl.when().then().otherwise()' },
    { label: "pl.concat", type: "function", info: "Concatenate LazyFrames", apply: 'pl.concat([])' },
    { label: "pl.struct", type: "function", info: "Create struct column", apply: 'pl.struct([])' },

    // LazyFrame methods - Selection & Filtering
    { label: "lf.select", type: "method", info: "Select columns", apply: 'lf.select()' },
    { label: "lf.filter", type: "method", info: "Filter rows by condition", apply: 'lf.filter()' },
    { label: "lf.with_columns", type: "method", info: "Add or modify columns", apply: 'lf.with_columns()' },
    { label: "lf.drop", type: "method", info: "Drop columns", apply: 'lf.drop()' },
    { label: "lf.rename", type: "method", info: "Rename columns", apply: 'lf.rename({})' },
    { label: "lf.cast", type: "method", info: "Cast column types", apply: 'lf.cast({})' },

    // LazyFrame methods - Sorting & Limiting
    { label: "lf.sort", type: "method", info: "Sort by columns", apply: 'lf.sort("")' },
    { label: "lf.head", type: "method", info: "Get first n rows", apply: 'lf.head()' },
    { label: "lf.tail", type: "method", info: "Get last n rows", apply: 'lf.tail()' },
    { label: "lf.limit", type: "method", info: "Limit number of rows", apply: 'lf.limit()' },
    { label: "lf.slice", type: "method", info: "Slice rows by offset and length", apply: 'lf.slice()' },
    { label: "lf.unique", type: "method", info: "Get unique rows", apply: 'lf.unique()' },

    // LazyFrame methods - Grouping & Aggregation
    { label: "lf.group_by", type: "method", info: "Group by columns", apply: 'lf.group_by().agg()' },
    { label: "lf.agg", type: "method", info: "Aggregate expressions", apply: 'lf.agg()' },
    { label: "lf.rolling", type: "method", info: "Rolling window operations", apply: 'lf.rolling()' },
    { label: "lf.group_by_dynamic", type: "method", info: "Dynamic time-based grouping", apply: 'lf.group_by_dynamic()' },

    // LazyFrame methods - Joins
    { label: "lf.join", type: "method", info: "Join with another LazyFrame", apply: 'lf.join(other, on="", how="left")' },
    { label: "lf.join_asof", type: "method", info: "As-of join for time series", apply: 'lf.join_asof()' },
    { label: "lf.cross_join", type: "method", info: "Cross join (cartesian product)", apply: 'lf.cross_join()' },

    // LazyFrame methods - Reshaping
    { label: "lf.explode", type: "method", info: "Explode list column to rows", apply: 'lf.explode("")' },
    { label: "lf.unpivot", type: "method", info: "Unpivot wide to long format", apply: 'lf.unpivot()' },
    { label: "lf.pivot", type: "method", info: "Pivot long to wide format", apply: 'lf.pivot()' },
    { label: "lf.unnest", type: "method", info: "Unnest struct column", apply: 'lf.unnest("")' },

    // LazyFrame methods - Missing data
    { label: "lf.fill_null", type: "method", info: "Fill null values", apply: 'lf.fill_null()' },
    { label: "lf.fill_nan", type: "method", info: "Fill NaN values", apply: 'lf.fill_nan()' },
    { label: "lf.drop_nulls", type: "method", info: "Drop rows with nulls", apply: 'lf.drop_nulls()' },
    { label: "lf.interpolate", type: "method", info: "Interpolate null values", apply: 'lf.interpolate()' },

    // LazyFrame methods - Other
    { label: "lf.with_row_index", type: "method", info: "Add row index column", apply: 'lf.with_row_index("index")' },
    { label: "lf.reverse", type: "method", info: "Reverse row order", apply: 'lf.reverse()' },
    { label: "lf.collect", type: "method", info: "Execute and collect to DataFrame", apply: 'lf.collect()' },
    { label: "lf.lazy", type: "method", info: "Convert to LazyFrame", apply: 'lf.lazy()' },

    // Expression methods (chainable)
    { label: ".alias", type: "method", info: "Rename expression result", apply: '.alias("")' },
    { label: ".cast", type: "method", info: "Cast to type", apply: '.cast(pl.Utf8)' },
    { label: ".is_null", type: "method", info: "Check for null", apply: '.is_null()' },
    { label: ".is_not_null", type: "method", info: "Check for not null", apply: '.is_not_null()' },
    { label: ".fill_null", type: "method", info: "Fill null values", apply: '.fill_null()' },
    { label: ".sum", type: "method", info: "Sum values", apply: '.sum()' },
    { label: ".mean", type: "method", info: "Calculate mean", apply: '.mean()' },
    { label: ".min", type: "method", info: "Get minimum", apply: '.min()' },
    { label: ".max", type: "method", info: "Get maximum", apply: '.max()' },
    { label: ".count", type: "method", info: "Count values", apply: '.count()' },
    { label: ".first", type: "method", info: "Get first value", apply: '.first()' },
    { label: ".last", type: "method", info: "Get last value", apply: '.last()' },
    { label: ".str", type: "property", info: "String operations namespace", apply: '.str.' },
    { label: ".dt", type: "property", info: "Datetime operations namespace", apply: '.dt.' },
    { label: ".list", type: "property", info: "List operations namespace", apply: '.list.' },
    { label: ".over", type: "method", info: "Window function over groups", apply: '.over("")' },
  ];

  return {
    from: wordMatch ? wordMatch.from : context.pos,
    options: polarsCompletions,
    validFor: /^\w*$/,
  };
}

// Tab keymap for accepting completions
const tabKeymap = keymap.of([
  {
    key: "Tab",
    run: (view: EditorView): boolean => {
      if (acceptCompletion(view)) {
        return true;
      }
      return indentMore(view);
    },
  },
]);

// CodeMirror extensions
const extensions: Extension[] = [
  python(),
  oneDark,
  EditorState.tabSize.of(4),
  autocompletion({
    override: [schemaCompletions],
    defaultKeymap: false,
    closeOnBlur: false,
  }),
  tabKeymap,
];

// Preview modal
const showPreviewModal = ref(false);
const generatedCode = ref("");

// Computed properties
const selectedComponent = computed(() => {
  if (selectedSectionIndex.value !== null && selectedComponentIndex.value !== null) {
    return sections.value[selectedSectionIndex.value]?.components[selectedComponentIndex.value] || null;
  }
  return null;
});

const canSave = computed(() => {
  return nodeMetadata.node_name.trim() !== "" &&
         nodeMetadata.node_category.trim() !== "" &&
         sections.value.length > 0;
});

// Helper functions
function getComponentIcon(type: string): string {
  const comp = availableComponents.find(c => c.type === type);
  return comp?.icon || "fa-solid fa-puzzle-piece";
}

function toSnakeCase(str: string): string {
  return str
    .replace(/\s+/g, '_')
    .replace(/([a-z])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '');
}

function toPascalCase(str: string): string {
  return str
    .split(/[\s_-]+/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join('');
}

function sanitizeSectionName(index: number) {
  // Ensure section name is a valid Python identifier
  let name = sections.value[index].name;
  // Replace spaces and hyphens with underscores
  name = name.replace(/[\s-]+/g, '_');
  // Remove any non-alphanumeric characters except underscores
  name = name.replace(/[^a-zA-Z0-9_]/g, '');
  // Ensure it doesn't start with a number
  if (/^[0-9]/.test(name)) {
    name = '_' + name;
  }
  // Convert to lowercase
  name = name.toLowerCase();
  sections.value[index].name = name;
}

// Section management
function addSection() {
  const sectionNumber = sections.value.length + 1;
  sections.value.push({
    name: `section_${sectionNumber}`,
    title: `Section ${sectionNumber}`,
    components: [],
  });
  selectedSectionIndex.value = sections.value.length - 1;
  selectedComponentIndex.value = null;
}

function removeSection(index: number) {
  sections.value.splice(index, 1);
  if (selectedSectionIndex.value === index) {
    selectedSectionIndex.value = null;
    selectedComponentIndex.value = null;
  }
}

function selectSection(index: number) {
  selectedSectionIndex.value = index;
  selectedComponentIndex.value = null;
}

// Component management
function selectComponent(sectionIndex: number, compIndex: number) {
  selectedSectionIndex.value = sectionIndex;
  selectedComponentIndex.value = compIndex;
}

function removeComponent(sectionIndex: number, compIndex: number) {
  sections.value[sectionIndex].components.splice(compIndex, 1);
  if (selectedSectionIndex.value === sectionIndex && selectedComponentIndex.value === compIndex) {
    selectedComponentIndex.value = null;
  }
}

// Drag and drop
function handleDragStart(event: DragEvent, component: { type: string }) {
  event.dataTransfer?.setData("component_type", component.type);
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

  sections.value[sectionIndex].components.push(newComponent);
  selectedSectionIndex.value = sectionIndex;
  selectedComponentIndex.value = sections.value[sectionIndex].components.length - 1;
}

// Code generation
function generateCode(): string {
  const nodeName = toPascalCase(nodeMetadata.node_name || "MyCustomNode");
  const nodeSettingsName = `${nodeName}Settings`;

  // Build imports
  const imports = new Set<string>();
  imports.add("CustomNodeBase");
  imports.add("Section");
  imports.add("NodeSettings");

  sections.value.forEach(section => {
    section.components.forEach(comp => {
      imports.add(comp.component_type);
      if (comp.options_source === "incoming_columns") {
        imports.add("IncomingColumns");
      }
    });
  });

  // Generate sections code
  let sectionsCode = "";
  sections.value.forEach(section => {
    // Use the sanitized variable name directly, fallback to snake_case of title if empty
    const sectionName = section.name || toSnakeCase(section.title || "section");
    const sectionTitle = section.title || section.name || "Section";
    sectionsCode += `\n# ${sectionTitle}\n`;
    sectionsCode += `${sectionName} = Section(\n`;
    sectionsCode += `    title="${sectionTitle}",\n`;

    section.components.forEach(comp => {
      const fieldName = toSnakeCase(comp.field_name);
      sectionsCode += `    ${fieldName}=${comp.component_type}(\n`;
      sectionsCode += `        label="${comp.label || fieldName}",\n`;

      if (comp.component_type === "TextInput") {
        if (comp.default) sectionsCode += `        default="${comp.default}",\n`;
        if (comp.placeholder) sectionsCode += `        placeholder="${comp.placeholder}",\n`;
      } else if (comp.component_type === "NumericInput") {
        if (comp.default !== undefined) sectionsCode += `        default=${comp.default},\n`;
        if (comp.min_value !== undefined) sectionsCode += `        min_value=${comp.min_value},\n`;
        if (comp.max_value !== undefined) sectionsCode += `        max_value=${comp.max_value},\n`;
      } else if (comp.component_type === "ToggleSwitch") {
        sectionsCode += `        default=${comp.default ? "True" : "False"},\n`;
        if (comp.description) sectionsCode += `        description="${comp.description}",\n`;
      } else if (comp.component_type === "SingleSelect" || comp.component_type === "MultiSelect") {
        if (comp.options_source === "incoming_columns") {
          sectionsCode += `        options=IncomingColumns,\n`;
        } else if (comp.options_string) {
          const options = comp.options_string.split(",").map(o => `"${o.trim()}"`).join(", ");
          sectionsCode += `        options=[${options}],\n`;
        }
      } else if (comp.component_type === "ColumnSelector") {
        if (comp.required) sectionsCode += `        required=True,\n`;
        if (comp.multiple) sectionsCode += `        multiple=True,\n`;
        if (comp.data_types && comp.data_types !== "ALL") {
          sectionsCode += `        data_types="${comp.data_types}",\n`;
        }
      } else if (comp.component_type === "SliderInput") {
        sectionsCode += `        min_value=${comp.min_value ?? 0},\n`;
        sectionsCode += `        max_value=${comp.max_value ?? 100},\n`;
        if (comp.step) sectionsCode += `        step=${comp.step},\n`;
      }

      sectionsCode += `    ),\n`;
    });

    sectionsCode += `)\n`;
  });

  // Generate settings class
  let settingsCode = `\nclass ${nodeSettingsName}(NodeSettings):\n`;
  sections.value.forEach(section => {
    // Use the sanitized variable name directly
    const sectionName = section.name || toSnakeCase(section.title || "section");
    settingsCode += `    ${sectionName}: Section = ${sectionName}\n`;
  });
  if (sections.value.length === 0) {
    settingsCode += `    pass\n`;
  }

  // Extract process method body (remove the def line and dedent)
  let processBody = processCode.value;
  const defMatch = processBody.match(/def\s+process\s*\([^)]*\)\s*->\s*[^:]+:\s*/);
  if (defMatch) {
    processBody = processBody.substring(defMatch[0].length);
  }

  // Dedent the process body - find minimum indentation and remove it
  const lines = processBody.split('\n');
  const nonEmptyLines = lines.filter(line => line.trim().length > 0);
  if (nonEmptyLines.length > 0) {
    const minIndent = Math.min(...nonEmptyLines.map(line => {
      const match = line.match(/^(\s*)/);
      return match ? match[1].length : 0;
    }));
    processBody = lines.map(line => line.substring(minIndent)).join('\n');
  }

  // Generate node class
  const nodeCode = `

class ${nodeName}(CustomNodeBase):
    node_name: str = "${nodeMetadata.node_name}"
    node_category: str = "${nodeMetadata.node_category}"
    title: str = "${nodeMetadata.title || nodeMetadata.node_name}"
    intro: str = "${nodeMetadata.intro || "A custom node for data processing"}"
    number_of_inputs: int = ${nodeMetadata.number_of_inputs}
    number_of_outputs: int = ${nodeMetadata.number_of_outputs}
    settings_schema: ${nodeSettingsName} = ${nodeSettingsName}()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
${processBody.split('\n').map(line => '        ' + line).join('\n')}
`;

  // Combine all parts
  const fullCode = `# Auto-generated custom node
# Generated by Node Designer

import polars as pl
from flowfile_core.flowfile.node_designer import (
    ${Array.from(imports).join(", ")}
)
${sectionsCode}${settingsCode}${nodeCode}`;

  return fullCode;
}

function previewCode() {
  generatedCode.value = generateCode();
  showPreviewModal.value = true;
}

function closePreview() {
  showPreviewModal.value = false;
}

function copyCode() {
  navigator.clipboard.writeText(generatedCode.value);
  alert("Code copied to clipboard!");
}

async function saveNode() {
  if (!canSave.value) return;

  const code = generateCode();
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
</script>

<style scoped>
.node-designer-container {
  padding: 1rem;
  height: 100%;
  display: flex;
  flex-direction: column;
  overflow: hidden;
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
  overflow: hidden;
}

.panel {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 8px;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.panel-header {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--border-color);
  flex-shrink: 0;
}

.panel-header h3 {
  margin: 0;
  font-size: 0.875rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-secondary);
}

.panel-content {
  padding: 1rem;
  overflow-y: auto;
  flex: 1;
}

/* Component Palette */
.component-palette .panel-content {
  padding: 0.5rem;
}

.component-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  margin-bottom: 0.25rem;
  background: var(--bg-secondary);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: grab;
  transition: all 0.2s;
}

.component-item:hover {
  background: var(--bg-hover);
  border-color: var(--primary-color);
}

.component-item i {
  width: 20px;
  text-align: center;
  color: var(--primary-color);
}

.component-item span {
  font-size: 0.8125rem;
}

/* Design Canvas */
.design-canvas .panel-content {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  padding-bottom: 2rem;
}

.metadata-section h4 {
  margin: 0 0 0.75rem 0;
  font-size: 0.9375rem;
  font-weight: 600;
}

.sections-header h4 {
  margin: 0;
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

.form-checkbox {
  width: 18px;
  height: 18px;
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

.sections-header .btn {
  flex-shrink: 0;
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

.btn-sm {
  padding: 0.375rem 0.75rem;
  font-size: 0.8125rem;
}

.section-card {
  border: 1px solid var(--border-color);
  border-radius: 6px;
  margin-bottom: 0.75rem;
  background: var(--bg-secondary);
}

.section-card.selected {
  border-color: var(--primary-color);
  box-shadow: 0 0 0 2px rgba(var(--primary-rgb), 0.1);
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  padding: 0.75rem;
  border-bottom: 1px solid var(--border-color);
  gap: 0.5rem;
}

.section-fields {
  display: flex;
  gap: 0.75rem;
  flex: 1;
}

.section-field {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  flex: 1;
}

.section-field label {
  font-size: 0.6875rem;
  font-weight: 500;
  color: var(--text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.02em;
}

.section-name-input,
.section-title-input {
  padding: 0.375rem 0.5rem;
  border: 1px solid var(--border-color);
  border-radius: 4px;
  background: var(--input-bg, #fff);
  font-size: 0.8125rem;
  color: var(--text-primary);
  width: 100%;
}

.section-name-input {
  font-family: 'Fira Code', 'Monaco', monospace;
  font-size: 0.75rem;
}

.section-title-input {
  font-weight: 500;
}

.section-name-input:focus,
.section-title-input:focus {
  outline: none;
  border-color: var(--primary-color, #4a6cf7);
  box-shadow: 0 0 0 2px rgba(74, 108, 247, 0.1);
}

.btn-icon {
  background: transparent;
  border: none;
  color: var(--text-secondary);
  cursor: pointer;
  padding: 0.25rem;
  border-radius: 4px;
}

.btn-icon:hover {
  background: var(--bg-hover);
  color: var(--danger-color);
}

.section-components {
  padding: 0.5rem;
  min-height: 60px;
}

.component-card {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.5rem 0.75rem;
  margin-bottom: 0.25rem;
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 4px;
  cursor: pointer;
}

.component-card:hover {
  border-color: var(--primary-color);
}

.component-card.selected {
  border-color: var(--primary-color);
  background: rgba(var(--primary-rgb), 0.05);
}

.component-preview {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.component-preview i {
  color: var(--primary-color);
}

.component-label {
  font-weight: 500;
}

.component-type {
  font-size: 0.75rem;
  color: var(--text-secondary);
}

.btn-remove {
  opacity: 0;
  transition: opacity 0.2s;
}

.component-card:hover .btn-remove {
  opacity: 1;
}

.drop-zone {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 1rem;
  border: 2px dashed var(--border-color);
  border-radius: 4px;
  color: var(--text-secondary);
  font-size: 0.8125rem;
}

.drop-zone i {
  margin-bottom: 0.25rem;
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

/* Code Editor */
.code-editor-section {
  margin-top: 1rem;
  padding: 1.25rem;
  background: var(--bg-secondary, #f8f9fa);
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 8px;
}

.code-editor-section h4 {
  margin: 0 0 0.5rem 0;
  font-size: 1rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.code-hint {
  margin: 0 0 1rem 0;
  font-size: 0.8125rem;
  color: var(--text-secondary, #6c757d);
  line-height: 1.5;
}

.code-hint code {
  background: var(--card-bg, #ffffff);
  padding: 0.2rem 0.5rem;
  border-radius: 4px;
  font-size: 0.75rem;
  border: 1px solid var(--border-color, #e0e0e0);
  font-family: 'Fira Code', 'Monaco', monospace;
}

.code-editor-wrapper {
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 6px;
  overflow: hidden;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

/* Property Editor */
.property-form h4 {
  margin: 0 0 1rem 0;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid var(--border-color);
}

.property-form .form-field {
  margin-bottom: 0.75rem;
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

/* Modal */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-container {
  background: var(--card-bg);
  border-radius: 8px;
  max-width: 600px;
  width: 90%;
  max-height: 80vh;
  display: flex;
  flex-direction: column;
}

.modal-large {
  max-width: 900px;
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem;
  border-bottom: 1px solid var(--border-color);
}

.modal-title {
  margin: 0;
}

.modal-close {
  background: transparent;
  border: none;
  color: var(--text-secondary);
  cursor: pointer;
  font-size: 1.25rem;
}

.modal-content {
  padding: 1rem;
  overflow-y: auto;
  flex: 1;
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
  padding: 1rem;
  border-top: 1px solid var(--border-color);
}

.code-preview {
  background: #282c34;
  border-radius: 4px;
  overflow-x: auto;
}

.code-preview pre {
  margin: 0;
  padding: 1rem;
}

.code-preview code {
  font-family: 'Fira Code', 'Monaco', monospace;
  font-size: 0.8125rem;
  color: #abb2bf;
  white-space: pre;
}

/* Buttons */
.btn {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 4px;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
}

.btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-primary {
  background: var(--primary-color, #4a6cf7);
  color: white;
}

.btn-primary:hover:not(:disabled) {
  background: var(--primary-hover, #3d5bd9);
}

.btn-secondary {
  background: var(--bg-secondary);
  color: var(--text-primary);
  border: 1px solid var(--border-color);
}

.btn-secondary:hover:not(:disabled) {
  background: var(--bg-hover);
}
</style>
