<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container modal-xl" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">
          <i class="fa-solid fa-circle-question"></i>
          Node Designer Guide
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <!-- Navigation tabs -->
        <div class="help-tabs">
          <button
            v-for="tab in tabs"
            :key="tab.id"
            :class="['help-tab', { active: activeTab === tab.id }]"
            @click="activeTab = tab.id"
          >
            <i :class="tab.icon"></i>
            {{ tab.label }}
          </button>
        </div>

        <!-- Tab content -->
        <div class="help-tab-content">
          <!-- Overview Tab -->
          <div v-if="activeTab === 'overview'" class="tab-panel">
            <h4>What is the Node Designer?</h4>
            <p>
              The Node Designer allows you to create custom data transformation nodes without
              writing boilerplate code. Design your node's UI visually, then write only the
              transformation logic.
            </p>

            <div class="feature-grid">
              <div class="feature-card">
                <div class="feature-icon">
                  <i class="fa-solid fa-palette"></i>
                </div>
                <h5>Visual UI Design</h5>
                <p>Drag and drop components to create your node's settings interface</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">
                  <i class="fa-solid fa-code"></i>
                </div>
                <h5>Python Processing</h5>
                <p>Write Polars transformation code with full autocomplete support</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">
                  <i class="fa-solid fa-plug"></i>
                </div>
                <h5>Instant Integration</h5>
                <p>Your custom nodes appear immediately in the flow editor</p>
              </div>
            </div>

            <h4>Quick Start</h4>
            <ol class="steps-list">
              <li>
                <strong>Set metadata</strong> - Name your node and choose a category
              </li>
              <li>
                <strong>Add sections</strong> - Create UI sections to organize your settings
              </li>
              <li>
                <strong>Add components</strong> - Drag components from the palette into sections
              </li>
              <li>
                <strong>Configure properties</strong> - Select a component to edit its properties
              </li>
              <li>
                <strong>Write process code</strong> - Implement your transformation logic
              </li>
              <li><strong>Save</strong> - Your node is ready to use!</li>
            </ol>
          </div>

          <!-- Layout Tab -->
          <div v-if="activeTab === 'layout'" class="tab-panel">
            <h4>Page Layout</h4>
            <div class="layout-diagram">
              <div class="layout-box palette">
                <span class="layout-label">Component Palette</span>
                <p>Drag components from here</p>
              </div>
              <div class="layout-box canvas">
                <span class="layout-label">Design Canvas</span>
                <p>Build your node UI here</p>
              </div>
              <div class="layout-box properties">
                <span class="layout-label">Properties</span>
                <p>Edit selected component</p>
              </div>
            </div>

            <h4>Component Palette (Left)</h4>
            <p>
              Contains all available UI components. Drag them into a section on the Design Canvas
              to add them to your node.
            </p>

            <h4>Design Canvas (Center)</h4>
            <ul class="help-list">
              <li>
                <strong>Node Metadata</strong> - Set your node's name, category, title,
                description, and number of inputs/outputs
              </li>
              <li>
                <strong>UI Sections</strong> - Organize your components into collapsible sections
              </li>
              <li>
                <strong>Process Method</strong> - Write your Python transformation code
              </li>
            </ul>

            <h4>Property Editor (Right)</h4>
            <p>
              When you select a component, its properties appear here. Configure labels, defaults,
              validation rules, and more.
            </p>
          </div>

          <!-- Components Tab -->
          <div v-if="activeTab === 'components'" class="tab-panel">
            <h4>Available Components</h4>

            <div class="component-list">
              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-font"></i>
                </div>
                <div class="component-info">
                  <h5>Text Input</h5>
                  <p>Single-line text entry for strings, names, or patterns</p>
                  <code>value: str</code>
                </div>
              </div>

              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-hashtag"></i>
                </div>
                <div class="component-info">
                  <h5>Numeric Input</h5>
                  <p>Number entry with optional min/max validation</p>
                  <code>value: int | float</code>
                </div>
              </div>

              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-toggle-on"></i>
                </div>
                <div class="component-info">
                  <h5>Toggle Switch</h5>
                  <p>Boolean on/off switch for feature flags</p>
                  <code>value: bool</code>
                </div>
              </div>

              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-list"></i>
                </div>
                <div class="component-info">
                  <h5>Single Select</h5>
                  <p>Dropdown to select one option from a list</p>
                  <code>value: str</code>
                </div>
              </div>

              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-list-check"></i>
                </div>
                <div class="component-info">
                  <h5>Multi Select</h5>
                  <p>Select multiple options from a list</p>
                  <code>value: list[str]</code>
                </div>
              </div>

              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-table-columns"></i>
                </div>
                <div class="component-info">
                  <h5>Column Selector</h5>
                  <p>Select columns from input data (single or multiple)</p>
                  <code>value: str | list[str]</code>
                </div>
              </div>

              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-sliders"></i>
                </div>
                <div class="component-info">
                  <h5>Slider</h5>
                  <p>Numeric slider with min/max/step</p>
                  <code>value: int | float</code>
                </div>
              </div>

              <div class="component-item">
                <div class="component-icon">
                  <i class="fa-solid fa-key"></i>
                </div>
                <div class="component-info">
                  <h5>Secret Selector</h5>
                  <p>Securely access stored secrets (API keys, passwords)</p>
                  <code>secret_value: SecretStr</code>
                </div>
              </div>
            </div>
          </div>

          <!-- Code Tab -->
          <div v-if="activeTab === 'code'" class="tab-panel">
            <h4>Process Method</h4>
            <p>The process method receives input data and returns transformed output:</p>
            <pre
              class="help-code"
            ><code>def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
    lf = inputs[0]  # First input
    # Your transformation logic here
    return lf</code></pre>

            <h4>Accessing Settings</h4>
            <p>Access user-configured values from your UI components:</p>
            <pre
              class="help-code"
            ><code># Pattern: self.settings_schema.section_name.component_name.value
column = self.settings_schema.options.column_name.value
threshold = self.settings_schema.filters.min_value.value</code></pre>

            <h4>Working with Secrets</h4>
            <pre class="help-code"><code># Get SecretStr, then extract the actual value
api_key: SecretStr = self.settings_schema.auth.api_key.secret_value
actual_key = api_key.get_secret_value()</code></pre>

            <h4>Keyboard Shortcuts</h4>
            <div class="shortcuts-grid">
              <div class="shortcut">
                <kbd>Tab</kbd>
                <span>Accept autocomplete / Indent</span>
              </div>
              <div class="shortcut">
                <kbd>Shift</kbd>+<kbd>Tab</kbd>
                <span>Outdent selected lines</span>
              </div>
              <div class="shortcut">
                <kbd>Arrow Up/Down</kbd>
                <span>Navigate autocomplete suggestions</span>
              </div>
              <div class="shortcut">
                <kbd>Escape</kbd>
                <span>Close autocomplete menu</span>
              </div>
              <div class="shortcut">
                <kbd>Shift</kbd>+<kbd>Arrow</kbd>
                <span>Extend selection</span>
              </div>
              <div class="shortcut">
                <kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>Arrow</kbd>
                <span>Select by word</span>
              </div>
            </div>
          </div>

          <!-- Tips Tab -->
          <div v-if="activeTab === 'tips'" class="tab-panel">
            <h4>Best Practices</h4>

            <div class="tip-card">
              <div class="tip-icon success">
                <i class="fa-solid fa-check"></i>
              </div>
              <div class="tip-content">
                <h5>Use descriptive names</h5>
                <p>
                  Choose clear variable names in sections (e.g., "filters", "options") and
                  components (e.g., "column_name", "threshold")
                </p>
              </div>
            </div>

            <div class="tip-card">
              <div class="tip-icon success">
                <i class="fa-solid fa-check"></i>
              </div>
              <div class="tip-content">
                <h5>Group related settings</h5>
                <p>
                  Use sections to organize related components together. This creates a better user
                  experience.
                </p>
              </div>
            </div>

            <div class="tip-card">
              <div class="tip-icon success">
                <i class="fa-solid fa-check"></i>
              </div>
              <div class="tip-content">
                <h5>Use Column Selector for dynamic columns</h5>
                <p>
                  Instead of hardcoding column names, use ColumnSelector to let users pick columns
                  from their data.
                </p>
              </div>
            </div>

            <div class="tip-card">
              <div class="tip-icon warning">
                <i class="fa-solid fa-exclamation"></i>
              </div>
              <div class="tip-content">
                <h5>Keep process code simple</h5>
                <p>
                  Focus on transformation logic. Complex operations should be broken into multiple
                  nodes.
                </p>
              </div>
            </div>

            <div class="tip-card">
              <div class="tip-icon warning">
                <i class="fa-solid fa-exclamation"></i>
              </div>
              <div class="tip-content">
                <h5>Use secrets for sensitive data</h5>
                <p>
                  Never hardcode API keys or passwords. Use the Secret Selector component for
                  secure credential access.
                </p>
              </div>
            </div>

            <h4>Common Patterns</h4>
            <pre class="help-code"><code># Filter rows
return lf.filter(pl.col(column) > threshold)

# Select and rename
return lf.select([
    pl.col(old_name).alias(new_name)
])

# Add computed column
return lf.with_columns(
    (pl.col("a") + pl.col("b")).alias("sum")
)

# Group and aggregate
return lf.group_by(group_col).agg(
    pl.col(value_col).sum().alias("total")
)</code></pre>
          </div>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" @click="emit('close')">Close</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";

defineProps<{
  show: boolean;
}>();

const emit = defineEmits<{
  (e: "close"): void;
}>();

const activeTab = ref("overview");

const tabs = [
  { id: "overview", label: "Overview", icon: "fa-solid fa-house" },
  { id: "layout", label: "Layout", icon: "fa-solid fa-table-columns" },
  { id: "components", label: "Components", icon: "fa-solid fa-puzzle-piece" },
  { id: "code", label: "Code", icon: "fa-solid fa-code" },
  { id: "tips", label: "Tips", icon: "fa-solid fa-lightbulb" },
];
</script>

<style scoped>
.modal-xl {
  max-width: 900px;
}

/* Tabs */
.help-tabs {
  display: flex;
  gap: 0.25rem;
  border-bottom: 1px solid var(--border-color, #e0e0e0);
  margin-bottom: 1.5rem;
  padding-bottom: 0;
}

.help-tab {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.75rem 1rem;
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  color: var(--text-secondary, #6c757d);
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.15s ease;
  margin-bottom: -1px;
}

.help-tab:hover {
  color: var(--text-primary, #1a1a2e);
  background: var(--bg-secondary, #f8f9fa);
}

.help-tab.active {
  color: var(--color-accent, #0891b2);
  border-bottom-color: var(--color-accent, #0891b2);
}

.help-tab i {
  font-size: 0.875rem;
}

/* Tab content */
.tab-panel h4 {
  margin: 0 0 0.75rem 0;
  font-size: 1rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.tab-panel h4:not(:first-child) {
  margin-top: 1.5rem;
}

.tab-panel p {
  margin: 0 0 1rem 0;
  color: var(--text-secondary, #6c757d);
  line-height: 1.6;
  font-size: 0.875rem;
}

/* Feature grid */
.feature-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.feature-card {
  padding: 1rem;
  background: var(--bg-secondary, #f8f9fa);
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 8px;
  text-align: center;
}

.feature-icon {
  width: 3rem;
  height: 3rem;
  margin: 0 auto 0.75rem;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-accent-subtle, #ecfeff);
  border-radius: 50%;
  color: var(--color-accent, #0891b2);
  font-size: 1.25rem;
}

.feature-card h5 {
  margin: 0 0 0.5rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.feature-card p {
  margin: 0;
  font-size: 0.8125rem;
  color: var(--text-secondary, #6c757d);
}

/* Steps list */
.steps-list {
  margin: 0;
  padding-left: 1.5rem;
  color: var(--text-secondary, #6c757d);
  font-size: 0.875rem;
}

.steps-list li {
  margin-bottom: 0.5rem;
  line-height: 1.6;
}

.steps-list strong {
  color: var(--text-primary, #1a1a2e);
}

/* Layout diagram */
.layout-diagram {
  display: grid;
  grid-template-columns: 150px 1fr 200px;
  gap: 0.5rem;
  margin-bottom: 1.5rem;
}

.layout-box {
  padding: 1rem;
  border: 2px dashed var(--border-color, #e0e0e0);
  border-radius: 8px;
  text-align: center;
}

.layout-box.palette {
  background: rgba(99, 102, 241, 0.05);
  border-color: rgba(99, 102, 241, 0.3);
}

.layout-box.canvas {
  background: rgba(8, 145, 178, 0.05);
  border-color: rgba(8, 145, 178, 0.3);
}

.layout-box.properties {
  background: rgba(16, 185, 129, 0.05);
  border-color: rgba(16, 185, 129, 0.3);
}

.layout-label {
  display: block;
  font-weight: 600;
  font-size: 0.8125rem;
  margin-bottom: 0.25rem;
  color: var(--text-primary, #1a1a2e);
}

.layout-box p {
  margin: 0;
  font-size: 0.75rem;
}

/* Help list */
.help-list {
  margin: 0 0 1rem 0;
  padding-left: 1.5rem;
  color: var(--text-secondary, #6c757d);
  font-size: 0.875rem;
}

.help-list li {
  margin-bottom: 0.5rem;
  line-height: 1.5;
}

.help-list strong {
  color: var(--text-primary, #1a1a2e);
}

/* Component list */
.component-list {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.component-item {
  display: flex;
  gap: 1rem;
  padding: 0.75rem;
  background: var(--bg-secondary, #f8f9fa);
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 6px;
}

.component-icon {
  width: 2.5rem;
  height: 2.5rem;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--card-bg, #ffffff);
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 6px;
  color: var(--color-accent, #0891b2);
  flex-shrink: 0;
}

.component-info {
  flex: 1;
  min-width: 0;
}

.component-info h5 {
  margin: 0 0 0.25rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.component-info p {
  margin: 0 0 0.25rem 0;
  font-size: 0.8125rem;
}

.component-info code {
  font-size: 0.75rem;
  color: var(--color-accent, #0891b2);
  font-family: "Fira Code", "Monaco", monospace;
}

/* Code blocks */
.help-code {
  display: block;
  background: #282c34;
  padding: 0.75rem 1rem;
  border-radius: 6px;
  font-family: "Fira Code", "Monaco", monospace;
  font-size: 0.8125rem;
  overflow-x: auto;
  margin: 0.5rem 0 1rem 0;
}

.help-code code {
  color: #abb2bf;
  white-space: pre;
}

/* Shortcuts grid */
.shortcuts-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 0.5rem;
}

.shortcut {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.8125rem;
  color: var(--text-secondary, #6c757d);
  padding: 0.5rem;
  background: var(--bg-secondary, #f8f9fa);
  border-radius: 4px;
}

.shortcut kbd {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 1.5rem;
  padding: 0.125rem 0.375rem;
  background: var(--card-bg, #ffffff);
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 3px;
  font-family: inherit;
  font-size: 0.6875rem;
  font-weight: 500;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
}

/* Tip cards */
.tip-card {
  display: flex;
  gap: 0.75rem;
  padding: 0.75rem;
  background: var(--bg-secondary, #f8f9fa);
  border-radius: 6px;
  margin-bottom: 0.75rem;
}

.tip-icon {
  width: 1.75rem;
  height: 1.75rem;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
  flex-shrink: 0;
  font-size: 0.75rem;
}

.tip-icon.success {
  background: var(--color-success-light, #d1fae5);
  color: var(--color-success, #10b981);
}

.tip-icon.warning {
  background: var(--color-warning-light, #fef3c7);
  color: var(--color-warning, #f59e0b);
}

.tip-content h5 {
  margin: 0 0 0.25rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.tip-content p {
  margin: 0;
  font-size: 0.8125rem;
}

/* Dark mode */
[data-theme="dark"] .layout-box.palette {
  background: rgba(99, 102, 241, 0.1);
}

[data-theme="dark"] .layout-box.canvas {
  background: rgba(8, 145, 178, 0.1);
}

[data-theme="dark"] .layout-box.properties {
  background: rgba(16, 185, 129, 0.1);
}
</style>
