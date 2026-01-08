<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container modal-large" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">
          <i class="fa-solid fa-circle-question"></i>
          Process Method Help
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <div class="help-section">
          <h4>Overview</h4>
          <p>
            The process method is where you write your data transformation logic. It receives input
            LazyFrames from connected nodes and returns a transformed LazyFrame.
          </p>
        </div>

        <div class="help-section">
          <h4>Method Signature</h4>
          <pre
            class="help-code"
          ><code>def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:</code></pre>
          <ul class="help-list">
            <li><code>inputs</code> - Tuple of input LazyFrames from connected nodes</li>
            <li><code>inputs[0]</code> - First input (most common)</li>
            <li><code>inputs[1]</code> - Second input (for joins, etc.)</li>
          </ul>
        </div>

        <div class="help-section">
          <h4>Accessing Settings</h4>
          <p>Access user-configured values from your UI components:</p>
          <pre
            class="help-code"
          ><code>self.settings_schema.section_name.component_name.value</code></pre>
          <p class="help-note">
            <i class="fa-solid fa-lightbulb"></i>
            Use autocomplete by typing <code>self.</code> to navigate the settings schema.
          </p>
        </div>

        <div class="help-section">
          <h4>Working with Secrets</h4>
          <p>For SecretSelector components, access the decrypted value:</p>
          <pre class="help-code"><code># Get the SecretStr object
secret = self.settings_schema.section.api_key.secret_value

# Get the actual decrypted string value
api_key = secret.get_secret_value()</code></pre>
        </div>

        <div class="help-section">
          <h4>Common Patterns</h4>
          <div class="pattern-grid">
            <div class="pattern-item">
              <h5>Filter Rows</h5>
              <pre class="help-code-small"><code>lf = inputs[0]
return lf.filter(pl.col("column") > 10)</code></pre>
            </div>
            <div class="pattern-item">
              <h5>Select Columns</h5>
              <pre class="help-code-small"><code>lf = inputs[0]
return lf.select(["col1", "col2"])</code></pre>
            </div>
            <div class="pattern-item">
              <h5>Add New Column</h5>
              <pre class="help-code-small"><code>lf = inputs[0]
return lf.with_columns(
    pl.col("a").alias("new_col")
)</code></pre>
            </div>
            <div class="pattern-item">
              <h5>Group & Aggregate</h5>
              <pre class="help-code-small"><code>lf = inputs[0]
return lf.group_by("category").agg(
    pl.col("value").sum()
)</code></pre>
            </div>
          </div>
        </div>

        <div class="help-section">
          <h4>Using Settings in Code</h4>
          <pre class="help-code"><code># Example: Using a TextInput value
column_name = self.settings_schema.options.column_name.value
lf = inputs[0]
return lf.select(pl.col(column_name))

# Example: Using a ColumnSelector value
selected_columns = self.settings_schema.columns.selected.value
return lf.select(selected_columns)</code></pre>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" @click="emit('close')">Close</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  show: boolean;
}>();

const emit = defineEmits<{
  (e: "close"): void;
}>();
</script>

<style scoped>
.help-section {
  margin-bottom: 1.5rem;
}

.help-section:last-child {
  margin-bottom: 0;
}

.help-section h4 {
  margin: 0 0 0.75rem 0;
  font-size: 1rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.help-section h5 {
  margin: 0 0 0.5rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--text-primary, #1a1a2e);
}

.help-section p {
  margin: 0 0 0.75rem 0;
  color: var(--text-secondary, #6c757d);
  line-height: 1.6;
  font-size: 0.875rem;
}

.help-list {
  margin: 0.5rem 0;
  padding-left: 1.5rem;
  color: var(--text-secondary, #6c757d);
  font-size: 0.875rem;
}

.help-list li {
  margin-bottom: 0.375rem;
  line-height: 1.5;
}

.help-list code {
  background: var(--bg-secondary, #f8f9fa);
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  font-family: var(--font-family-mono);
  font-size: 0.8125rem;
}

.help-code {
  display: block;
  background: var(--color-code-bg);
  padding: 0.75rem 1rem;
  border-radius: 6px;
  font-family: var(--font-family-mono);
  font-size: 0.8125rem;
  overflow-x: auto;
  margin: 0.5rem 0;
}

.help-code code {
  color: var(--color-code-text);
  white-space: pre;
}

.help-code-small {
  display: block;
  background: var(--color-code-bg);
  padding: 0.5rem 0.75rem;
  border-radius: 4px;
  font-family: var(--font-family-mono);
  font-size: 0.75rem;
  overflow-x: auto;
  margin: 0;
}

.help-code-small code {
  color: var(--color-code-text);
  white-space: pre;
}

.help-note {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  padding: 0.75rem;
  background: var(--color-accent-subtle, #ecfeff);
  border-radius: 6px;
  border-left: 3px solid var(--color-accent, #0891b2);
  margin-top: 0.75rem !important;
}

.help-note i {
  color: var(--color-accent, #0891b2);
  flex-shrink: 0;
  margin-top: 0.125rem;
}

.help-note code {
  background: var(--color-white-alpha-40);
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  font-family: var(--font-family-mono);
  font-size: 0.8125rem;
}

.pattern-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1rem;
  margin-top: 0.75rem;
}

.pattern-item {
  background: var(--bg-secondary, #f8f9fa);
  padding: 0.75rem;
  border-radius: 6px;
  border: 1px solid var(--border-color, #e0e0e0);
}

.shortcuts-grid {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.shortcut {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  font-size: 0.875rem;
  color: var(--text-secondary, #6c757d);
}

.shortcut kbd {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 2rem;
  padding: 0.25rem 0.5rem;
  background: var(--bg-secondary, #f8f9fa);
  border: 1px solid var(--border-color, #e0e0e0);
  border-radius: 4px;
  font-family: inherit;
  font-size: 0.75rem;
  font-weight: 500;
  box-shadow: var(--shadow-xs);
}

/* Dark mode adjustments */
[data-theme="dark"] .help-note {
  background: var(--color-focus-ring-accent);
}

[data-theme="dark"] .help-note code {
  background: var(--color-black-alpha-20);
}

[data-theme="dark"] .help-list code {
  background: var(--color-background-tertiary, #0f3460);
}
</style>
