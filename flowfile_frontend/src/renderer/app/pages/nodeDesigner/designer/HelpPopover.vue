<template>
  <el-popover placement="bottom-end" :width="380" trigger="click">
    <template #reference>
      <button class="btn btn-secondary">
        <i class="fa-solid fa-circle-question"></i>
        Help
      </button>
    </template>

    <div class="help-popover">
      <h4>Quick reference</h4>
      <p class="help-hint">
        Design the UI with sections and components, then read their values in
        <code>process</code>:
      </p>
      <pre class="help-code">self.settings_schema.&lt;section&gt;.&lt;field&gt;.value</pre>
      <p class="help-hint">
        Secrets use <code>.secret_value</code> instead of <code>.value</code>. Inputs arrive as
        Polars LazyFrames; return the transformed frame.
      </p>

      <h4>Components</h4>
      <ul class="help-components">
        <li v-for="comp in availableComponents" :key="comp.type">
          <i :class="comp.icon"></i>
          <span class="comp-type">{{ comp.type }}</span>
          <span class="comp-label">{{ comp.label }}</span>
        </li>
      </ul>

      <a class="help-docs-link" :href="docsUrl" target="_blank" rel="noopener">
        <i class="fa-solid fa-book"></i>
        Full guide: creating custom nodes
      </a>
    </div>
  </el-popover>
</template>

<script setup lang="ts">
import { availableComponents } from "../constants";

const docsUrl = "https://edwardvaneechoud.github.io/Flowfile/for-developers/creating-custom-nodes/";
</script>

<style scoped>
.help-popover h4 {
  margin: 0 0 0.5rem 0;
  font-size: 0.8125rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  color: var(--color-text-secondary, #6b7280);
}

.help-popover h4 + .help-hint {
  margin-top: 0;
}

.help-hint {
  margin: 0 0 0.5rem 0;
  font-size: 0.8125rem;
  line-height: 1.5;
  color: var(--color-text-primary, #374151);
}

.help-hint code,
.help-code {
  font-family: "Fira Code", "Monaco", monospace;
  font-size: 0.75rem;
  background: var(--color-background-tertiary, #f1f3f5);
  border-radius: 4px;
}

.help-hint code {
  padding: 0.1rem 0.3rem;
}

.help-code {
  margin: 0 0 0.75rem 0;
  padding: 0.5rem 0.75rem;
  overflow-x: auto;
}

.help-components {
  list-style: none;
  margin: 0 0 0.75rem 0;
  padding: 0;
  max-height: 220px;
  overflow-y: auto;
}

.help-components li {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.25rem 0;
  font-size: 0.8125rem;
}

.help-components i {
  width: 18px;
  text-align: center;
  color: var(--color-accent, #0891b2);
}

.comp-type {
  font-family: "Fira Code", "Monaco", monospace;
  font-size: 0.75rem;
}

.comp-label {
  color: var(--color-text-secondary, #6b7280);
}

.help-docs-link {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  font-size: 0.8125rem;
  color: var(--color-accent, #0891b2);
  text-decoration: none;
}

.help-docs-link:hover {
  text-decoration: underline;
}
</style>
