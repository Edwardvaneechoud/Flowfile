<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container modal-large" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">
          <i class="fa-solid fa-shapes"></i>
          Component Reference
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <section class="cr-quickref">
          <div class="cr-field-title">Quick reference</div>
          <p class="cr-lead">
            Design the settings UI with sections and components, then read their values inside
            <code>process</code>:
          </p>
          <pre
            class="help-code"
          ><code>self.settings_schema.&lt;section&gt;.&lt;field&gt;.value</code></pre>
          <p class="cr-lead">
            Secrets use <code>.secret_value</code> instead of <code>.value</code>. Inputs arrive as
            Polars LazyFrames; return the transformed frame.
          </p>
        </section>

        <div class="cr-section-label">
          <span>Components</span>
          <span class="cr-section-hint">{{ componentDocs.length }} · click to expand</span>
        </div>

        <CollapsibleSection
          v-for="comp in componentDocs"
          :key="comp.type"
          class="cr-entry"
          :icon="comp.icon"
          :title="comp.type"
          :summary="returnsChip(comp.returns)"
          :default-open="false"
          :persist-key="`nodeDesigner.componentRef.${comp.type}`"
        >
          <div class="cr-body">
            <div class="cr-meta">
              <span class="cr-label">{{ comp.label }}</span>
              <span
                class="cr-badge"
                :class="comp.needsInput ? 'cr-badge--input' : 'cr-badge--muted'"
              >
                <i
                  :class="comp.needsInput ? 'fa-solid fa-plug' : 'fa-solid fa-plug-circle-xmark'"
                ></i>
                {{ comp.needsInput ? "Needs a connected input" : "No input required" }}
              </span>
            </div>

            <p class="cr-what">{{ comp.whatItDoes }}</p>

            <div class="cr-field">
              <div class="cr-field-title">Returns</div>
              <pre class="help-code"><code>{{ returnsChip(comp.returns) }}</code></pre>
              <pre
                v-if="comp.returns.shape"
                class="help-code"
              ><code>{{ comp.returns.shape }}</code></pre>
            </div>

            <div class="cr-field">
              <div class="cr-field-title">Configure</div>
              <ul class="cr-config">
                <li v-for="p in comp.configure" :key="p.prop">
                  <code>{{ p.prop }}</code> — {{ p.desc }}
                </li>
              </ul>
            </div>

            <div class="cr-field">
              <div class="cr-field-title">How to use</div>
              <pre class="help-code"><code>{{ comp.howToUse }}</code></pre>
            </div>

            <div class="cr-field">
              <div class="cr-field-title">When to use</div>
              <p class="cr-when">{{ comp.whenToUse }}</p>
            </div>

            <div v-if="comp.note" class="help-note">
              <i class="fa-solid fa-lightbulb"></i>
              <span>{{ comp.note }}</span>
            </div>
          </div>
        </CollapsibleSection>
      </div>
      <div class="modal-actions">
        <a class="help-docs-link" :href="docsUrl" target="_blank" rel="noopener">
          <i class="fa-solid fa-book"></i>
          Full guide: creating custom nodes
        </a>
        <button class="btn btn-primary" @click="emit('close')">Close</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import CollapsibleSection from "../../components/common/CollapsibleSection/CollapsibleSection.vue";
import { componentDocs, returnsChip } from "./componentDocs";

defineProps<{
  show: boolean;
}>();

const emit = defineEmits<{
  (e: "close"): void;
}>();

const docsUrl = "https://edwardvaneechoud.github.io/Flowfile/for-developers/creating-custom-nodes/";
</script>

<style scoped>
.cr-quickref {
  margin-bottom: 1.25rem;
  padding-bottom: 1.25rem;
  border-bottom: 1px solid var(--color-border-primary, #e2e8f0);
}

.cr-lead {
  margin: 0.5rem 0;
  font-size: 0.875rem;
  line-height: 1.6;
  color: var(--color-text-secondary, #6c757d);
}

.cr-lead code {
  background: var(--color-background-tertiary, #f1f3f5);
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  font-family: var(--font-family-mono);
  font-size: 0.8125rem;
}

.cr-quickref .help-code {
  margin: 0.5rem 0;
}

.cr-section-label {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 0.5rem;
  margin-bottom: 0.25rem;
  font-size: 0.6875rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary, #6b7280);
}

.cr-section-hint {
  font-weight: 500;
  text-transform: none;
  letter-spacing: 0;
  color: var(--color-text-tertiary, #718096);
}

.cr-entry {
  padding: 0.75rem 0;
  border-bottom: 1px solid var(--color-border-light, #edf2f7);
}

.cr-entry:last-child {
  border-bottom: none;
}

/* The collapsed header shows the component class name + its return-type chip. */
.cr-entry :deep(.cs-title) {
  font-family: var(--font-family-mono);
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--color-text-primary, #1a1a2e);
}

.cr-entry :deep(.cs-summary) {
  font-family: var(--font-family-mono);
  font-size: 0.75rem;
  padding: 0.125rem 0.4rem;
  border-radius: 4px;
  background: var(--color-background-tertiary, #f1f3f5);
  color: var(--color-accent, #0891b2);
}

.cr-body {
  display: flex;
  flex-direction: column;
  gap: 0.875rem;
  padding-left: 1.5rem;
}

.cr-meta {
  display: flex;
  align-items: center;
  gap: 0.625rem;
  flex-wrap: wrap;
}

.cr-label {
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--color-text-primary, #1a1a2e);
}

.cr-badge {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  padding: 0.125rem 0.5rem;
  border-radius: var(--border-radius-full, 9999px);
  font-size: 0.6875rem;
  font-weight: 500;
}

.cr-badge--input {
  background: var(--color-accent-subtle, #ecfeff);
  color: var(--color-accent-hover, #0e7490);
}

.cr-badge--muted {
  background: var(--color-background-secondary, #f8f9fa);
  color: var(--color-text-tertiary, #718096);
}

.cr-what {
  margin: 0;
  font-size: 0.875rem;
  line-height: 1.6;
  color: var(--color-text-secondary, #6c757d);
}

.cr-field {
  display: flex;
  flex-direction: column;
  gap: 0.375rem;
}

.cr-field-title {
  font-size: 0.6875rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-tertiary, #718096);
}

.cr-config {
  margin: 0;
  padding-left: 1.25rem;
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.8125rem;
  line-height: 1.5;
  color: var(--color-text-secondary, #6c757d);
}

.cr-config code {
  background: var(--color-background-tertiary, #f1f3f5);
  padding: 0.0625rem 0.3rem;
  border-radius: 3px;
  font-family: var(--font-family-mono);
  font-size: 0.75rem;
  color: var(--color-text-primary, #1a1a2e);
}

.cr-when {
  margin: 0;
  font-size: 0.8125rem;
  line-height: 1.5;
  color: var(--color-text-secondary, #6c757d);
}

.help-code {
  display: block;
  background: var(--color-code-bg);
  padding: 0.75rem 1rem;
  border-radius: 6px;
  font-family: var(--font-family-mono);
  font-size: 0.8125rem;
  overflow-x: auto;
  margin: 0;
}

.help-code code {
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
  font-size: 0.8125rem;
  line-height: 1.5;
  color: var(--color-text-secondary, #6c757d);
}

.help-note i {
  color: var(--color-accent, #0891b2);
  flex-shrink: 0;
  margin-top: 0.125rem;
}

.modal-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 1rem;
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

/* Dark mode adjustments (mirrors ProcessCodeHelpModal). */
[data-theme="dark"] .help-note {
  background: var(--color-focus-ring-accent);
}

[data-theme="dark"] .cr-lead code,
[data-theme="dark"] .cr-config code,
[data-theme="dark"] .cr-entry :deep(.cs-summary) {
  background: var(--color-background-tertiary, #0f3460);
}
</style>
