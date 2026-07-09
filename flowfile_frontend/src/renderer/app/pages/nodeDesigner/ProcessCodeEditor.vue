<template>
  <div class="code-editor-section">
    <div class="code-editor-header">
      <h4>Process Method</h4>
      <button class="btn btn-sm btn-secondary" title="Show help" @click="showHelp = true">
        <i class="fa-solid fa-circle-question"></i>
        <span>Help</span>
      </button>
    </div>
    <p class="code-hint">
      Write your data transformation logic. Access settings via
      <code>self.settings_schema.section_name.component_name.value</code>
    </p>
    <div v-if="multiOutputKeys" class="multi-output-hint">
      <i class="fa-solid fa-diagram-project"></i>
      <div class="multi-output-hint-body">
        <span>
          Return a <code>dict</code> with one LazyFrame per output — keys:
          <code v-for="(name, i) in multiOutputKeys" :key="i" class="key-chip">{{ name }}</code>
        </span>
        <button type="button" class="insert-return-btn" @click="insertReturnTemplate">
          <i class="fa-solid fa-plus"></i>
          Insert return template
        </button>
      </div>
    </div>
    <div class="code-editor-wrapper" :class="{ 'fill-height': fillHeight }">
      <div v-if="signature" class="signature-line">
        <span class="signature-gutter">
          <span v-for="n in signatureLineCount" :key="n" class="signature-gutter-num">{{ n }}</span>
        </span>
        <pre class="signature-code">{{ signature }}</pre>
      </div>
      <Codemirror
        :model-value="modelValue"
        placeholder="# Write your process logic here..."
        :style="fillHeight ? undefined : { height: height ?? '300px' }"
        :autofocus="false"
        :indent-with-tab="false"
        :tab-size="4"
        :extensions="editorExtensions"
        @update:model-value="emit('update:modelValue', $event)"
      />
    </div>

    <ProcessCodeHelpModal :show="showHelp" @close="showHelp = false" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { Codemirror } from "vue-codemirror";
import type { Extension } from "@codemirror/state";
import { lineNumbers } from "@codemirror/view";
import ProcessCodeHelpModal from "./ProcessCodeHelpModal.vue";

const props = defineProps<{
  modelValue: string;
  extensions: Extension[];
  height?: string;
  signature?: string;
  outputNames?: string[];
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: string): void;
  (e: "insert", code: string): void;
}>();

const fillHeight = computed(() => props.height === "100%");
const showHelp = ref(false);

// Multi-output nodes must return a dict keyed by output name; surface the exact
// required keys, live, so the contract is visible even when the body was edited
// past the scaffold. Single-output nodes (and code-only mode) show nothing.
const multiOutputKeys = computed(() =>
  props.outputNames && props.outputNames.length > 1 ? props.outputNames : null,
);

// Insert a ready-to-edit `return {...}` keyed by the declared outputs, so authors
// see the exact dict shape their node must return. Reuses the editor's insert path.
function insertReturnTemplate() {
  const keys = multiOutputKeys.value;
  if (!keys) return;
  const entries = keys
    .map((n) => `        ${JSON.stringify(n)}: ...,  # replace ... with the output`)
    .join("\n");
  emit("insert", `    return {\n${entries}\n    }`);
}

// The read-only signature above the editor occupies line(s) 1..N, so number the
// editable body continuing from N+1 — making `def process(...)` visibly row 1.
// (No offset in code-only mode, where the full file — signature included — is
// in the editor and numbers naturally.)
const signatureLineCount = computed(() =>
  props.signature ? props.signature.split("\n").length : 0,
);

const editorExtensions = computed<Extension[]>(() => {
  const offset = signatureLineCount.value;
  if (!offset) return props.extensions;
  return [...props.extensions, lineNumbers({ formatNumber: (n) => String(n + offset) })];
});
</script>

<style scoped>
.code-editor-section {
  margin-top: 1rem;
  margin-bottom: 1.5rem;
  padding: 1.25rem;
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-lg);
}

.code-editor-section:has(.fill-height) {
  display: flex;
  flex-direction: column;
  height: 100%;
  margin-top: 0;
  margin-bottom: 0;
  box-sizing: border-box;
}

.code-editor-wrapper.fill-height {
  flex: 1;
  min-height: 0;
}

.code-editor-wrapper.fill-height :deep(.cm-editor) {
  flex: 1;
  min-height: 0;
}

.signature-line {
  display: flex;
  align-items: stretch;
  flex-shrink: 0;
  /* Faint accent wash + the left rail below mark this as the block-opening
     header of the function; the indented editor beneath reads as its body. */
  background: var(--color-focus-ring-accent, rgba(8, 145, 178, 0.06));
  border-bottom: 1px solid var(--color-border-light, #e5e7eb);
}

/* Mirror CodeMirror's line-number gutter so the signature's "1" lines up with
   the body's 2, 3, … into one continuous number column. */
.signature-gutter {
  display: flex;
  flex-direction: column;
  flex-shrink: 0;
  padding: 0.45rem 0;
  background: var(--color-background-primary, #fff);
  border-right: 1px solid var(--color-border-primary, #d1d5db);
  color: var(--color-text-muted, #9ca3af);
  font-family: "Fira Code", "Monaco", monospace;
  font-size: 0.8125rem;
  line-height: 1.4;
}

/* One continuous number column: the signature "1" and the editor's 2, 3, … get
   the same fixed width (fits 3-digit numbers), so they align regardless of font
   or digit count — CodeMirror otherwise sizes its gutter off a hidden spacer. */
.signature-gutter-num,
.code-editor-wrapper :deep(.cm-lineNumbers .cm-gutterElement) {
  box-sizing: border-box;
  width: 2.75rem;
  padding: 0 6px 0 0;
  text-align: right;
}

/* Same font as the signature header, so gutter digits and body code render
   identically to the def line above. */
.code-editor-wrapper :deep(.cm-editor) {
  font-family: "Fira Code", "Monaco", monospace;
  font-size: 0.8125rem;
}

/* Short process bodies don't need code folding; dropping the fold gutter keeps
   the numbers and the code as one clean, aligned two-column layout.
   !important needed: CM's base theme sets .cm-gutter { display: flex !important }. */
.code-editor-wrapper :deep(.cm-foldGutter) {
  display: none !important;
}

.signature-code {
  flex: 1;
  min-width: 0;
  margin: 0;
  /* Left inset matches CodeMirror's .cm-line so `def process(...)` aligns with the body. */
  padding: 0.45rem 0.75rem 0.45rem 6px;
  overflow-x: auto;
  white-space: pre;
  color: var(--color-text-secondary, #6c757d);
  font-family: "Fira Code", "Monaco", monospace;
  font-size: 0.8125rem;
  font-weight: 500;
  line-height: 1.4;
}

.code-editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.5rem;
}

.code-editor-header h4 {
  margin: 0;
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.code-hint {
  margin: 0 0 1rem 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.code-hint code {
  background: var(--color-background-primary);
  padding: 0.2rem 0.5rem;
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
  border: 1px solid var(--color-border-primary);
  font-family: "Fira Code", "Monaco", monospace;
}

/* Multi-output contract callout: same accent-wash + left-rail idiom as the
   help modal's .help-note, so guidance reads consistently across the designer. */
.multi-output-hint {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  margin: 0 0 1rem 0;
  padding: 0.5rem 0.75rem;
  background: var(--color-accent-subtle, #ecfeff);
  border-left: 3px solid var(--color-accent, #0891b2);
  border-radius: var(--border-radius-sm, 4px);
  font-size: var(--font-size-sm);
  line-height: 1.5;
  color: var(--color-text-secondary);
}

.multi-output-hint > i {
  color: var(--color-accent, #0891b2);
  flex-shrink: 0;
  margin-top: 0.15rem;
}

.multi-output-hint-body {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.4rem;
}

.insert-return-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.3rem;
  padding: 0.2rem 0.55rem;
  border: 1px solid var(--color-accent, #0891b2);
  border-radius: var(--border-radius-sm, 4px);
  background: transparent;
  color: var(--color-accent, #0891b2);
  font-size: var(--font-size-xs);
  font-weight: 500;
  cursor: pointer;
}

.insert-return-btn:hover {
  background: var(--color-accent, #0891b2);
  color: #fff;
}

.multi-output-hint code {
  background: var(--color-background-primary);
  padding: 0.1rem 0.35rem;
  border-radius: var(--border-radius-sm, 3px);
  border: 1px solid var(--color-border-primary);
  font-family: "Fira Code", "Monaco", monospace;
  font-size: var(--font-size-xs);
}

.multi-output-hint .key-chip {
  margin: 0 0.15rem;
  color: var(--color-accent, #0891b2);
  font-weight: 500;
}

.code-editor-wrapper {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--color-border-primary);
  /* Accent rail down the left brackets the signature + body as one scope, so
     it's obvious the code you write lives inside def process(...). */
  border-left: 3px solid var(--color-accent, #0891b2);
  border-radius: 6px;
  overflow: hidden;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

/* Subtle indent guide down the body: a thin vertical line sitting in the
   function-body indentation, reinforcing that each line hangs inside the def. */
.code-editor-wrapper :deep(.cm-line) {
  background-image: linear-gradient(
    to right,
    transparent calc(2ch - 1px),
    var(--color-border-light, #e5e7eb) calc(2ch - 1px),
    var(--color-border-light, #e5e7eb) 2ch,
    transparent 2ch
  );
  background-repeat: no-repeat;
}
</style>
