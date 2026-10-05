<template>
  <CollapsibleSection
    title="Python code"
    icon="fa-brands fa-python"
    persist-key="flow.code"
    :default-open="true"
    :summary="summary"
  >
    <template #actions>
      <div class="mode-toggle" role="group" aria-label="Code dialect">
        <button
          :class="['mode-segment', { active: dialect === 'flowframe' }]"
          :aria-pressed="dialect === 'flowframe'"
          data-testid="flow-code-dialect-flowframe"
          @click="setDialect('flowframe')"
        >
          FlowFrame
        </button>
        <button
          :class="['mode-segment', { active: dialect === 'polars' }]"
          :aria-pressed="dialect === 'polars'"
          data-testid="flow-code-dialect-polars"
          @click="setDialect('polars')"
        >
          Polars
        </button>
      </div>
      <button
        class="btn btn-secondary btn-sm"
        :disabled="!code"
        data-testid="flow-code-copy"
        title="Copy the code to the clipboard"
        @click="copyCode"
      >
        <i class="fa-regular fa-copy"></i>
        Copy
      </button>
      <button
        class="btn btn-primary btn-sm"
        :disabled="!fileExists"
        data-testid="flow-code-modify"
        title="Open this flow in the designer with the notebook, where edits sync back to the canvas"
        @click="$emit('modifyInNotebook')"
      >
        <i class="fa-solid fa-book"></i>
        Modify in notebook
      </button>
    </template>

    <p v-if="!fileExists" class="flow-code-note">
      The flow file is missing, so there is no code to show.
    </p>
    <p v-else-if="loading && !code" class="flow-code-note">
      <i class="fa-solid fa-spinner fa-spin"></i> Generating code…
    </p>
    <p v-else-if="error" class="flow-code-note is-error" data-testid="flow-code-error">
      This flow cannot be shown as code: {{ error }}
    </p>
    <div v-else class="flow-code-editor" :class="{ 'is-stale': loading }" data-testid="flow-code">
      <codemirror :model-value="code ?? ''" :extensions="extensions" :disabled="true" />
    </div>
  </CollapsibleSection>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { Codemirror } from "vue-codemirror";
import { python } from "@codemirror/lang-python";
import { EditorView } from "@codemirror/view";
import { flowfileEditorTheme } from "@/utils/codemirrorTheme";
import { CatalogApi } from "../../api/catalog.api";
import { CollapsibleSection } from "../../components/common";
import { copyTextEverywhere } from "../../utils/clipboardUtils";
import type { FlowCodeDialect } from "../../types";

const props = defineProps<{
  registrationId: number;
  fileExists: boolean;
}>();

defineEmits<{ (e: "modifyInNotebook"): void }>();

const extensions = [
  python(),
  flowfileEditorTheme(),
  EditorView.theme({ ".cm-content": { padding: "8px 0" } }),
];

const dialect = ref<FlowCodeDialect>("flowframe");
const code = ref<string | null>(null);
const error = ref<string | null>(null);
const loading = ref(false);
let requestSeq = 0;

const summary = computed(() => {
  if (!props.fileExists || !code.value) return undefined;
  return `${code.value.split("\n").length} lines`;
});

async function load() {
  const seq = ++requestSeq;
  if (!props.fileExists) {
    code.value = null;
    error.value = null;
    return;
  }
  loading.value = true;
  try {
    const result = await CatalogApi.getFlowCode(props.registrationId, dialect.value);
    if (seq !== requestSeq) return;
    code.value = result.code;
    error.value = result.error;
  } catch (e: any) {
    if (seq !== requestSeq) return;
    code.value = null;
    error.value = e?.response?.data?.detail ?? e?.message ?? "the request failed";
  } finally {
    if (seq === requestSeq) loading.value = false;
  }
}

function setDialect(next: FlowCodeDialect) {
  if (dialect.value === next) return;
  dialect.value = next;
  void load();
}

async function copyCode() {
  if (!code.value) return;
  if (await copyTextEverywhere(code.value)) {
    ElMessage.success("Copied the code");
  } else {
    ElMessage.error("Couldn't copy to the clipboard.");
  }
}

watch(
  () => [props.registrationId, props.fileExists],
  () => {
    code.value = null;
    error.value = null;
    void load();
  },
  { immediate: true },
);
</script>

<style scoped>
.flow-code-note {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.flow-code-note.is-error {
  color: var(--color-danger);
}

.flow-code-editor {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
  transition: opacity var(--transition-fast);
}

.flow-code-editor.is-stale {
  opacity: 0.6;
}

.flow-code-editor :deep(.cm-editor) {
  max-height: 480px;
}

.flow-code-editor :deep(.cm-scroller) {
  overflow: auto;
}

/* Same raised-segment control as the designer's code pane. */
.mode-toggle {
  display: flex;
  flex: none;
  gap: 2px;
  padding: 2px;
  margin-right: var(--spacing-2);
  background: var(--color-background-tertiary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}

.mode-segment {
  height: 24px;
  padding: 0 10px;
  border: none;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-family: inherit;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  white-space: nowrap;
  transition:
    background-color var(--transition-fast),
    color var(--transition-fast),
    box-shadow var(--transition-fast);
}

.mode-segment.active {
  background: var(--color-background-primary);
  color: var(--color-primary);
  box-shadow: var(--shadow-xs);
}

[data-theme="dark"] .mode-segment.active {
  background: color-mix(in srgb, var(--color-background-tertiary) 78%, white);
  color: var(--color-accent-dark);
}

.mode-segment:not(.active):hover {
  color: var(--color-text-primary);
}
</style>
