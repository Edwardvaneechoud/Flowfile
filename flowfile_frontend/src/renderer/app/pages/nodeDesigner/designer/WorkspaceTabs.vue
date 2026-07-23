<template>
  <div class="panel workspace-panel">
    <el-tabs v-model="activeTab" class="workspace-tabs">
      <el-tab-pane label="Form" name="form">
        <div v-if="store.codeOnly" class="mode-banner">
          <i class="fa-solid fa-circle-info"></i>
          <div>
            <p class="mode-banner-title">Visual editing unavailable</p>
            <p class="mode-banner-body">
              This file is written outside the visual subset. Edit it in the
              <b>Code</b> tab, or click Re-check there once it fits the subset again.
            </p>
          </div>
        </div>
        <div v-else class="form-tab-layout">
          <div class="form-groups-column">
            <FormCanvas />
          </div>
          <ControlInspector
            v-if="store.selectedComponent"
            @insert-variable="handleInsertVariable"
          />
          <SectionInspector v-else />
        </div>
      </el-tab-pane>

      <el-tab-pane label="Code" name="code">
        <div class="code-tab-layout">
          <FormFieldsOverview v-if="!store.codeOnly" @insert="handleInsertVariable" />
          <div class="code-editor-column">
            <div v-if="store.codeOnly" class="code-only-banner">
              <div class="code-only-head">
                <i class="fa-solid fa-triangle-exclamation"></i>
                <span>Written outside the visual subset — showing the full file.</span>
                <button
                  class="btn btn-sm btn-secondary recheck-btn"
                  type="button"
                  :disabled="rechecking"
                  @click="recheck"
                >
                  <i v-if="rechecking" class="fa-solid fa-spinner fa-spin"></i>
                  Re-check
                </button>
              </div>
              <ul v-if="store.parseIssues.length" class="issue-list">
                <li v-for="(issue, i) in store.parseIssues" :key="i" :class="issue.severity">
                  {{ issue.message }}<span v-if="issue.line"> (line {{ issue.line }})</span>
                </li>
              </ul>
            </div>
            <div class="code-editor-wrap">
              <ProcessCodeEditor
                v-model="processCode"
                :extensions="extensions"
                :signature="store.codeOnly ? undefined : store.processSignature"
                :output-names="store.codeOnly ? undefined : store.nodeMetadata.output_names"
                height="100%"
                @insert="handleInsertVariable"
              />
            </div>
            <CollapsibleSection
              v-if="!store.codeOnly"
              title="Schema prediction (optional)"
              icon="fa-solid fa-table-columns"
              persist-key="nd-code-predict-schema"
              :default-open="store.predictSchemaEnabled"
              class="code-predict-dock"
            >
              <div v-if="!store.predictSchemaEnabled" class="predict-schema-empty">
                <p class="predict-schema-hint">
                  Declare the output schema so Flowfile can predict columns instantly, without
                  running the node (or starting a kernel). Mirrors <code>process</code> on
                  schema-only inputs — pure-polars nodes can just
                  <code>return self.process(*inputs)</code>.
                </p>
                <button
                  class="btn btn-sm btn-secondary"
                  type="button"
                  @click="store.predictSchemaEnabled = true"
                >
                  <i class="fa-solid fa-plus"></i>
                  Add schema prediction
                </button>
              </div>
              <div v-else class="predict-schema-editor">
                <ProcessCodeEditor
                  v-model="predictSchemaBody"
                  :extensions="extensions"
                  :signature="store.predictSchemaSignature"
                  title="Schema Prediction"
                  hint="Inputs mirror process: real upstream data once it has run, schema-only before. Return a frame with the output schema, or None to fall back to running the node."
                  hide-help
                  height="100%"
                  @insert="handleInsertPredictVariable"
                />
                <button
                  class="btn btn-sm btn-secondary predict-schema-remove"
                  type="button"
                  @click="store.predictSchemaEnabled = false"
                >
                  Remove schema prediction
                </button>
              </div>
            </CollapsibleSection>
            <CollapsibleSection
              title="Test"
              icon="fa-solid fa-flask"
              persist-key="nd-code-test"
              :default-open="false"
              class="code-test-dock"
            >
              <TestPanel />
            </CollapsibleSection>
          </div>
        </div>
      </el-tab-pane>

      <el-tab-pane label="Test" name="test">
        <TestPanel />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import type { Extension } from "@codemirror/state";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import ProcessCodeEditor from "../ProcessCodeEditor.vue";
import FormFieldsOverview from "./FormFieldsOverview.vue";
import FormCanvas from "./FormCanvas.vue";
import ControlInspector from "./ControlInspector.vue";
import SectionInspector from "./SectionInspector.vue";
import TestPanel from "./testPanel/TestPanel.vue";
import CollapsibleSection from "../../../components/common/CollapsibleSection/CollapsibleSection.vue";
import { recheckCode } from "../loadSave";

defineProps<{
  extensions: Extension[];
}>();

const store = useNodeDesignerStore();
const activeTab = ref("form");
const rechecking = ref(false);

// Designer mode edits only the body (the signature header above the editor is
// read-only and composed back into process_code by the store); code-only mode
// edits the whole file.
const processCode = computed({
  get: () => (store.codeOnly ? store.processCode : store.processBody),
  set: (value: string) => {
    if (store.codeOnly) store.processCode = value;
    else store.processBody = value;
  },
});

const predictSchemaBody = computed({
  get: () => store.predictSchemaBody,
  set: (value: string) => {
    store.predictSchemaBody = value;
  },
});

function handleInsertPredictVariable(code: string) {
  const lines = store.predictSchemaBody.split("\n");
  let insertIndex = 0;
  while (
    insertIndex < lines.length &&
    (lines[insertIndex].trim().startsWith("#") || lines[insertIndex].trim() === "")
  ) {
    insertIndex++;
  }
  lines.splice(insertIndex, 0, code);
  store.predictSchemaBody = lines.join("\n");
}

async function recheck() {
  rechecking.value = true;
  try {
    const flipped = await recheckCode();
    if (flipped) {
      ElMessage.success("This file now fits the visual editor — switched to Form view.");
      activeTab.value = "form";
    } else {
      ElMessage.info("Still outside the visual subset. See the listed issues.");
    }
  } catch {
    ElMessage.error("Re-check failed.");
  } finally {
    rechecking.value = false;
  }
}

function handleInsertVariable(code: string) {
  if (store.codeOnly) {
    const lines = store.processCode.split("\n");
    let insertIndex = 1;
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].trim().startsWith("def process")) {
        insertIndex = i + 1;
        while (
          insertIndex < lines.length &&
          (lines[insertIndex].trim().startsWith("#") || lines[insertIndex].trim() === "")
        ) {
          insertIndex++;
        }
        break;
      }
    }
    lines.splice(insertIndex, 0, code);
    store.processCode = lines.join("\n");
    return;
  }

  const lines = store.processBody.split("\n");
  let insertIndex = 0;
  while (
    insertIndex < lines.length &&
    (lines[insertIndex].trim().startsWith("#") || lines[insertIndex].trim() === "")
  ) {
    insertIndex++;
  }
  lines.splice(insertIndex, 0, code);
  store.processBody = lines.join("\n");
}
</script>

<style scoped>
.workspace-panel {
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.workspace-tabs {
  display: flex;
  flex-direction: column;
  flex: 1;
  min-height: 0;
  padding: 0 1rem;
}

.workspace-tabs :deep(.el-tabs__content) {
  flex: 1;
  min-height: 0;
}

/* Line the tab bar's content start up with the left panel's content (40px header
   + 16px gap), so both designer columns share the same top baseline. */
.workspace-tabs :deep(.el-tabs__header) {
  margin-bottom: var(--spacing-4);
}

.workspace-tabs :deep(.el-tabs__nav-wrap) {
  min-height: 40px;
}

.workspace-tabs :deep(.el-tab-pane) {
  height: 100%;
}

.form-tab-layout {
  display: grid;
  grid-template-columns: 1fr 300px;
  gap: 1rem;
  height: 100%;
  min-height: 0;
  padding-bottom: 0.75rem;
}

.form-groups-column {
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}

.code-tab-layout {
  display: grid;
  grid-template-columns: 260px 1fr;
  gap: 1rem;
  height: 100%;
  min-height: 0;
  padding-bottom: 0.75rem;
}

.code-editor-column {
  display: flex;
  flex-direction: column;
  min-height: 0;
  gap: 0.5rem;
}

.code-editor-wrap {
  flex: 1;
  min-height: 0;
}

/* Test dock: sits below the editor; the editor keeps the remaining height, and
   the dock body is capped with its own scroll so it reads like an IDE panel. */
.code-test-dock {
  flex-shrink: 0;
}

.code-predict-dock {
  flex-shrink: 0;
}

.code-predict-dock :deep(.cs-body) {
  margin-top: 0.5rem;
}

.predict-schema-empty {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.5rem;
}

.predict-schema-hint {
  margin: 0;
  font-size: 0.75rem;
  color: var(--color-text-secondary, #6b7280);
}

.predict-schema-editor {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.predict-schema-editor :deep(.cm-editor) {
  max-height: 220px;
}

.predict-schema-remove {
  align-self: flex-end;
}

.code-test-dock :deep(.cs-body) {
  height: 320px;
  overflow: hidden;
  margin-top: 0.5rem;
}

.mode-banner,
.code-only-banner {
  display: flex;
  gap: 0.625rem;
  padding: 0.75rem;
  border-radius: 6px;
  background: var(--color-background-secondary, #f3f4f6);
  border: 1px solid var(--color-border-primary, #e5e7eb);
  margin-bottom: 0.5rem;
}

.mode-banner {
  align-items: flex-start;
}

.code-only-banner {
  flex-direction: column;
  flex-shrink: 0;
}

.mode-banner i,
.code-only-head i {
  color: var(--color-warning, #f59e0b);
}

.mode-banner-title {
  margin: 0;
  font-weight: 600;
  font-size: 0.8125rem;
}

.mode-banner-body {
  margin: 0.25rem 0 0;
  font-size: 0.75rem;
  color: var(--color-text-secondary, #6b7280);
}

.code-only-head {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.8125rem;
}

.recheck-btn {
  margin-left: auto;
}

.issue-list {
  margin: 0.25rem 0 0;
  padding-left: 1.25rem;
  font-size: 0.75rem;
}

.issue-list li.error {
  color: var(--color-text-danger, #dc2626);
}

.issue-list li.warning {
  color: var(--color-warning, #d97706);
}
</style>
