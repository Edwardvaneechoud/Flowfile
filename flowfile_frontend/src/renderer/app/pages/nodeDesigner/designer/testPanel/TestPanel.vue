<template>
  <div class="test-panel">
    <div class="test-toolbar">
      <button
        class="run-btn"
        type="button"
        data-testid="test-run"
        :disabled="!canRun"
        :title="isKernelEnv && !selectedKernelId ? 'Select a kernel to run the test' : undefined"
        @click="run"
      >
        <i v-if="store.dryRun.running" class="fa-solid fa-spinner fa-spin"></i>
        <i v-else class="fa-solid fa-play"></i>
        {{ store.dryRun.running ? "Running..." : "Run test" }}
      </button>

      <div v-if="isKernelEnv" class="kernel-picker">
        <template v-if="kernelsAvailable && kernels.length">
          <label class="kernel-label" for="test-kernel">Kernel</label>
          <select id="test-kernel" v-model="selectedKernelId" class="kernel-select">
            <option v-for="k in kernels" :key="k.id" :value="k.id">
              {{ k.name }} ({{ k.state }})
            </option>
          </select>
          <button class="kernel-refresh" type="button" title="Refresh kernels" @click="loadKernels">
            <i class="fa-solid fa-rotate" :class="{ 'fa-spin': kernelsLoading }"></i>
          </button>
        </template>
        <span v-else class="kernel-empty">
          <i class="fa-solid fa-triangle-exclamation"></i>
          This node runs in an isolated kernel, but none are available —
          <router-link :to="{ name: 'kernelManager' }">Open Kernel Manager</router-link>
        </span>
      </div>

      <label class="save-toggle">
        <input v-model="saveWithNode" type="checkbox" />
        Save test setup with node
      </label>
    </div>

    <div class="test-body">
      <div class="inputs-column">
        <CollapsibleSection
          v-if="hasParameters"
          title="Parameters"
          icon="fa-solid fa-sliders"
          persist-key="nd-test-params"
          :default-open="true"
          class="test-section params-section"
        >
          <p class="block-hint">
            Values passed to <code>self.settings_schema</code> for this test run.
          </p>
          <CustomNodeForm
            :schema="store.frontendSchema"
            :form-data="store.previewValues"
            :incoming-columns="paramColumns"
            :column-types="paramColumnTypes"
            @update:form-data="store.setPreviewValues"
          />
        </CollapsibleSection>

        <CollapsibleSection
          title="Sample input"
          icon="fa-solid fa-table-cells"
          persist-key="nd-test-sample"
          :default-open="true"
          class="test-section"
        >
          <div v-if="inputCount === 0" class="no-inputs">This node takes no inputs.</div>
          <div v-else class="input-tabs">
            <div v-if="inputCount > 1" class="input-tab-row">
              <button
                v-for="i in inputCount"
                :key="i"
                class="input-tab"
                :class="{ active: activeInput === i - 1 }"
                type="button"
                @click="activeInput = i - 1"
              >
                Input {{ i - 1 }}
              </button>
            </div>
            <SampleInputEditor
              :key="activeInput"
              :table="tables[activeInput]"
              @update:table="setTable(activeInput, $event)"
            />
          </div>
        </CollapsibleSection>
      </div>

      <div class="results-column">
        <div v-if="!store.dryRun.result && !store.dryRun.running" class="results-empty">
          <i class="fa-solid fa-flask"></i>
          <p>Run the node against your sample data to see results.</p>
        </div>
        <div v-else-if="store.dryRun.running" class="results-empty">
          <i class="fa-solid fa-spinner fa-spin"></i>
          <p>Running…</p>
        </div>
        <DryRunResults v-else-if="store.dryRun.result" :result="store.dryRun.result" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import SampleInputEditor from "./SampleInputEditor.vue";
import DryRunResults from "./DryRunResults.vue";
import CustomNodeForm from "../../../../components/nodes/node-types/elements/customNode/CustomNodeForm.vue";
import CollapsibleSection from "../../../../components/common/CollapsibleSection/CollapsibleSection.vue";
import { useDryRunTest } from "../../composables/useDryRunTest";

const store = useNodeDesignerStore();

// Shared singleton state so the Test tab and the Code-tab test dock stay in sync.
const {
  tables,
  activeInput,
  saveWithNode,
  inputCount,
  hasParameters,
  paramColumns,
  paramColumnTypes,
  setTable,
  run,
  isKernelEnv,
  kernels,
  kernelsLoading,
  kernelsAvailable,
  selectedKernelId,
  loadKernels,
} = useDryRunTest();

// A kernel-env node can't run in the worker, so gate Run until a kernel is chosen.
const canRun = computed(
  () => !store.dryRun.running && !(isKernelEnv.value && !selectedKernelId.value),
);
</script>

<style scoped>
.test-panel {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  height: 100%;
  min-height: 0;
}

.test-toolbar {
  display: flex;
  align-items: center;
  gap: 1rem;
  flex-shrink: 0;
  padding-bottom: 0.75rem;
  border-bottom: 1px solid var(--color-border-light, #e5e7eb);
}

.run-btn {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  background: var(--color-button-primary, #4a6cf7);
  color: #fff;
  border: none;
  border-radius: var(--border-radius-sm, 4px);
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
}

.run-btn:disabled {
  opacity: 0.7;
  cursor: default;
}

.save-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  font-size: 0.8125rem;
  color: var(--color-text-secondary, #6b7280);
}

.kernel-picker {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  font-size: 0.8125rem;
  color: var(--color-text-secondary, #6b7280);
}

.kernel-label {
  font-weight: 500;
}

.kernel-select {
  padding: 0.3rem 0.5rem;
  font-size: 0.8125rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-sm, 4px);
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary, #374151);
  max-width: 16rem;
}

.kernel-refresh {
  padding: 0.25rem 0.4rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-sm, 4px);
  background: var(--color-background-primary, #fff);
  color: var(--color-text-secondary, #6b7280);
  cursor: pointer;
}

.kernel-empty {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
}

.kernel-empty i {
  color: var(--color-warning, #f59e0b);
}

.kernel-empty a {
  color: var(--color-button-primary, #4a6cf7);
}

.test-body {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
  flex: 1;
  min-height: 0;
}

.inputs-column,
.results-column {
  overflow-y: auto;
  min-height: 0;
}

/* Divider between the config (left) and results (right) columns. */
.results-column {
  border-left: 1px solid var(--color-border-light, #e5e7eb);
  padding-left: 1rem;
}

/* Divider between the Parameters and Sample-input sections. */
.test-section + .test-section {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border-light, #e5e7eb);
}

.block-hint {
  margin: 0 0 0.75rem;
  font-size: 0.75rem;
  color: var(--color-text-secondary, #6b7280);
}

.block-hint code {
  font-family: var(--font-family-mono, monospace);
  font-size: 0.6875rem;
}

/* Flatten CustomNodeForm's card chrome inside the params section: the collapsible
   header already groups it, so drop the inner white card and align to the edge. */
.params-section :deep(.listbox-wrapper) {
  margin: 0;
  padding: 0;
  background: none;
  box-shadow: none;
}

.params-section :deep(.listbox-wrapper + .listbox-wrapper) {
  margin-top: 0.75rem;
}

.input-tab-row {
  display: flex;
  gap: 0.25rem;
  margin-bottom: 0.5rem;
}

.input-tab {
  padding: 0.25rem 0.625rem;
  font-size: 0.75rem;
  border: 1px solid transparent;
  border-radius: 4px;
  background: transparent;
  color: var(--color-text-secondary, #6b7280);
  cursor: pointer;
}

.input-tab.active {
  background: var(--color-background-secondary, #f3f4f6);
  border-color: var(--color-border-primary, #d1d5db);
  color: var(--color-text-primary, #374151);
}

.no-inputs,
.results-empty {
  text-align: center;
  padding: 2rem 1rem;
  color: var(--color-text-secondary, #6b7280);
}

.results-empty i {
  font-size: 1.75rem;
  margin-bottom: 0.5rem;
}

.results-empty p {
  margin: 0;
  font-size: 0.8125rem;
}
</style>
