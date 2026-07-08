<template>
  <div class="test-panel">
    <div class="test-toolbar">
      <button
        class="run-btn"
        type="button"
        data-testid="test-run"
        :disabled="store.dryRun.running"
        @click="run"
      >
        <i v-if="store.dryRun.running" class="fa-solid fa-spinner fa-spin"></i>
        <i v-else class="fa-solid fa-play"></i>
        {{ store.dryRun.running ? "Running..." : "Run test" }}
      </button>
      <label class="save-toggle">
        <input v-model="saveWithNode" type="checkbox" />
        Save test setup with node
      </label>
    </div>

    <div class="test-body">
      <div class="inputs-column">
        <div v-if="hasParameters" class="test-block">
          <div class="block-title">Parameters</div>
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
        </div>

        <div class="test-block">
          <div v-if="hasParameters" class="block-title">Sample input</div>
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
              @update:table="onTableUpdate(activeInput, $event)"
            />
          </div>
        </div>
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
import { computed, ref, watch } from "vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import SampleInputEditor from "./SampleInputEditor.vue";
import DryRunResults from "./DryRunResults.vue";
import CustomNodeForm from "../../../../components/nodes/node-types/elements/customNode/CustomNodeForm.vue";
import type { FileColumn } from "../../../../components/nodes/baseNode/nodeInterfaces";
import {
  columnDataToTable,
  emptyTable,
  tableToColumnData,
  type SampleTable,
} from "../../sampleData";
import { dryRunCustomNode, type DryRunBody } from "../../../../api/nodeDesigner";
import type { ExampleInput } from "../../designerState";

const store = useNodeDesignerStore();

const inputCount = computed(() => Math.max(0, store.designerState.number_of_inputs));
const activeInput = ref(0);
const saveWithNode = ref(store.designerState.example_inputs !== null);

// One SampleTable per input port, seeded from example_inputs when present.
const tables = ref<SampleTable[]>([]);

function seedTables() {
  const count = inputCount.value;
  const examples = store.designerState.example_inputs;
  const next: SampleTable[] = [];
  for (let i = 0; i < count; i++) {
    const example = examples?.[i];
    next.push(example ? columnDataToTable(example.data) : emptyTable());
  }
  tables.value = next;
  if (activeInput.value >= count) activeInput.value = 0;
}

seedTables();
watch(inputCount, seedTables);

// Column-driven controls (ColumnSelector, IncomingColumns selects) preview against
// the first sample input's columns, mirroring the Form-tab live preview.
const hasParameters = computed(() => store.sections.some((s) => s.components.length > 0));
const paramColumns = computed<string[]>(() => tables.value[0]?.columns.map((c) => c.name) ?? []);
const paramColumnTypes = computed<FileColumn[]>(() =>
  paramColumns.value.map((name) => ({ name, data_type: "String" }) as FileColumn),
);

function onTableUpdate(index: number, table: SampleTable) {
  tables.value[index] = table;
  persistExampleInputs();
}

function persistExampleInputs() {
  if (!saveWithNode.value) {
    store.designerState.example_inputs = null;
    return;
  }
  const examples: ExampleInput[] = tables.value.map((t) => ({ data: tableToColumnData(t) }));
  store.designerState.example_inputs = examples;
  store.designerState.example_settings = snapshotSettings();
}

watch(saveWithNode, persistExampleInputs);

function snapshotSettings(): Record<string, Record<string, unknown>> {
  return JSON.parse(JSON.stringify(store.previewValues));
}

async function run() {
  const sampleInputs = tables.value.map((t) => tableToColumnData(t));
  const body: DryRunBody = {
    settings_values: snapshotSettings(),
    sample_inputs: sampleInputs.length ? sampleInputs : null,
    row_limit: 100,
    timeout_seconds: 30,
  };
  if (store.codeOnly) body.code = store.codeText;
  else body.designer_state = store.designerState;

  store.dryRun.running = true;
  store.dryRun.error = null;
  try {
    const result = await dryRunCustomNode(body);
    store.dryRun.result = result;
    if (saveWithNode.value) persistExampleInputs();
  } catch (err: unknown) {
    const e = err as { response?: { data?: { detail?: string } }; message?: string };
    store.dryRun.error = e.response?.data?.detail || e.message || "Dry run failed";
    store.dryRun.result = null;
  } finally {
    store.dryRun.running = false;
  }
}
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

.test-block + .test-block {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border-light, #e5e7eb);
}

.block-title {
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary, #6b7280);
  margin-bottom: 0.25rem;
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
