<template>
  <div class="test-panel">
    <div class="test-toolbar">
      <button
        class="btn btn-primary"
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
          <div class="kernel-select-row">
            <label class="kernel-label">Kernel</label>
            <el-select
              v-model="selectedKernelId"
              placeholder="Select a kernel…"
              class="kernel-select-el"
            >
              <el-option v-for="k in sortedKernels" :key="k.id" :value="k.id" :label="k.name">
                <span class="kernel-option">
                  <span class="kernel-state-dot" :class="`kernel-state-dot--${k.state}`"></span>
                  <span class="kernel-option-name">{{ k.name }}</span>
                  <span
                    v-if="badgeFor(k.id).label"
                    class="match-badge"
                    :class="`match-badge--${badgeFor(k.id).tone}`"
                    :title="badgeFor(k.id).title"
                  >
                    {{ badgeFor(k.id).label }}
                  </span>
                </span>
              </el-option>
            </el-select>
            <button
              class="btn btn-sm btn-icon btn-secondary"
              type="button"
              title="Refresh kernels"
              @click="refreshKernels"
            >
              <i class="fa-solid fa-rotate" :class="{ 'fa-spin': kernelsLoading }"></i>
            </button>
          </div>
          <div
            v-if="matchUnavailable && hasDeps"
            class="kernel-match-hint kernel-match-hint--muted"
          >
            Package matching unavailable — showing all kernels.
          </div>
          <template v-else-if="hasDeps && !matchLoading">
            <div
              v-if="selectedMatch && selectedMatch.level !== 'full'"
              class="kernel-match-hint kernel-match-hint--warn"
            >
              <span :title="selectedMatch.missing.join(', ')">
                Selected kernel is missing: {{ selectedMatch.missing.join(", ") }}
              </span>
              <button
                class="kernel-action-btn"
                type="button"
                :disabled="upgradePhase !== null"
                @click="onAddMissingPackages"
              >
                {{ upgradeLabel }}
              </button>
            </div>
            <div v-else-if="!hasFullMatch" class="kernel-match-hint">
              <span>No kernel has all required packages.</span>
              <button class="kernel-action-btn" type="button" @click="createDialogOpen = true">
                Create kernel for this node
              </button>
            </div>
          </template>
        </template>
        <span v-else class="kernel-empty">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>
            This node runs in an isolated kernel, but none are available —
            <router-link :to="{ name: 'compute', query: { tab: 'kernels' } }"
              >Open Python Kernels</router-link
            >
          </span>
          <button
            class="kernel-action-btn kernel-action-btn--primary"
            type="button"
            @click="createDialogOpen = true"
          >
            Create kernel for this node
          </button>
        </span>
      </div>

      <div class="save-toggle-group">
        <label class="save-toggle">
          <input v-model="saveWithNode" type="checkbox" />
          Save test setup with node
        </label>
        <el-tooltip placement="top" :show-after="150">
          <template #content>
            <div style="max-width: 15rem; line-height: 1.5">
              Keeps the sample data and settings from this panel <strong>inside the node</strong>,
              so they're ready the next time you open it — and travel with the node when you share
              or export it. Uncheck to test without saving anything to the node.
            </div>
          </template>
          <i class="fa-solid fa-circle-info help-icon" aria-hidden="true"></i>
        </el-tooltip>
      </div>
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
        <EmptyState
          v-if="!store.dryRun.result && !store.dryRun.running"
          icon="fa-solid fa-flask"
          title="No results yet"
          description="Run the node against your sample data to see results."
        />
        <EmptyState
          v-else-if="store.dryRun.running"
          icon="fa-solid fa-spinner fa-spin"
          title="Running…"
        />
        <DryRunResults v-else-if="store.dryRun.result" :result="store.dryRun.result" />
      </div>
    </div>

    <CreateKernelDialog
      v-model="createDialogOpen"
      :suggestion="createSeed"
      @created="onKernelCreated"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { KernelInfo } from "@/types";
import CreateKernelDialog from "@/components/kernel/CreateKernelDialog.vue";
import { addPackagesToKernel } from "@/components/kernel/kernelActions";
import { describeMatch, fallbackSuggestion } from "@/components/kernel/kernelMatch";
import SampleInputEditor from "./SampleInputEditor.vue";
import DryRunResults from "./DryRunResults.vue";
import CustomNodeForm from "../../../../components/nodes/node-types/elements/customNode/CustomNodeForm.vue";
import CollapsibleSection from "../../../../components/common/CollapsibleSection/CollapsibleSection.vue";
import EmptyState from "../../../../components/common/EmptyState/EmptyState.vue";
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
  refreshKernels,
  deps,
  hasDeps,
  nodeDisplayName,
  suggestion,
  matchLoading,
  matchUnavailable,
  matchFor,
  sortedKernels,
  selectedMatch,
  hasFullMatch,
  upgradePhase,
} = useDryRunTest();

// A kernel-env node can't run in the worker, so gate Run until a kernel is chosen.
const canRun = computed(
  () => !store.dryRun.running && !(isKernelEnv.value && !selectedKernelId.value),
);

const createDialogOpen = ref(false);

const badgeFor = (kernelId: string) => describeMatch(matchFor(kernelId), deps.value.length);

const createSeed = computed(
  () =>
    suggestion.value ??
    fallbackSuggestion(
      nodeDisplayName.value,
      deps.value,
      kernels.value.map((k) => k.id),
    ),
);

const upgradeLabel = computed(() => {
  switch (upgradePhase.value) {
    case "stopping":
      return "Stopping…";
    case "rebuilding":
      return "Rebuilding…";
    case "starting":
      return "Starting…";
    default:
      return "Add missing packages";
  }
});

function onKernelCreated(kernel: KernelInfo) {
  if (!kernels.value.some((k) => k.id === kernel.id)) {
    kernels.value = [...kernels.value, kernel];
  }
  selectedKernelId.value = kernel.id;
  void refreshKernels();
}

async function onAddMissingPackages() {
  const match = selectedMatch.value;
  const kernel = kernels.value.find((k) => k.id === match?.kernel_id);
  if (!match || !kernel) return;
  try {
    await ElMessageBox.confirm(
      `This adds ${match.missing.join(", ")} to the kernel. The kernel will be stopped, ` +
        "its image rebuilt with the new packages (~30 s), and started again. " +
        "Any in-memory kernel state is lost.",
      `Add packages to "${kernel.name}"?`,
      { confirmButtonText: "Add packages", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  try {
    const updated = await addPackagesToKernel(kernel, match.missing, (phase) => {
      upgradePhase.value = phase;
    });
    ElMessage.success(`Kernel "${updated.name}" now has all required packages.`);
  } catch (error) {
    ElMessage.error(
      `Rebuild failed — kernel "${kernel.name}" is stopped. ` +
        `${(error as Error).message ?? ""} Retry or edit it on the Python Kernels page.`,
    );
  } finally {
    upgradePhase.value = null;
  }
  await refreshKernels();
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
  padding-bottom: 0.75rem;
  border-bottom: 1px solid var(--color-border-light);
}

.save-toggle-group {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
}

.save-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.help-icon {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  cursor: help;
}

.help-icon:hover {
  color: var(--color-accent);
}

.kernel-picker {
  display: inline-flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.kernel-select-row {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
}

.kernel-label {
  font-weight: var(--font-weight-medium);
}

.kernel-select-el {
  width: 16rem;
}

.kernel-option {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 0;
}

.kernel-option-name {
  overflow: hidden;
  text-overflow: ellipsis;
}

.kernel-state-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.kernel-state-dot--idle {
  background-color: #67c23a;
}

.kernel-state-dot--executing {
  background-color: #e6a23c;
}

.kernel-state-dot--starting {
  background-color: #409eff;
}

.kernel-state-dot--stopped {
  background-color: #909399;
}

.kernel-state-dot--error {
  background-color: #f56c6c;
}

.match-badge {
  margin-left: auto;
  padding: 0 6px;
  border-radius: 999px;
  font-size: var(--font-size-xs, 12px);
  white-space: nowrap;
}

.match-badge--full {
  color: #3f9c35;
  background: rgba(103, 194, 58, 0.12);
}

.match-badge--partial {
  color: #b7791f;
  background: rgba(230, 162, 60, 0.12);
}

.match-badge--none {
  color: var(--color-text-secondary, #909399);
  background: rgba(144, 147, 153, 0.12);
}

.kernel-match-hint {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
}

.kernel-match-hint--warn {
  color: #b7791f;
}

.kernel-match-hint--muted {
  color: var(--color-text-secondary, #909399);
}

.kernel-action-btn {
  padding: 4px 10px;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-md, 6px);
  background: var(--color-background-primary, #fff);
  color: var(--color-text-primary);
  font-size: var(--font-size-xs, 12px);
  cursor: pointer;
  white-space: nowrap;
}

.kernel-action-btn:hover:not(:disabled) {
  border-color: var(--color-accent, #0891b2);
  color: var(--color-accent, #0891b2);
}

.kernel-action-btn:disabled {
  opacity: 0.6;
  cursor: default;
}

.kernel-action-btn--primary {
  border-color: var(--color-accent, #0891b2);
  background: var(--color-accent, #0891b2);
  color: #fff;
}

.kernel-action-btn--primary:hover:not(:disabled) {
  color: #fff;
  opacity: 0.9;
}

.kernel-empty {
  display: inline-flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 0.375rem;
}

.kernel-empty i {
  color: var(--color-warning);
}

.kernel-empty a {
  color: var(--color-accent);
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
  border-left: 1px solid var(--color-border-light);
  padding-left: 1rem;
}

/* Divider between the Parameters and Sample-input sections. */
.test-section + .test-section {
  margin-top: 1rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border-light);
}

.block-hint {
  margin: 0 0 0.75rem;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.block-hint code {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
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
  font-size: var(--font-size-sm);
  border: 1px solid transparent;
  border-radius: var(--border-radius-sm);
  background: transparent;
  color: var(--color-text-secondary);
  cursor: pointer;
}

.input-tab.active {
  background: var(--color-background-secondary);
  border-color: var(--color-border-primary);
  color: var(--color-text-primary);
}

.no-inputs {
  text-align: center;
  padding: 2rem 1rem;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}
</style>
