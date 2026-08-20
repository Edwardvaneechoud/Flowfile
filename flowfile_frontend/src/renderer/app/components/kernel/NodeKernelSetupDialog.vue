<template>
  <el-dialog
    :model-value="modelValue"
    :title="`Set up a kernel for ${nodeName}`"
    width="560px"
    align-center
    append-to-body
    :close-on-click-modal="false"
    @update:model-value="emit('update:modelValue', $event)"
  >
    <div class="setup-body">
      <p class="lead">This node runs in a kernel and needs these Python packages:</p>
      <div class="dep-chips">
        <span v-for="dep in dependencies" :key="dep" class="dep-chip">{{ dep }}</span>
      </div>

      <p v-if="loading" class="loading-line">
        <i class="fa-solid fa-spinner fa-spin"></i> Checking your kernels…
      </p>
      <template v-else>
        <div v-if="topMatches.length" class="match-list">
          <div v-for="match in topMatches" :key="match.kernel_id" class="match-row">
            <div class="match-info">
              <span class="match-name">{{ match.kernel_name }}</span>
              <span class="match-state">{{ match.state }}</span>
              <span
                class="match-badge"
                :class="`tone-${badge(match).tone}`"
                :title="badge(match).title"
              >
                {{ badge(match).label }}
              </span>
            </div>
            <span v-if="match.level === 'full'" class="use-hint">Use this kernel</span>
            <el-button
              v-else
              size="small"
              :loading="upgradingId === match.kernel_id"
              :disabled="upgradingId !== null && upgradingId !== match.kernel_id"
              @click="upgrade(match)"
            >
              {{ upgradingId === match.kernel_id ? phaseLabel : "Add missing packages" }}
            </el-button>
          </div>
        </div>
        <p v-else class="no-matches">None of your kernels have these packages.</p>
      </template>
    </div>

    <template #footer>
      <div class="setup-footer">
        <div class="setup-footer__buttons">
          <el-button :disabled="upgradingId !== null" @click="emit('update:modelValue', false)">
            Later
          </el-button>
          <el-button type="primary" :disabled="upgradingId !== null" @click="createOpen = true">
            Create kernel
          </el-button>
        </div>
        <p class="setup-footer__hint">You can also do this from the node's settings.</p>
      </div>
    </template>
  </el-dialog>

  <CreateKernelDialog v-model="createOpen" :suggestion="createSuggestion" @created="onCreated" />
</template>

<script setup lang="ts">
import { ElButton, ElDialog, ElMessage, ElMessageBox } from "element-plus";
import { computed, ref, watch } from "vue";

import { KernelApi } from "@/api/kernel.api";
import type { KernelInfo, KernelMatch, KernelSuggestion } from "@/types";

import CreateKernelDialog from "./CreateKernelDialog.vue";
import { addPackagesToKernel, type UpgradePhase } from "./kernelActions";
import { describeMatch, fallbackSuggestion } from "./kernelMatch";

const props = defineProps<{
  modelValue: boolean;
  nodeName: string;
  dependencies: string[];
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: boolean): void;
}>();

const loading = ref(false);
const kernels = ref<KernelInfo[]>([]);
const matches = ref<KernelMatch[]>([]);
const suggestion = ref<KernelSuggestion | null>(null);
const createOpen = ref(false);
const upgradingId = ref<string | null>(null);
const phase = ref<UpgradePhase | null>(null);

// @open never fires for a dialog mounted already-open — watch the model instead.
watch(
  () => props.modelValue,
  (open) => {
    if (open) void load();
  },
  { immediate: true },
);

async function load(): Promise<void> {
  loading.value = true;
  const [kernelsRes, matchRes] = await Promise.allSettled([
    KernelApi.getAll(),
    KernelApi.matchKernels(props.dependencies, props.nodeName),
  ]);
  kernels.value = kernelsRes.status === "fulfilled" ? kernelsRes.value : [];
  if (matchRes.status === "fulfilled") {
    matches.value = matchRes.value.matches;
    suggestion.value = matchRes.value.suggestion;
  } else {
    matches.value = [];
    suggestion.value = null;
  }
  loading.value = false;
}

const topMatches = computed(() => matches.value.filter((m) => m.level !== "none").slice(0, 3));
const createSuggestion = computed(
  () =>
    suggestion.value ??
    fallbackSuggestion(
      props.nodeName,
      props.dependencies,
      kernels.value.map((k) => k.id),
    ),
);

function badge(match: KernelMatch) {
  return describeMatch(match, props.dependencies.length);
}

const PHASE_LABELS: Record<UpgradePhase, string> = {
  stopping: "Stopping…",
  rebuilding: "Rebuilding…",
  starting: "Starting…",
};
const phaseLabel = computed(() => (phase.value ? PHASE_LABELS[phase.value] : "Working…"));

async function upgrade(match: KernelMatch): Promise<void> {
  const kernel = kernels.value.find((k) => k.id === match.kernel_id);
  if (!kernel) {
    ElMessage.error("Could not load this kernel — try again from the Python Kernels page.");
    return;
  }
  try {
    await ElMessageBox.confirm(
      `"${kernel.name}" will be stopped, its image rebuilt with the new packages ` +
        "(about 30 seconds), and started again. Anything held in the kernel's memory is lost.",
      "Add missing packages",
      { confirmButtonText: "Add packages", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return;
  }
  upgradingId.value = match.kernel_id;
  try {
    await addPackagesToKernel(kernel, match.missing, (p) => (phase.value = p));
    ElMessage.success(`Kernel "${kernel.name}" is ready for ${props.nodeName}.`);
    emit("update:modelValue", false);
  } catch (error) {
    ElMessage.error(
      `Failed to rebuild "${kernel.name}": ${(error as Error).message}. ` +
        "The kernel is stopped — retry here or from the Python Kernels page.",
    );
  } finally {
    upgradingId.value = null;
    phase.value = null;
  }
}

function onCreated(): void {
  // The creation tracker already notified about the outcome.
  emit("update:modelValue", false);
}
</script>

<style scoped>
.setup-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
}
.lead {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
.dep-chips {
  display: flex;
  flex-wrap: wrap;
  gap: var(--spacing-1);
}
.dep-chip {
  display: inline-flex;
  align-items: center;
  font-size: var(--font-size-xs);
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  background: var(--color-background-tertiary);
  color: var(--color-text-secondary);
  font-family: var(--font-family-mono);
  white-space: nowrap;
}
.loading-line,
.no-matches {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
.match-list {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}
.match-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
}
.match-info {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-width: 0;
}
.match-name {
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.match-state {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-transform: capitalize;
}
.match-badge {
  display: inline-flex;
  align-items: center;
  font-size: var(--font-size-xs);
  padding: 1px 8px;
  border-radius: var(--border-radius-full);
  white-space: nowrap;
}
.tone-full {
  background: var(--color-success-light);
  color: var(--color-success-hover);
}
.tone-partial {
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
}
.tone-none,
.tone-unknown {
  background: var(--color-background-tertiary);
  color: var(--color-text-muted);
}
.use-hint {
  font-size: var(--font-size-sm);
  color: var(--color-success-hover);
  white-space: nowrap;
}
.setup-footer {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: var(--spacing-2);
}
.setup-footer__buttons {
  display: flex;
  gap: var(--spacing-2);
}
.setup-footer__hint {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}
</style>
