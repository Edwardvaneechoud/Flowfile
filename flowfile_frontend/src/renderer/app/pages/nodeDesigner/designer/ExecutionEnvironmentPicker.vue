<template>
  <div class="env-picker">
    <div class="env-cards">
      <button
        class="env-card"
        :class="{ active: environment.kind === 'local' }"
        type="button"
        @click="setKind('local')"
      >
        <div class="env-card-head">
          <i class="fa-solid fa-microchip"></i>
          <span class="env-card-title">Local (Polars)</span>
          <i v-if="environment.kind === 'local'" class="fa-solid fa-circle-check env-check"></i>
        </div>
        <p class="env-card-desc">Runs in the Flowfile worker. Fast, no Docker needed.</p>
      </button>

      <button
        class="env-card"
        :class="{ active: environment.kind === 'kernel', disabled: !kernelsAvailable }"
        type="button"
        @click="setKind('kernel')"
      >
        <div class="env-card-head">
          <i class="fa-solid fa-box"></i>
          <span class="env-card-title">Isolated kernel</span>
          <i v-if="environment.kind === 'kernel'" class="fa-solid fa-circle-check env-check"></i>
        </div>
        <p class="env-card-desc">Runs in a Docker container with your own dependencies.</p>

        <div v-if="!kernelsAvailable" class="env-card-status">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>{{ statusMessage }}</span>
        </div>
        <div v-else class="env-card-status ok">
          <i class="fa-solid fa-circle-check"></i>
          <span>{{ kernels.length }} kernel(s) available</span>
        </div>
      </button>
    </div>

    <div v-if="!kernelsAvailable" class="env-actions">
      <a class="env-link" href="#/main/kernels" @click.prevent="openKernelManager">
        <i class="fa-solid fa-up-right-from-square"></i>
        Open Kernel Manager
      </a>
      <button class="env-retry" type="button" @click="fetchKernels">
        <i class="fa-solid fa-rotate-right"></i>
        Retry
      </button>
    </div>

    <div v-if="environment.kind === 'kernel'" class="deps-editor">
      <label class="deps-label">Dependencies (pip)</label>
      <div class="deps-tags">
        <el-tag
          v-for="(dep, index) in environment.dependencies"
          :key="dep + index"
          closable
          size="small"
          @close="removeDep(index)"
        >
          {{ dep }}
        </el-tag>
        <input
          v-model="depInput"
          class="deps-input"
          type="text"
          placeholder="e.g. scikit-learn"
          @keydown.enter.prevent="addDep"
          @blur="addDep"
        />
      </div>
      <p class="deps-hint">Installed inside the kernel container before the node runs.</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import axios from "axios";
import { ElTag } from "element-plus";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import type { EnvironmentKind } from "../designerState";
import type { KernelInfo } from "../types";

const store = useNodeDesignerStore();
const router = useRouter();

const environment = computed(() => store.environment);

const kernels = ref<KernelInfo[]>([]);
const dockerError = ref(false);
const depInput = ref("");

const kernelsAvailable = computed(() => !dockerError.value);

const statusMessage = computed(() =>
  dockerError.value
    ? "Docker is unavailable — start Docker and open the Kernel Manager to configure a kernel."
    : "No kernels configured yet.",
);

function setKind(kind: EnvironmentKind) {
  if (kind === "kernel" && !kernelsAvailable.value) {
    // Still allow selecting the intent; the drawer will require a kernel binding at run time.
  }
  environment.value.kind = kind;
}

function addDep() {
  const value = depInput.value.trim();
  if (!value) return;
  if (!environment.value.dependencies.includes(value)) {
    environment.value.dependencies.push(value);
  }
  depInput.value = "";
}

function removeDep(index: number) {
  environment.value.dependencies.splice(index, 1);
}

function openKernelManager() {
  router.push("/main/kernels");
}

async function fetchKernels() {
  try {
    const response = await axios.get("/kernels/");
    kernels.value = response.data || [];
    dockerError.value = false;
  } catch {
    kernels.value = [];
    dockerError.value = true;
  }
}

onMounted(fetchKernels);
</script>

<style scoped>
.env-picker {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.env-cards {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.env-card {
  text-align: left;
  padding: 0.75rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-md, 6px);
  background: var(--color-background-primary, #fff);
  cursor: pointer;
  transition: border-color 0.15s;
}

.env-card:hover {
  border-color: var(--color-accent, #0891b2);
}

.env-card.active {
  border-color: var(--color-accent, #0891b2);
  background: var(--color-focus-ring-accent, rgba(8, 145, 178, 0.06));
}

.env-card.disabled {
  opacity: 0.85;
}

.env-card-head {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  font-size: 0.875rem;
  color: var(--color-text-primary, #374151);
}

.env-card-head > i:first-child {
  color: var(--color-text-secondary, #6b7280);
}

.env-check {
  margin-left: auto;
  color: var(--color-accent, #0891b2);
}

.env-card-desc {
  margin: 0.375rem 0 0;
  font-size: 0.75rem;
  color: var(--color-text-secondary, #6b7280);
}

.env-card-status {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  margin-top: 0.5rem;
  font-size: 0.75rem;
  color: var(--color-text-danger, #dc2626);
}

.env-card-status.ok {
  color: var(--color-success, #16a34a);
}

.env-actions {
  display: flex;
  align-items: center;
  gap: 0.75rem;
}

.env-link,
.env-retry {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  font-size: 0.75rem;
  color: var(--color-button-primary, #4a6cf7);
  background: transparent;
  border: none;
  cursor: pointer;
  padding: 0;
}

.env-link:hover,
.env-retry:hover {
  text-decoration: underline;
}

.deps-editor {
  display: flex;
  flex-direction: column;
  gap: 0.375rem;
}

.deps-label {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--text-secondary, #6b7280);
}

.deps-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 0.375rem;
  align-items: center;
  padding: 0.375rem;
  border: 1px solid var(--color-border-primary, #d1d5db);
  border-radius: var(--border-radius-md, 6px);
  background: var(--color-background-primary, #fff);
}

.deps-input {
  flex: 1;
  min-width: 100px;
  border: none;
  outline: none;
  background: transparent;
  font-size: 0.8125rem;
  color: var(--color-text-primary, #374151);
}

.deps-hint {
  margin: 0;
  font-size: 0.6875rem;
  color: var(--color-text-secondary, #6b7280);
}
</style>
