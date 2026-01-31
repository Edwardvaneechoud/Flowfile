<template>
  <div class="card mb-3">
    <div class="card-header" style="cursor: pointer" @click="isExpanded = !isExpanded">
      <h3 class="card-title">
        <i class="fa-solid fa-plus" style="margin-right: 6px"></i>
        Create New Kernel
      </h3>
      <i
        :class="isExpanded ? 'fa-solid fa-chevron-up' : 'fa-solid fa-chevron-down'"
        style="color: var(--color-text-muted)"
      ></i>
    </div>
    <div v-if="isExpanded" class="card-content">
      <form class="form" @submit.prevent="handleSubmit">
        <div class="form-grid">
          <div class="form-field">
            <label for="kernel-id" class="form-label">Kernel ID</label>
            <input
              id="kernel-id"
              v-model="form.id"
              type="text"
              class="form-input"
              placeholder="my-kernel"
              pattern="[a-zA-Z0-9_-]+"
              title="Only letters, numbers, hyphens, and underscores"
              required
            />
          </div>

          <div class="form-field">
            <label for="kernel-name" class="form-label">Display Name</label>
            <input
              id="kernel-name"
              v-model="form.name"
              type="text"
              class="form-input"
              placeholder="My Data Science Kernel"
              required
            />
          </div>
        </div>

        <div class="form-field">
          <label for="kernel-packages" class="form-label">
            Python Packages
            <span class="form-label-hint">(comma-separated)</span>
          </label>
          <input
            id="kernel-packages"
            v-model="packagesInput"
            type="text"
            class="form-input"
            placeholder="pandas, scikit-learn, torch"
          />
        </div>

        <div class="form-grid form-grid--3col">
          <div class="form-field">
            <label for="kernel-memory" class="form-label">Memory (GB)</label>
            <input
              id="kernel-memory"
              v-model.number="form.memory_gb"
              type="number"
              class="form-input"
              min="0.5"
              max="64"
              step="0.5"
            />
          </div>

          <div class="form-field">
            <label for="kernel-cpu" class="form-label">CPU Cores</label>
            <input
              id="kernel-cpu"
              v-model.number="form.cpu_cores"
              type="number"
              class="form-input"
              min="0.5"
              max="32"
              step="0.5"
            />
          </div>

          <div class="form-field">
            <label class="form-label">GPU</label>
            <label class="toggle-label">
              <input v-model="form.gpu" type="checkbox" class="toggle-checkbox" />
              <span class="toggle-text">{{ form.gpu ? "Enabled" : "Disabled" }}</span>
            </label>
          </div>
        </div>

        <div class="form-actions">
          <button type="submit" class="btn btn-primary" :disabled="!isValid || isSubmitting">
            <i :class="isSubmitting ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-plus'"></i>
            {{ isSubmitting ? "Creating..." : "Create Kernel" }}
          </button>
        </div>
      </form>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from "vue";
import type { KernelConfig } from "../../types";

const emit = defineEmits<{
  create: [config: KernelConfig];
}>();

const isExpanded = ref(false);
const isSubmitting = ref(false);
const packagesInput = ref("");

const form = ref({
  id: "",
  name: "",
  memory_gb: 4.0,
  cpu_cores: 2.0,
  gpu: false,
});

const isValid = computed(() => {
  return form.value.id.trim() !== "" && form.value.name.trim() !== "";
});

const parsePackages = (): string[] => {
  if (!packagesInput.value.trim()) return [];
  return packagesInput.value
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
};

const handleSubmit = async () => {
  if (!isValid.value || isSubmitting.value) return;

  isSubmitting.value = true;
  try {
    const config: KernelConfig = {
      id: form.value.id.trim(),
      name: form.value.name.trim(),
      packages: parsePackages(),
      memory_gb: form.value.memory_gb,
      cpu_cores: form.value.cpu_cores,
      gpu: form.value.gpu,
    };
    emit("create", config);
    // Reset form on emit (parent handles success/error)
    form.value = { id: "", name: "", memory_gb: 4.0, cpu_cores: 2.0, gpu: false };
    packagesInput.value = "";
    isExpanded.value = false;
  } finally {
    isSubmitting.value = false;
  }
};
</script>

<style scoped>
.form-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-3);
}

.form-grid--3col {
  grid-template-columns: 1fr 1fr 1fr;
}

.form-label-hint {
  font-weight: var(--font-weight-normal);
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.toggle-label {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  cursor: pointer;
  height: 36px;
}

.toggle-checkbox {
  width: 16px;
  height: 16px;
  accent-color: var(--color-accent);
  cursor: pointer;
}

.toggle-text {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
</style>
