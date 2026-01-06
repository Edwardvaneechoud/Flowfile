// SecretSelector.vue - Component for selecting secrets in custom nodes

<template>
  <div class="component-container">
    <label class="listbox-subtitle">
      {{ schema.label }}
      <span v-if="schema.required" class="required-indicator">*</span>
    </label>
    <p v-if="schema.description" class="field-description">{{ schema.description }}</p>
    <el-select
      :model-value="modelValue"
      filterable
      clearable
      placeholder="Select a secret"
      style="width: 100%"
      size="large"
      :loading="loading"
      @update:model-value="$emit('update:modelValue', $event)"
    >
      <el-option
        v-for="secret in filteredSecrets"
        :key="secret.name"
        :label="secret.name"
        :value="secret.name"
      >
        <div class="secret-option">
          <i class="fa-solid fa-key secret-icon"></i>
          <span>{{ secret.name }}</span>
        </div>
      </el-option>
    </el-select>
    <p v-if="!loading && filteredSecrets.length === 0" class="no-secrets-hint">
      No secrets available. <span class="hint-link" @click="openSecretsManager">Add secrets</span>
    </p>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, PropType } from "vue";
import type { SecretSelectorComponent } from "../interface";
import { SecretsApi } from "../../../../../../api";
import type { Secret } from "../../../../../../types";

const props = defineProps({
  schema: {
    type: Object as PropType<SecretSelectorComponent>,
    required: true,
  },
  modelValue: {
    type: String as PropType<string | null>,
    default: null,
  },
});

defineEmits(["update:modelValue"]);

const secrets = ref<Secret[]>([]);
const loading = ref(true);

const filteredSecrets = computed(() => {
  if (!props.schema.name_prefix) {
    return secrets.value;
  }
  return secrets.value.filter((secret) =>
    secret.name.toLowerCase().startsWith(props.schema.name_prefix!.toLowerCase()),
  );
});

const loadSecrets = async () => {
  loading.value = true;
  try {
    secrets.value = await SecretsApi.getAll();
  } catch (error) {
    console.error("Failed to load secrets:", error);
    secrets.value = [];
  } finally {
    loading.value = false;
  }
};

const openSecretsManager = () => {
  window.open("/#/secrets", "_blank");
};

onMounted(() => {
  loadSecrets();
});
</script>

<style scoped>
.component-container {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.listbox-subtitle {
  font-size: 0.875rem;
  font-weight: 500;
  color: #374151;
}

.required-indicator {
  color: #ef4444;
  margin-left: 0.25rem;
}

.field-description {
  font-size: 0.75rem;
  color: #6b7280;
  margin: 0;
  margin-bottom: 0.25rem;
}

.secret-option {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.secret-icon {
  color: #6b7280;
  font-size: 0.75rem;
}

.no-secrets-hint {
  font-size: 0.75rem;
  color: #6b7280;
  margin: 0.25rem 0 0 0;
}

.hint-link {
  color: #3b82f6;
  cursor: pointer;
  text-decoration: underline;
}

.hint-link:hover {
  color: #2563eb;
}
</style>
