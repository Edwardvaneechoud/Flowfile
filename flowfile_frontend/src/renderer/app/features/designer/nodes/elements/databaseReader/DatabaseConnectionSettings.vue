<template>
  <div class="connection-settings-container">
    <div class="connection-header" @click="toggleExpanded">
      <h4 class="connection-title">In line connection settings</h4>
      <button class="toggle-button">
        {{ isExpanded ? "▲" : "▼" }}
      </button>
    </div>

    <div v-if="isExpanded" class="connection-content">
      <!-- Database Type -->
      <div class="form-group">
        <label for="database-type">Database Type</label>
        <select
          id="database-type"
          :value="modelValue.database_type"
          class="form-control"
          @change="
            (e: Event) =>
              updateField('database_type', (e.target as HTMLSelectElement).value as 'postgresql')
          "
        >
          <option value="postgresql">PostgreSQL</option>
        </select>
      </div>

      <!-- Connection Details -->
      <div class="form-group">
        <label for="username">Username</label>
        <input
          id="username"
          :value="modelValue.username"
          type="text"
          class="form-control"
          placeholder="Enter username"
          @input="(e: Event) => updateField('username', (e.target as HTMLInputElement).value)"
        />
      </div>

      <div class="form-group">
        <label for="password-ref">Password Reference</label>
        <select
          id="password-ref"
          :value="modelValue.password_ref"
          class="form-control"
          @change="(e: Event) => updateField('password_ref', (e.target as HTMLSelectElement).value)"
        >
          <option value="">Select a password from the secrets</option>
          <option v-for="secret in availableSecrets" :key="secret.name" :value="secret.name">
            {{ secret.name }}
          </option>
        </select>
      </div>

      <div class="form-group">
        <label for="host">Host</label>
        <input
          id="host"
          :value="modelValue.host"
          type="text"
          class="form-control"
          placeholder="Enter host"
          @input="(e: Event) => updateField('host', (e.target as HTMLInputElement).value)"
        />
      </div>

      <div class="form-row">
        <div class="form-group half">
          <label for="port">Port</label>
          <input
            id="port"
            :value="modelValue.port"
            type="number"
            class="form-control"
            placeholder="Enter port"
            @input="(e: Event) => updateField('port', Number((e.target as HTMLInputElement).value))"
          />
        </div>

        <div class="form-group half">
          <label for="database">Database</label>
          <input
            id="database"
            :value="modelValue.database"
            type="text"
            class="form-control"
            placeholder="Enter database name"
            @input="(e: Event) => updateField('database', (e.target as HTMLInputElement).value)"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, onMounted } from "vue";
import { DatabaseConnection } from "../../../baseNode/nodeInput";
import { fetchSecretsApi } from "../../../../../pages/secretManager/secretApi";
import { Secret } from "../../../../../pages/secretManager/secretTypes";

// Props & Emits
const props = defineProps<{
  modelValue: DatabaseConnection;
}>();

const emit = defineEmits<{
  (e: "update:modelValue", value: DatabaseConnection): void;
}>();

// State
const isExpanded = ref(false);
const availableSecrets = ref<Secret[]>([]);

// Methods
const updateField = <T extends keyof DatabaseConnection>(
  field: T,
  value: DatabaseConnection[T],
) => {
  emit("update:modelValue", {
    ...props.modelValue,
    [field]: value,
  });
};

const fetchSecrets = async () => {
  try {
    const secrets = await fetchSecretsApi();
    availableSecrets.value = secrets;
  } catch (error) {
    console.error(
      "Error fetching secrets:",
      error instanceof Error ? error.message : String(error),
    );
    // Provide empty array as fallback if fetch fails
    availableSecrets.value = [];
  }
};

const toggleExpanded = () => {
  isExpanded.value = !isExpanded.value;
};

onMounted(() => {
  fetchSecrets();
});
</script>

<style scoped>
.connection-settings-container {
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  margin-bottom: 1rem;
  overflow: hidden;
}

.connection-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 1rem;
  background-color: #f8fafc;
  cursor: pointer;
  user-select: none;
  border-bottom: 1px solid #e2e8f0;
}

.connection-header:hover {
  background-color: #edf2f7;
}

.connection-title {
  margin: 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: #4a5568;
}

.toggle-button {
  background: none;
  border: none;
  color: #718096;
  font-size: 0.9rem;
  cursor: pointer;
  padding: 0;
}

.connection-content {
  padding: 1rem;
  background-color: #ffffff;
}

.form-row {
  display: flex;
  gap: 0.75rem;
  margin-bottom: 0.75rem;
  width: 100%;
  box-sizing: border-box;
}

.half {
  flex: 1;
  min-width: 0; /* Allow fields to shrink below their content size */
  max-width: calc(50% - 0.375rem); /* Account for the gap between items */
}

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
}
.form-group {
  margin-bottom: 0.75rem;
  width: 100%;
}

label {
  display: block;
  margin-bottom: 0.25rem;
  font-size: 0.875rem;
  font-weight: 500;
  color: #4a5568;
}

select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234a5568' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}
</style>
