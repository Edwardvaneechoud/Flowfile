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
          @change="(e: Event) => updateDatabaseType((e.target as HTMLSelectElement).value)"
        >
          <option v-for="dialect in dialects" :key="dialect.name" :value="dialect.name">
            {{ dialect.display_name }}
          </option>
        </select>
      </div>

      <!-- Connection Details (hidden for file-based databases) -->
      <div v-if="!isFileBasedConnection" class="form-group">
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

      <div v-if="!isFileBasedConnection && authMethodOptions.length > 1" class="form-group">
        <label for="auth-method">Authentication Method</label>
        <select
          id="auth-method"
          :value="modelValue.auth_method || 'password'"
          class="form-control"
          @change="(e: Event) => updateField('auth_method', (e.target as HTMLSelectElement).value)"
        >
          <option v-for="method in authMethodOptions" :key="method" :value="method">
            {{ authMethodLabel(method) }}
          </option>
        </select>
      </div>

      <div v-if="!isFileBasedConnection && !usesKeyPair" class="form-group">
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

      <div v-if="!isFileBasedConnection && usesKeyPair" class="form-group">
        <label for="private-key-ref">Private Key Reference</label>
        <select
          id="private-key-ref"
          :value="modelValue.private_key_ref"
          class="form-control"
          @change="
            (e: Event) => updateField('private_key_ref', (e.target as HTMLSelectElement).value)
          "
        >
          <option value="">Select the private key (PEM) from the secrets</option>
          <option v-for="secret in availableSecrets" :key="secret.name" :value="secret.name">
            {{ secret.name }}
          </option>
        </select>
      </div>

      <div v-if="!isFileBasedConnection && usesKeyPair" class="form-group">
        <label for="private-key-passphrase-ref">Key Passphrase Reference (optional)</label>
        <select
          id="private-key-passphrase-ref"
          :value="modelValue.private_key_passphrase_ref"
          class="form-control"
          @change="
            (e: Event) =>
              updateField('private_key_passphrase_ref', (e.target as HTMLSelectElement).value)
          "
        >
          <option value="">No passphrase</option>
          <option v-for="secret in availableSecrets" :key="secret.name" :value="secret.name">
            {{ secret.name }}
          </option>
        </select>
      </div>

      <div v-for="field in dialectExtraFields" :key="field.name" class="form-group">
        <label :for="`extra-${field.name}`">{{ field.label }}</label>
        <input
          :id="`extra-${field.name}`"
          :value="modelValue.extra_params?.[field.name] ?? ''"
          type="text"
          class="form-control"
          :placeholder="`Enter ${field.label.toLowerCase()}`"
          :required="field.required"
          @input="(e: Event) => updateExtraParam(field.name, (e.target as HTMLInputElement).value)"
        />
      </div>

      <div v-if="!isFileBasedConnection && !isHidden('host')" class="form-group">
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

      <div v-if="!isFileBasedConnection" class="form-row">
        <div v-if="!isHidden('port')" class="form-group half">
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

      <div v-if="isFileBasedConnection" class="form-group">
        <label for="database">File Path</label>
        <input
          id="database"
          :value="modelValue.database"
          type="text"
          class="form-control"
          placeholder="/path/to/database.db"
          @input="(e: Event) => updateField('database', (e.target as HTMLInputElement).value)"
        />
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, computed, onMounted } from "vue";
import { DatabaseConnection } from "../../../baseNode/nodeInput";
import { SecretsApi } from "../../../../../api";
import type { Secret } from "../../../../../types";
import { useDbDialects } from "../../../../../composables/useDbDialects";

const fetchSecretsApi = SecretsApi.getAll;

// Props & Emits
const props = defineProps<{
  modelValue: DatabaseConnection;
}>();

const { dialects, isFileBased, extraFields, isFieldHidden, authMethods } = useDbDialects();

const AUTH_METHOD_LABELS: Record<string, string> = {
  password: "Password",
  key_pair: "Key pair (JWT)",
};

const isFileBasedConnection = computed(() => isFileBased(props.modelValue.database_type));

const dialectExtraFields = computed(() => extraFields(props.modelValue.database_type));

const isHidden = (field: string) => isFieldHidden(props.modelValue.database_type, field);

const authMethodOptions = computed(() => authMethods(props.modelValue.database_type));

const usesKeyPair = computed(() => props.modelValue.auth_method === "key_pair");

const authMethodLabel = (method: string): string => AUTH_METHOD_LABELS[method] ?? method;

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

const updateDatabaseType = (value: string) => {
  const next: DatabaseConnection = { ...props.modelValue, database_type: value };
  // Auth fields are dialect-specific: a stale key_pair selection on a dialect
  // without it would fail backend validation with no visible field to fix.
  if (!authMethods(value).includes(next.auth_method || "password")) {
    next.auth_method = undefined;
    next.private_key_ref = undefined;
    next.private_key_passphrase_ref = undefined;
  }
  emit("update:modelValue", next);
};

const updateExtraParam = (name: string, value: string) => {
  const params = { ...(props.modelValue.extra_params || {}) };
  if (value) {
    params[name] = value;
  } else {
    delete params[name];
  }
  updateField("extra_params", Object.keys(params).length ? params : null);
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
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  margin-bottom: 1rem;
  overflow: hidden;
}

.connection-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem 1rem;
  background-color: var(--color-background-secondary);
  cursor: pointer;
  user-select: none;
  border-bottom: 1px solid var(--color-border-primary);
}

.connection-header:hover {
  background-color: var(--color-background-tertiary);
}

.connection-title {
  margin: 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.toggle-button {
  background: none;
  border: none;
  color: var(--color-text-tertiary);
  font-size: 0.9rem;
  cursor: pointer;
  padding: 0;
}

.connection-content {
  padding: 1rem;
  background-color: var(--color-background-primary);
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
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
  background-color: var(--color-background-primary);
  color: var(--color-text-primary);
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
  color: var(--color-text-secondary);
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
