//flowfile_frontend/src/renderer/app/pages/databaseManager/DatabaseConnectionForm.vue

<template>
  <form class="form" @submit.prevent="submitForm">
    <div class="form-grid">
      <div class="form-field">
        <label for="connection-name" class="form-label">Connection Name</label>
        <input
          id="connection-name"
          v-model="connection.connectionName"
          type="text"
          class="form-input"
          placeholder="my_postgres_db"
          required
          :disabled="props.isEditing"
        />
      </div>

      <div class="form-field">
        <label for="database-type" class="form-label">Database Type</label>
        <select id="database-type" v-model="connection.databaseType" class="form-input" required>
          <option value="postgresql">PostgreSQL</option>
          <option value="mysql">MySQL</option>
        </select>
      </div>

      <div class="form-field">
        <label for="host" class="form-label">Host</label>
        <input
          id="host"
          v-model="connection.host"
          type="text"
          class="form-input"
          placeholder="localhost or IP address"
          required
        />
      </div>

      <div class="form-field">
        <label for="port" class="form-label">Port</label>
        <input
          id="port"
          v-model="connection.port"
          type="number"
          class="form-input"
          :placeholder="String(defaultPorts[connection.databaseType as DatabaseType] || 5432)"
        />
      </div>

      <div class="form-field">
        <label for="database" class="form-label">Database</label>
        <input
          id="database"
          v-model="connection.database"
          type="text"
          class="form-input"
          placeholder="Database name"
        />
      </div>

      <div class="form-field">
        <label for="username" class="form-label">Username</label>
        <input
          id="username"
          v-model="connection.username"
          type="text"
          class="form-input"
          placeholder="Username"
          required
        />
      </div>

      <div class="form-field">
        <label for="password" class="form-label">Password</label>
        <div class="password-field">
          <input
            id="password"
            v-model="connection.password"
            :type="showPassword ? 'text' : 'password'"
            class="form-input"
            :placeholder="props.isEditing ? 'Leave blank to keep existing' : 'Password'"
            :required="!props.isEditing"
          />
          <button
            type="button"
            class="toggle-visibility"
            aria-label="Toggle password visibility"
            @click="showPassword = !showPassword"
          >
            <i :class="showPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
          </button>
        </div>
      </div>

      <div class="form-field">
        <div class="checkbox-container">
          <input
            id="ssl-enabled"
            v-model="connection.sslEnabled"
            type="checkbox"
            class="checkbox-input"
          />
          <label for="ssl-enabled" class="form-label">Enable SSL</label>
        </div>
      </div>
    </div>

    <div class="form-actions">
      <button type="button" class="btn btn-secondary" @click="$emit('cancel')">Cancel</button>
      <button type="submit" class="btn btn-primary" :disabled="!isValid || isSubmitting">
        {{ submitButtonText }}
      </button>
    </div>
  </form>
</template>

<script lang="ts" setup>
import { ref, computed, defineProps, defineEmits, watch } from "vue";
import type { FullDatabaseConnection } from "./databaseConnectionTypes";
import { defaultPorts } from "./databaseConnectionTypes";
import type { DatabaseType } from "./databaseConnectionTypes";

const props = defineProps<{
  initialConnection?: FullDatabaseConnection;
  isSubmitting?: boolean;
  isEditing?: boolean;
}>();

const emit = defineEmits<{
  (e: "submit", connection: FullDatabaseConnection): void;
  (e: "cancel"): void;
}>();

// Create a default connection object
const defaultConnection = (): FullDatabaseConnection => ({
  connectionName: "",
  databaseType: "postgresql",
  username: "",
  password: "",
  host: "",
  port: 5432,
  database: "",
  sslEnabled: false,
  url: "",
});

// Initialize connection with props or default values
const connection = ref<FullDatabaseConnection>(
  props.initialConnection ? { ...props.initialConnection } : defaultConnection(),
);

// Watch for changes in initialConnection prop
watch(
  () => props.initialConnection,
  (newVal) => {
    if (newVal) {
      connection.value = { ...newVal };
    }
  },
);

// Update default port when database type changes
watch(
  () => connection.value.databaseType,
  (newType, oldType) => {
    if (newType !== oldType) {
      const oldDefault = defaultPorts[oldType as DatabaseType];
      // Only update port if it was the previous default (user hasn't customized it)
      if (!connection.value.port || connection.value.port === oldDefault) {
        connection.value.port = defaultPorts[newType as DatabaseType];
      }
    }
  },
);

const showPassword = ref(false);

// Computed property to determine if the form is valid
const isValid = computed(() => {
  return (
    !!connection.value.connectionName &&
    !!connection.value.username &&
    (props.isEditing || !!connection.value.password) &&
    !!connection.value.host
  );
});

// Computed property for the submit button text
const submitButtonText = computed(() => {
  if (props.isSubmitting) {
    return "Saving...";
  }
  return props.initialConnection ? "Update Connection" : "Create Connection";
});

// Submit form
const submitForm = () => {
  if (isValid.value) {
    emit("submit", connection.value);
  }
};
</script>
