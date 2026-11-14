//flowfile_frontend/src/renderer/app/views/DatabaseView/DatabaseConnectionSettings.vue

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
          <option v-for="dialect in dialects" :key="dialect.name" :value="dialect.name">
            {{ dialect.display_name }}
          </option>
          <option
            v-for="dialect in missingDialects"
            :key="dialect.name"
            :value="dialect.name"
            disabled
          >
            {{ notInstalledLabel(dialect) }}
          </option>
        </select>
        <div
          v-for="note in installNotes"
          :key="note"
          class="form-hint"
          data-testid="dialect-install-hint"
        >
          {{ note }}
        </div>
      </div>

      <div v-if="showServerFields" class="form-field">
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

      <div v-if="showServerFields" class="form-field">
        <label for="port" class="form-label">Port</label>
        <input
          id="port"
          v-model="connection.port"
          type="number"
          class="form-input"
          :placeholder="String(defaultPort(connection.databaseType) || 5432)"
        />
      </div>

      <div v-if="!isSnowflake" class="form-field">
        <label for="database" class="form-label">{{
          isFileBasedType ? "File Path" : "Database"
        }}</label>
        <input
          id="database"
          v-model="connection.database"
          type="text"
          class="form-input"
          :placeholder="isFileBasedType ? '/path/to/database.db' : 'Database name'"
        />
      </div>

      <div v-if="showServerFields" class="form-field">
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

      <div v-if="showServerFields" class="form-field">
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

      <div v-if="showServerFields" class="form-field">
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

      <SnowflakeConnectionFields
        v-if="isSnowflake"
        v-model="connection"
        :is-editing="props.isEditing"
        :stored-auth-method="props.initialConnection?.authMethod"
        :stored-host="props.initialConnection?.host"
        :stored-role="props.initialConnection?.role"
        :auth-methods="authMethods(connection.databaseType)"
      />
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
import { ref, computed, defineProps, defineEmits, watch, onMounted } from "vue";
import type { FullDatabaseConnection } from "./databaseConnectionTypes";
import { useDbDialects } from "../../composables/useDbDialects";
import {
  type DbDialectInfo,
  getUnavailableDbDialects,
  installNote,
  notInstalledLabel,
} from "../../api/dbDialects";
import SnowflakeConnectionFields from "./SnowflakeConnectionFields.vue";
import {
  SNOWFLAKE,
  buildPayloadFields,
  isSnowflakeFormValid,
  snowflakeResetFields,
} from "./snowflakeConnection";

const props = defineProps<{
  initialConnection?: FullDatabaseConnection;
  isSubmitting?: boolean;
  isEditing?: boolean;
}>();

const emit = defineEmits<{
  (e: "submit", connection: FullDatabaseConnection): void;
  (e: "cancel"): void;
}>();

const { dialects, isFileBased, defaultPort, authMethods } = useDbDialects();

const unavailableDialects = ref<DbDialectInfo[]>([]);
onMounted(async () => {
  unavailableDialects.value = await getUnavailableDbDialects();
});
const missingDialects = computed(() =>
  unavailableDialects.value.filter(
    (missing) => !dialects.value.some((dialect) => dialect.name === missing.name),
  ),
);
const installNotes = computed(() =>
  missingDialects.value.map(installNote).filter((note): note is string => note !== null),
);

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

const connection = ref<FullDatabaseConnection>(
  props.initialConnection ? { ...props.initialConnection } : defaultConnection(),
);

watch(
  () => props.initialConnection,
  (newVal) => {
    if (newVal) {
      connection.value = { ...newVal };
    }
  },
);

watch(
  () => [connection.value, connection.value.databaseType] as const,
  ([current, newType], [previous, oldType]) => {
    // A replaced connection (an edit loading) keeps its fields.
    if (current !== previous || newType === oldType) return;
    if (newType === SNOWFLAKE || oldType === SNOWFLAKE) {
      Object.assign(connection.value, snowflakeResetFields());
    }
    if (isFileBased(newType) || newType === SNOWFLAKE) {
      connection.value.port = undefined;
    } else {
      const oldDefault = defaultPort(oldType);
      if (!connection.value.port || connection.value.port === oldDefault) {
        connection.value.port = defaultPort(newType);
      }
    }
  },
);

const showPassword = ref(false);

const isFileBasedType = computed(() => isFileBased(connection.value.databaseType));
const isSnowflake = computed(() => connection.value.databaseType === SNOWFLAKE);
const showServerFields = computed(() => !isFileBasedType.value && !isSnowflake.value);

const isValid = computed(() => {
  if (isSnowflake.value) {
    return isSnowflakeFormValid(connection.value, {
      isEditing: !!props.isEditing,
      storedAuthMethod: props.initialConnection?.authMethod,
      storedHost: props.initialConnection?.host,
      storedRole: props.initialConnection?.role,
    });
  }
  if (isFileBasedType.value) {
    return !!connection.value.connectionName && !!connection.value.database;
  }
  return (
    !!connection.value.connectionName &&
    !!connection.value.username &&
    (props.isEditing || !!connection.value.password) &&
    !!connection.value.host
  );
});

const submitButtonText = computed(() => {
  if (props.isSubmitting) {
    return "Saving...";
  }
  return props.initialConnection ? "Update Connection" : "Create Connection";
});

const submitForm = () => {
  if (isValid.value) {
    emit(
      "submit",
      isSnowflake.value
        ? { ...connection.value, ...buildPayloadFields(connection.value) }
        : connection.value,
    );
  }
};
</script>
