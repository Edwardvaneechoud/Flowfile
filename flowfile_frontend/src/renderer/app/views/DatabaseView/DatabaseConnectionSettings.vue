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
          <option v-for="dialect in dialects" :key="dialect.name" :value="dialect.name">
            {{ dialect.display_name }}
          </option>
        </select>
      </div>

      <div v-if="dialectAuthMethods.length > 1" class="form-field">
        <label for="auth-method" class="form-label">Authentication Method</label>
        <select id="auth-method" v-model="authMethodModel" class="form-input" required>
          <option v-for="method in dialectAuthMethods" :key="method" :value="method">
            {{ authMethodLabel(method) }}
          </option>
        </select>
      </div>

      <div v-for="field in dialectExtraFields" :key="field.name" class="form-field">
        <label :for="`extra-${field.name}`" class="form-label">{{ field.label }}</label>
        <input
          :id="`extra-${field.name}`"
          :value="extraParamValue(field.name)"
          type="text"
          class="form-input"
          :placeholder="field.label"
          :required="field.required"
          @input="setExtraParam(field.name, ($event.target as HTMLInputElement).value)"
        />
      </div>

      <div v-if="!isFileBasedType && !isHidden('host')" class="form-field">
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

      <div v-if="!isFileBasedType && !isHidden('port')" class="form-field">
        <label for="port" class="form-label">Port</label>
        <input
          id="port"
          v-model="connection.port"
          type="number"
          class="form-input"
          :placeholder="String(defaultPort(connection.databaseType) || 5432)"
        />
      </div>

      <div class="form-field">
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

      <div v-if="!isFileBasedType" class="form-field">
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

      <div v-if="!isFileBasedType && !usesKeyPair && !usesOauth" class="form-field">
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

      <div v-if="usesKeyPair" class="form-field">
        <label for="private-key" class="form-label">Private Key (PEM)</label>
        <textarea
          id="private-key"
          v-model="connection.privateKey"
          class="form-input private-key-input"
          rows="5"
          spellcheck="false"
          :placeholder="
            props.isEditing ? 'Leave blank to keep existing' : '-----BEGIN PRIVATE KEY-----'
          "
          :required="!props.isEditing"
        ></textarea>
      </div>

      <div v-if="usesKeyPair" class="form-field">
        <label for="private-key-passphrase" class="form-label">Key Passphrase (optional)</label>
        <div class="password-field">
          <input
            id="private-key-passphrase"
            v-model="connection.privateKeyPassphrase"
            :type="showPassphrase ? 'text' : 'password'"
            class="form-input"
            :placeholder="props.isEditing ? 'Leave blank to keep existing' : 'Passphrase'"
          />
          <button
            type="button"
            class="toggle-visibility"
            aria-label="Toggle passphrase visibility"
            @click="showPassphrase = !showPassphrase"
          >
            <i :class="showPassphrase ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
          </button>
        </div>
      </div>

      <div v-if="usesOauth" class="form-field">
        <label for="oauth-client-id" class="form-label">OAuth Client ID</label>
        <input
          id="oauth-client-id"
          v-model="connection.oauthClientId"
          type="text"
          class="form-input"
          placeholder="Client id from the security integration / IdP app"
          required
        />
      </div>

      <div v-if="usesOauth" class="form-field">
        <label for="oauth-client-secret" class="form-label">OAuth Client Secret</label>
        <div class="password-field">
          <input
            id="oauth-client-secret"
            v-model="connection.oauthClientSecret"
            :type="showClientSecret ? 'text' : 'password'"
            class="form-input"
            :placeholder="props.isEditing ? 'Leave blank to keep existing' : 'Client secret'"
            :required="!props.isEditing"
          />
          <button
            type="button"
            class="toggle-visibility"
            aria-label="Toggle client secret visibility"
            @click="showClientSecret = !showClientSecret"
          >
            <i :class="showClientSecret ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
          </button>
        </div>
      </div>

      <div v-if="usesOauth" class="form-field">
        <label for="oauth-authorize-endpoint" class="form-label">
          Authorize Endpoint (optional)
        </label>
        <input
          id="oauth-authorize-endpoint"
          v-model="connection.oauthAuthorizeEndpoint"
          type="url"
          class="form-input"
          placeholder="Leave blank to use Snowflake's built-in OAuth"
        />
      </div>

      <div v-if="usesOauth" class="form-field">
        <label for="oauth-token-endpoint" class="form-label">Token Endpoint (optional)</label>
        <input
          id="oauth-token-endpoint"
          v-model="connection.oauthTokenEndpoint"
          type="url"
          class="form-input"
          placeholder="Set both endpoints for an external IdP (Okta, Entra ID, …)"
        />
      </div>

      <div v-if="usesOauth" class="form-field">
        <label for="oauth-redirect-uri" class="form-label">Redirect URI (optional)</label>
        <input
          id="oauth-redirect-uri"
          v-model="connection.oauthRedirectUri"
          type="url"
          class="form-input"
          placeholder="http://localhost:63578/db_connection_lib/oauth/callback"
        />
      </div>

      <div v-if="usesOauth && props.isEditing" class="form-field form-field--full">
        <DbOauthSignInCard
          :connection-name="connection.connectionName"
          :connected="props.oauthConnected"
          @changed="$emit('oauthChanged')"
        />
      </div>

      <div v-if="!isFileBasedType && !isHidden('ssl')" class="form-field">
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
import { ref, computed, defineProps, defineEmits, watch, nextTick } from "vue";
import type { FullDatabaseConnection } from "./databaseConnectionTypes";
import { useDbDialects } from "../../composables/useDbDialects";
import DbOauthSignInCard from "./DbOauthSignInCard.vue";

const props = defineProps<{
  initialConnection?: FullDatabaseConnection;
  isSubmitting?: boolean;
  isEditing?: boolean;
  oauthConnected?: boolean;
}>();

const emit = defineEmits<{
  (e: "submit", connection: FullDatabaseConnection): void;
  (e: "cancel"): void;
  (e: "oauthChanged"): void;
}>();

const { dialects, isFileBased, defaultPort, extraFields, isFieldHidden, authMethods } =
  useDbDialects();

const AUTH_METHOD_LABELS: Record<string, string> = {
  password: "Password",
  key_pair: "Key pair (JWT)",
  oauth: "Single sign-on (OAuth)",
};

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
  authMethod: "password",
  privateKey: "",
  privateKeyPassphrase: "",
  oauthClientId: "",
  oauthClientSecret: "",
  oauthAuthorizeEndpoint: "",
  oauthTokenEndpoint: "",
  oauthRedirectUri: "",
});

const connection = ref<FullDatabaseConnection>(
  props.initialConnection ? { ...props.initialConnection } : defaultConnection(),
);

// The dialect-change watcher must only react to USER type changes: when an edit
// re-seeds the whole connection (the dialog is not destroy-on-close, so the form
// survives across connections), clearing would wipe the just-seeded extraParams.
let seedingConnection = false;

watch(
  () => props.initialConnection,
  (newVal) => {
    if (newVal) {
      seedingConnection = true;
      connection.value = { ...newVal };
      nextTick(() => {
        seedingConnection = false;
      });
    }
  },
);

watch(
  () => connection.value.databaseType,
  (newType, oldType) => {
    if (seedingConnection) {
      return;
    }
    if (newType !== oldType) {
      const newDefault = defaultPort(newType);
      if (isFileBased(newType)) {
        // File-based databases have no port
        connection.value.port = undefined;
      } else {
        const oldDefault = defaultPort(oldType);
        if (!connection.value.port || connection.value.port === oldDefault) {
          connection.value.port = newDefault;
        }
      }
      // Extra params and auth fields are dialect-specific; a stale set must not
      // leak into the new dialect.
      connection.value.extraParams = undefined;
      if (!authMethods(newType).includes(connection.value.authMethod || "password")) {
        connection.value.authMethod = "password";
      }
      connection.value.privateKey = "";
      connection.value.privateKeyPassphrase = "";
      clearOauthFields();
    }
  },
);

const showPassword = ref(false);
const showPassphrase = ref(false);
const showClientSecret = ref(false);

const clearOauthFields = () => {
  connection.value.oauthClientId = "";
  connection.value.oauthClientSecret = "";
  connection.value.oauthAuthorizeEndpoint = "";
  connection.value.oauthTokenEndpoint = "";
  connection.value.oauthRedirectUri = "";
};

const isFileBasedType = computed(() => isFileBased(connection.value.databaseType));

const dialectExtraFields = computed(() => extraFields(connection.value.databaseType));

const dialectAuthMethods = computed(() => authMethods(connection.value.databaseType));

const authMethodModel = computed({
  get: () => connection.value.authMethod || "password",
  set: (value: string) => {
    connection.value.authMethod = value;
    if (value !== "key_pair") {
      // The hidden fields must not keep a pasted key: a stray private_key alongside
      // auth_method="password" would win over the password server-side.
      connection.value.privateKey = "";
      connection.value.privateKeyPassphrase = "";
    }
    if (value !== "oauth") {
      clearOauthFields();
    }
  },
});

const usesKeyPair = computed(() => authMethodModel.value === "key_pair");
const usesOauth = computed(() => authMethodModel.value === "oauth");

const authMethodLabel = (method: string): string => AUTH_METHOD_LABELS[method] ?? method;

const isHidden = (field: string) => isFieldHidden(connection.value.databaseType, field);

const extraParamValue = (name: string): string => connection.value.extraParams?.[name] ?? "";

const setExtraParam = (name: string, value: string) => {
  const params = { ...(connection.value.extraParams || {}) };
  if (value) {
    params[name] = value;
  } else {
    delete params[name];
  }
  connection.value.extraParams = Object.keys(params).length ? params : undefined;
};

const requiredExtraFieldsFilled = computed(() =>
  dialectExtraFields.value.every((f) => !f.required || !!connection.value.extraParams?.[f.name]),
);

const isValid = computed(() => {
  if (isFileBasedType.value) {
    return !!connection.value.connectionName && !!connection.value.database;
  }
  const credentialFilled = usesKeyPair.value
    ? props.isEditing || !!connection.value.privateKey
    : usesOauth.value
      ? !!connection.value.oauthClientId &&
        (props.isEditing || !!connection.value.oauthClientSecret)
      : props.isEditing || !!connection.value.password;
  return (
    !!connection.value.connectionName &&
    !!connection.value.username &&
    credentialFilled &&
    (isHidden("host") || !!connection.value.host) &&
    requiredExtraFieldsFilled.value
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
    emit("submit", connection.value);
  }
};
</script>

<style scoped>
.form-field--full {
  grid-column: 1 / -1;
}

.private-key-input {
  font-family: monospace;
  resize: vertical;
}
</style>
