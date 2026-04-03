<template>
  <form class="form" @submit.prevent="submitForm">
    <div class="form-grid">
      <!-- Connection Name -->
      <div class="form-field">
        <label for="connection-name" class="form-label">Connection Name</label>
        <input
          id="connection-name"
          v-model="connection.connection_name"
          type="text"
          class="form-input"
          placeholder="my_kafka_cluster"
          required
          :disabled="props.isEditing"
        />
      </div>

      <!-- Bootstrap Servers -->
      <div class="form-field">
        <label for="bootstrap-servers" class="form-label">Bootstrap Servers</label>
        <input
          id="bootstrap-servers"
          v-model="connection.bootstrap_servers"
          type="text"
          class="form-input"
          placeholder="localhost:9092"
          required
        />
      </div>

      <!-- Security Protocol -->
      <div class="form-field">
        <label for="security-protocol" class="form-label">Security Protocol</label>
        <select
          id="security-protocol"
          v-model="connection.security_protocol"
          class="form-input"
          required
        >
          <option value="PLAINTEXT">PLAINTEXT</option>
          <option value="SSL">SSL</option>
          <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
          <option value="SASL_SSL">SASL_SSL</option>
        </select>
      </div>

      <!-- SASL Auth Fields -->
      <template v-if="showSaslFields">
        <div class="form-field">
          <label for="sasl-mechanism" class="form-label">SASL Mechanism</label>
          <select id="sasl-mechanism" v-model="connection.sasl_mechanism" class="form-input">
            <option value="PLAIN">PLAIN</option>
            <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
            <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
          </select>
        </div>

        <div class="form-field">
          <label for="sasl-username" class="form-label">SASL Username</label>
          <input
            id="sasl-username"
            v-model="connection.sasl_username"
            type="text"
            class="form-input"
            placeholder="username"
          />
        </div>

        <div class="form-field">
          <label for="sasl-password" class="form-label">SASL Password</label>
          <div class="password-field">
            <input
              id="sasl-password"
              v-model="connection.sasl_password"
              :type="showSaslPassword ? 'text' : 'password'"
              class="form-input"
              :placeholder="props.isEditing ? 'Leave blank to keep existing' : 'password'"
            />
            <button
              type="button"
              class="toggle-visibility"
              aria-label="Toggle SASL password visibility"
              @click="showSaslPassword = !showSaslPassword"
            >
              <i :class="showSaslPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
        </div>
      </template>

      <!-- SSL Fields -->
      <template v-if="showSslFields">
        <div class="form-field">
          <label for="ssl-ca-location" class="form-label">SSL CA Location (Optional)</label>
          <input
            id="ssl-ca-location"
            v-model="connection.ssl_ca_location"
            type="text"
            class="form-input"
            placeholder="/path/to/ca-cert.pem"
          />
        </div>

        <div class="form-field">
          <label for="ssl-cert-location" class="form-label"
            >SSL Certificate Location (Optional)</label
          >
          <input
            id="ssl-cert-location"
            v-model="connection.ssl_cert_location"
            type="text"
            class="form-input"
            placeholder="/path/to/client-cert.pem"
          />
        </div>

        <div class="form-field">
          <label for="ssl-key-pem" class="form-label">SSL Key PEM (Optional)</label>
          <div class="password-field">
            <input
              id="ssl-key-pem"
              v-model="connection.ssl_key_pem"
              :type="showSslKey ? 'text' : 'password'"
              class="form-input"
              :placeholder="props.isEditing ? 'Leave blank to keep existing' : 'PEM-encoded key'"
            />
            <button
              type="button"
              class="toggle-visibility"
              aria-label="Toggle SSL key visibility"
              @click="showSslKey = !showSslKey"
            >
              <i :class="showSslKey ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
        </div>
      </template>

      <!-- Schema Registry URL -->
      <div class="form-field">
        <label for="schema-registry-url" class="form-label">Schema Registry URL (Optional)</label>
        <input
          id="schema-registry-url"
          v-model="connection.schema_registry_url"
          type="text"
          class="form-input"
          placeholder="http://localhost:8081"
        />
      </div>

      <!-- Extra Config -->
      <div class="form-field">
        <label class="form-label">Extra Configuration (Optional)</label>
        <div class="extra-config-list">
          <div v-for="(_, index) in extraConfigEntries" :key="index" class="extra-config-row">
            <input
              v-model="extraConfigEntries[index].key"
              type="text"
              class="form-input extra-config-input"
              placeholder="config.key"
            />
            <input
              v-model="extraConfigEntries[index].value"
              type="text"
              class="form-input extra-config-input"
              placeholder="value"
            />
            <button
              type="button"
              class="btn btn-icon btn-danger-subtle"
              @click="removeExtraConfigEntry(index)"
            >
              <i class="fa-solid fa-times"></i>
            </button>
          </div>
          <button type="button" class="btn btn-secondary btn-sm" @click="addExtraConfigEntry">
            <i class="fa-solid fa-plus"></i> Add Config
          </button>
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
import { ref, computed, watch } from "vue";
import type { KafkaConnectionCreate, KafkaSecurityProtocol } from "./KafkaConnectionTypes";

const props = defineProps<{
  initialConnection?: KafkaConnectionCreate;
  isSubmitting?: boolean;
  isEditing?: boolean;
}>();

const emit = defineEmits<{
  (e: "submit", connection: KafkaConnectionCreate): void;
  (e: "cancel"): void;
}>();

const defaultConnection = (): KafkaConnectionCreate => ({
  connection_name: "",
  bootstrap_servers: "",
  security_protocol: "PLAINTEXT",
  sasl_mechanism: null,
  sasl_username: null,
  sasl_password: null,
  ssl_ca_location: null,
  ssl_cert_location: null,
  ssl_key_pem: null,
  schema_registry_url: null,
  extra_config: null,
});

const connection = ref<KafkaConnectionCreate>(
  props.initialConnection ? { ...props.initialConnection } : defaultConnection(),
);

watch(
  () => props.initialConnection,
  (newVal) => {
    if (newVal) {
      connection.value = { ...newVal };
      syncExtraConfigEntries();
    }
  },
);

// Password visibility toggles
const showSaslPassword = ref(false);
const showSslKey = ref(false);

// Computed visibility flags
const showSaslFields = computed(() => {
  const protocol = connection.value.security_protocol;
  return protocol === "SASL_PLAINTEXT" || protocol === "SASL_SSL";
});

const showSslFields = computed(() => {
  const protocol = connection.value.security_protocol;
  return protocol === "SSL" || protocol === "SASL_SSL";
});

// Extra config key-value entries
const extraConfigEntries = ref<{ key: string; value: string }[]>([]);

const syncExtraConfigEntries = () => {
  if (connection.value.extra_config) {
    extraConfigEntries.value = Object.entries(connection.value.extra_config).map(
      ([key, value]) => ({
        key,
        value,
      }),
    );
  } else {
    extraConfigEntries.value = [];
  }
};

syncExtraConfigEntries();

const addExtraConfigEntry = () => {
  extraConfigEntries.value.push({ key: "", value: "" });
};

const removeExtraConfigEntry = (index: number) => {
  extraConfigEntries.value.splice(index, 1);
};

// Sync extra config entries back to connection on submit
const syncExtraConfigToConnection = () => {
  const validEntries = extraConfigEntries.value.filter((e) => e.key.trim() !== "");
  if (validEntries.length > 0) {
    connection.value.extra_config = Object.fromEntries(validEntries.map((e) => [e.key, e.value]));
  } else {
    connection.value.extra_config = null;
  }
};

// Set SASL defaults when protocol changes to SASL
watch(
  () => connection.value.security_protocol,
  (newProtocol: KafkaSecurityProtocol) => {
    if (newProtocol === "SASL_PLAINTEXT" || newProtocol === "SASL_SSL") {
      if (!connection.value.sasl_mechanism) {
        connection.value.sasl_mechanism = "PLAIN";
      }
    } else {
      connection.value.sasl_mechanism = null;
      connection.value.sasl_username = null;
      connection.value.sasl_password = null;
    }
    if (newProtocol !== "SSL" && newProtocol !== "SASL_SSL") {
      connection.value.ssl_ca_location = null;
      connection.value.ssl_cert_location = null;
      connection.value.ssl_key_pem = null;
    }
  },
);

const isValid = computed(() => {
  return !!connection.value.connection_name && !!connection.value.bootstrap_servers;
});

const submitButtonText = computed(() => {
  if (props.isSubmitting) {
    return "Saving...";
  }
  return props.isEditing ? "Update Connection" : "Create Connection";
});

const submitForm = () => {
  if (isValid.value) {
    syncExtraConfigToConnection();
    emit("submit", connection.value);
  }
};
</script>

<style scoped>
.extra-config-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.extra-config-row {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.extra-config-input {
  flex: 1;
}

.btn-icon {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 2rem;
  height: 2rem;
  padding: 0;
  border-radius: var(--border-radius-md);
  flex-shrink: 0;
}

.btn-danger-subtle {
  background: none;
  border: 1px solid var(--color-danger, #ef4444);
  color: var(--color-danger, #ef4444);
  cursor: pointer;
}

.btn-danger-subtle:hover {
  background-color: var(--color-danger, #ef4444);
  color: white;
}

.btn-sm {
  font-size: var(--font-size-xs, 0.75rem);
  padding: 0.25rem 0.5rem;
}
</style>
