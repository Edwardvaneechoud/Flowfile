<template>
  <form class="form" @submit.prevent="submitForm">
    <div class="form-grid">
      <!-- Connection Name -->
      <div class="form-field">
        <label for="connection-name" class="form-label">Connection Name</label>
        <input
          id="connection-name"
          v-model="connection.connectionName"
          type="text"
          class="form-input"
          placeholder="my_cloud_storage"
          required
        />
      </div>

      <!-- Storage Type -->
      <div class="form-field">
        <label for="storage-type" class="form-label">Storage Type</label>
        <select id="storage-type" v-model="connection.storageType" class="form-input" required>
          <option value="s3">AWS S3</option>
          <option value="adls">Azure Data Lake Storage</option>
        </select>
      </div>

      <!-- Authentication Method -->
      <div class="form-field">
        <label for="auth-method" class="form-label">Authentication Method</label>
        <select id="auth-method" v-model="connection.authMethod" class="form-input" required>
          <option v-for="method in availableAuthMethods" :key="method.value" :value="method.value">
            {{ method.label }}
          </option>
        </select>
      </div>

      <!-- AWS S3 Fields -->
      <template v-if="connection.storageType === 's3'">
        <!-- AWS Region -->
        <div class="form-field">
          <label for="aws-region" class="form-label">AWS Region</label>
          <input
            id="aws-region"
            v-model="connection.awsRegion"
            type="text"
            class="form-input"
            placeholder="us-east-1"
            :required="connection.storageType === 's3'"
          />
        </div>

        <!-- AWS Access Key ID (for access_key auth) -->
        <div v-if="connection.authMethod === 'access_key'" class="form-field">
          <label for="aws-access-key-id" class="form-label">AWS Access Key ID</label>
          <input
            id="aws-access-key-id"
            v-model="connection.awsAccessKeyId"
            type="text"
            class="form-input"
            placeholder="AKIAIOSFODNN7EXAMPLE"
            :required="connection.authMethod === 'access_key'"
          />
        </div>

        <!-- AWS Secret Access Key (for access_key auth) -->
        <div v-if="connection.authMethod === 'access_key'" class="form-field">
          <label for="aws-secret-access-key" class="form-label">AWS Secret Access Key</label>
          <div class="password-field">
            <input
              id="aws-secret-access-key"
              v-model="connection.awsSecretAccessKey"
              :type="showAwsSecret ? 'text' : 'password'"
              class="form-input"
              placeholder="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
              :required="connection.authMethod === 'access_key'"
            />
            <button
              type="button"
              class="toggle-visibility"
              aria-label="Toggle AWS secret visibility"
              @click="showAwsSecret = !showAwsSecret"
            >
              <i :class="showAwsSecret ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
        </div>

        <!-- AWS Role ARN (for iam_role auth) -->
        <div v-if="connection.authMethod === 'iam_role'" class="form-field">
          <label for="aws-role-arn" class="form-label">AWS Role ARN</label>
          <input
            id="aws-role-arn"
            v-model="connection.awsRoleArn"
            type="text"
            class="form-input"
            placeholder="arn:aws:iam::123456789012:role/MyRole"
            :required="connection.authMethod === 'iam_role'"
          />
        </div>

        <!-- AWS Allow Unsafe HTML -->
        <div class="form-field">
          <div class="checkbox-container">
            <input
              id="aws-allow-unsafe-html"
              v-model="connection.awsAllowUnsafeHtml"
              type="checkbox"
              class="checkbox-input"
            />
            <label for="aws-allow-unsafe-html" class="form-label">Allow Unsafe HTML</label>
          </div>
        </div>
      </template>

      <!-- Azure ADLS Fields -->
      <template v-if="connection.storageType === 'adls'">
        <!-- Azure Account Name -->
        <div class="form-field">
          <label for="azure-account-name" class="form-label">Azure Account Name</label>
          <input
            id="azure-account-name"
            v-model="connection.azureAccountName"
            type="text"
            class="form-input"
            placeholder="mystorageaccount"
            :required="connection.storageType === 'adls'"
          />
        </div>

        <!-- Azure Account Key (for access_key auth) -->
        <div v-if="connection.authMethod === 'access_key'" class="form-field">
          <label for="azure-account-key" class="form-label">Azure Account Key</label>
          <div class="password-field">
            <input
              id="azure-account-key"
              v-model="connection.azureAccountKey"
              :type="showAzureKey ? 'text' : 'password'"
              class="form-input"
              placeholder="Account key"
              :required="connection.authMethod === 'access_key'"
            />
            <button
              type="button"
              class="toggle-visibility"
              aria-label="Toggle Azure key visibility"
              @click="showAzureKey = !showAzureKey"
            >
              <i :class="showAzureKey ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
        </div>

        <!-- Azure Service Principal Fields (for service_principal auth) -->
        <template v-if="connection.authMethod === 'service_principal'">
          <!-- Azure Tenant ID -->
          <div class="form-field">
            <label for="azure-tenant-id" class="form-label">Azure Tenant ID</label>
            <input
              id="azure-tenant-id"
              v-model="connection.azureTenantId"
              type="text"
              class="form-input"
              placeholder="12345678-1234-1234-1234-123456789012"
              :required="connection.authMethod === 'service_principal'"
            />
          </div>

          <!-- Azure Client ID -->
          <div class="form-field">
            <label for="azure-client-id" class="form-label">Azure Client ID</label>
            <input
              id="azure-client-id"
              v-model="connection.azureClientId"
              type="text"
              class="form-input"
              placeholder="12345678-1234-1234-1234-123456789012"
              :required="connection.authMethod === 'service_principal'"
            />
          </div>

          <!-- Azure Client Secret -->
          <div class="form-field">
            <label for="azure-client-secret" class="form-label">Azure Client Secret</label>
            <div class="password-field">
              <input
                id="azure-client-secret"
                v-model="connection.azureClientSecret"
                :type="showAzureSecret ? 'text' : 'password'"
                class="form-input"
                placeholder="Client secret"
                :required="connection.authMethod === 'service_principal'"
              />
              <button
                type="button"
                class="toggle-visibility"
                aria-label="Toggle Azure secret visibility"
                @click="showAzureSecret = !showAzureSecret"
              >
                <i :class="showAzureSecret ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
              </button>
            </div>
          </div>
        </template>
      </template>

      <!-- Common Fields -->
      <div class="form-field">
        <label for="endpoint-url" class="form-label">Custom Endpoint URL (Optional)</label>
        <input
          id="endpoint-url"
          v-model="connection.endpointUrl"
          type="text"
          class="form-input"
          placeholder="https://custom-endpoint.example.com"
        />
      </div>

      <div class="form-field">
        <div class="checkbox-container">
          <input
            id="verify-ssl"
            v-model="connection.verifySsl"
            type="checkbox"
            class="checkbox-input"
          />
          <label for="verify-ssl" class="form-label">Verify SSL</label>
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
import type {
  FullCloudStorageConnection,
  CloudStorageType,
  AuthMethod,
} from "./CloudConnectionTypes";

const props = defineProps<{
  initialConnection?: FullCloudStorageConnection;
  isSubmitting?: boolean;
}>();

const emit = defineEmits<{
  (e: "submit", connection: FullCloudStorageConnection): void;
  (e: "cancel"): void;
}>();

// Authentication methods available for each storage type
const authMethodsByStorageType = {
  s3: [
    { value: "access_key", label: "Access Key" },
    { value: "iam_role", label: "IAM Role" },
    { value: "aws-cli", label: "AWS CLI" },
    { value: "auto", label: "Auto" },
  ],
  adls: [
    { value: "access_key", label: "Access Key" },
    { value: "service_principal", label: "Service Principal" },
    { value: "managed_identity", label: "Managed Identity" },
    { value: "sas_token", label: "SAS Token" },
    { value: "auto", label: "Auto" },
  ],
};

// Create a default connection object
const defaultConnection = (): FullCloudStorageConnection => ({
  connectionName: "",
  storageType: "s3",
  authMethod: "access_key",
  verifySsl: true,
  awsAllowUnsafeHtml: false,
});

// Initialize connection with props or default values
const connection = ref<FullCloudStorageConnection>(
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

// Password visibility toggles
const showAwsSecret = ref(false);
const showAzureKey = ref(false);
const showAzureSecret = ref(false);

// Computed property for available auth methods based on storage type
const availableAuthMethods = computed(() => {
  const cloudStorageType = connection.value.storageType as CloudStorageType;
  return authMethodsByStorageType[cloudStorageType] || [];
});

// Reset auth method when storage type changes
watch(
  () => connection.value.storageType,
  (newStorageType) => {
    const methods = authMethodsByStorageType[newStorageType as CloudStorageType];
    if (methods && methods.length > 0) {
      // If current auth method is not available for new storage type, reset to first option
      const currentMethodAvailable = methods.some((m) => m.value === connection.value.authMethod);
      if (!currentMethodAvailable) {
        connection.value.authMethod = methods[0].value as AuthMethod;
      }
    }
  },
);

// Computed property to determine if the form is valid
const isValid = computed(() => {
  const baseValid =
    !!connection.value.connectionName &&
    !!connection.value.storageType &&
    !!connection.value.authMethod;

  if (!baseValid) return false;

  // AWS S3 validation
  if (connection.value.storageType === "s3") {
    if (!connection.value.awsRegion) return false;

    if (connection.value.authMethod === "access_key") {
      return !!connection.value.awsAccessKeyId && !!connection.value.awsSecretAccessKey;
    } else if (connection.value.authMethod === "iam_role") {
      return !!connection.value.awsRoleArn;
    }
  }

  // Azure ADLS validation
  if (connection.value.storageType === "adls") {
    if (!connection.value.azureAccountName) return false;

    if (connection.value.authMethod === "access_key") {
      return !!connection.value.azureAccountKey;
    } else if (connection.value.authMethod === "service_principal") {
      return (
        !!connection.value.azureTenantId &&
        !!connection.value.azureClientId &&
        !!connection.value.azureClientSecret
      );
    }
  }

  return true;
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
