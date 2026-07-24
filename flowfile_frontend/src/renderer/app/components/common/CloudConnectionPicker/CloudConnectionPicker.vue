<template>
  <div class="form-group">
    <label for="connection-select">{{ label }}</label>
    <div v-if="loading" class="loading-state">
      <div class="loading-spinner" />
      <p>Loading connections...</p>
    </div>
    <div v-else>
      <select
        id="connection-select"
        :value="connectionSelectValue(modelValue, unavailableConnection)"
        class="form-control"
        @change="onChange"
      >
        <option :value="NO_CONNECTION_VALUE">{{ noConnectionLabel }}</option>
        <option v-if="unavailableConnection" :value="unavailableConnection" disabled>
          {{ unavailableConnection }} (unavailable)
        </option>
        <option v-for="conn in connections" :key="conn.connectionName" :value="conn.connectionName">
          {{ conn.connectionName }} ({{ getStorageTypeLabel(conn.storageType) }} -
          {{ getAuthMethodLabel(conn.authMethod) }})
        </option>
      </select>
      <div v-if="unavailableConnection" class="warning-text">
        <i class="fa-solid fa-triangle-exclamation" />
        Connection "{{ unavailableConnection }}" is no longer available. Pick another connection, or
        this node will fail when it runs.
      </div>
      <div v-else-if="!modelValue" class="helper-text">
        <i class="fa-solid fa-info-circle" />
        {{ helperText }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { FullCloudStorageConnectionInterface } from "../../../views/CloudConnectionView/CloudConnectionTypes";
import {
  getAuthMethodLabel,
  getStorageTypeLabel,
} from "../../../views/CloudConnectionView/cloudConnectionFormatters";
import { NO_CONNECTION_VALUE, connectionSelectValue, resolveConnection } from "./connectionOptions";

const props = withDefaults(
  defineProps<{
    modelValue: FullCloudStorageConnectionInterface | null;
    connections: FullCloudStorageConnectionInterface[];
    unavailableConnection?: string | null;
    loading?: boolean;
    label?: string;
    noConnectionLabel?: string;
    helperText?: string;
  }>(),
  {
    unavailableConnection: null,
    loading: false,
    label: "Cloud Storage Connection",
    noConnectionLabel: "No connection (use local credentials)",
    helperText: "Will use local AWS CLI credentials or environment variables",
  },
);

const emit = defineEmits(["update:modelValue", "change"]);

function onChange(event: Event) {
  const value = resolveConnection(props.connections, (event.target as HTMLSelectElement).value);
  emit("update:modelValue", value);
  emit("change", value);
}
</script>

<style scoped>
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

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
}

select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234a5568' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}

.helper-text {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
  font-size: 0.8125rem;
  color: #718096;
}

.helper-text i {
  color: #4299e1;
  font-size: 0.875rem;
}

.warning-text {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
  font-size: 0.8125rem;
  color: #b7791f;
}

.warning-text i {
  color: #d69e2e;
  font-size: 0.875rem;
}

.loading-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.5rem;
  padding: 1rem;
}

.loading-state p {
  margin: 0;
  color: #718096;
  font-size: 0.875rem;
}

.loading-spinner {
  width: 2rem;
  height: 2rem;
  border: 2px solid #e2e8f0;
  border-top-color: #4299e1;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}
</style>
