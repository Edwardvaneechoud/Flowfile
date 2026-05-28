<template>
  <el-form :model="form" label-position="top" @submit.prevent>
    <el-form-item label="Connection Name" required>
      <el-input
        v-model="form.connectionName"
        placeholder="e.g. marketing-ga4"
        :disabled="isEditing"
      />
      <div class="hint-text">A unique name used to reference this connection from nodes.</div>
    </el-form-item>

    <el-form-item label="Description">
      <el-input v-model="form.description" placeholder="Optional description" />
    </el-form-item>

    <el-form-item label="Default GA4 Property ID">
      <el-input v-model="form.defaultPropertyId" placeholder="e.g. 123456789" />
      <div class="hint-text">Optional default property ID. Nodes can override this value.</div>
    </el-form-item>

    <el-form-item label="Google Account">
      <div v-if="isEditing && initialConnection?.oauthUserEmail" class="connected-row">
        <span class="connected-pill">
          <i class="fa-solid fa-circle-check"></i>
          Connected as {{ initialConnection.oauthUserEmail }}
        </span>
        <el-button size="small" :loading="isConnecting" @click="handleConnect">
          <i class="fa-solid fa-rotate-right"></i>&nbsp;Reconnect
        </el-button>
      </div>
      <div v-else>
        <el-button
          type="primary"
          :loading="isConnecting"
          :disabled="!form.connectionName.trim()"
          @click="handleConnect"
        >
          <i class="fa-brands fa-google"></i>&nbsp;Connect Google Account
        </el-button>
        <div class="hint-text">
          Opens Google sign-in in a new window. Flowfile stores a refresh token (encrypted at rest)
          so it can read GA4 data on your behalf — no service-account key is required.
        </div>
      </div>
    </el-form-item>

    <div class="form-actions">
      <el-button @click="$emit('cancel')">Cancel</el-button>
      <el-button
        v-if="isEditing"
        type="primary"
        :loading="isSubmitting"
        @click="handleSaveMetadata"
      >
        Save
      </el-button>
    </div>
  </el-form>
</template>

<script setup lang="ts">
import { reactive, watch } from "vue";
import { ElButton, ElForm, ElFormItem, ElInput, ElMessage } from "element-plus";
import type {
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionMetadata,
} from "./GoogleAnalyticsConnectionTypes";

const props = defineProps<{
  initialConnection?: GoogleAnalyticsConnectionInterface;
  isEditing: boolean;
  isSubmitting: boolean;
  isConnecting: boolean;
}>();

const emit = defineEmits<{
  (e: "save-metadata", metadata: GoogleAnalyticsConnectionMetadata): void;
  (e: "connect-oauth", metadata: GoogleAnalyticsConnectionMetadata): void;
  (e: "cancel"): void;
}>();

const form = reactive<GoogleAnalyticsConnectionMetadata>({
  connectionName: "",
  description: "",
  defaultPropertyId: "",
});

watch(
  () => props.initialConnection,
  (value) => {
    form.connectionName = value?.connectionName ?? "";
    form.description = value?.description ?? "";
    form.defaultPropertyId = value?.defaultPropertyId ?? "";
  },
  { immediate: true },
);

function handleConnect() {
  if (!form.connectionName.trim()) {
    ElMessage.error("Connection name is required");
    return;
  }
  emit("connect-oauth", { ...form });
}

function handleSaveMetadata() {
  if (!form.connectionName.trim()) {
    ElMessage.error("Connection name is required");
    return;
  }
  emit("save-metadata", { ...form });
}
</script>

<style scoped>
.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  margin-top: var(--spacing-1);
}

.connected-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  flex-wrap: wrap;
}

.connected-pill {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-3);
  background: var(--color-accent-subtle, #ebf4ff);
  color: var(--color-accent, #2b6cb0);
  border-radius: var(--border-radius-full, 9999px);
  font-size: var(--font-size-sm);
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  margin-top: var(--spacing-4);
}
</style>
