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
      <div class="hint-text">
        Optional default property ID. Nodes can override this value.
      </div>
    </el-form-item>

    <el-form-item :label="isEditing ? 'Service Account JSON (leave blank to keep existing)' : 'Service Account JSON'">
      <div class="sa-input-row">
        <el-input
          v-model="form.serviceAccountJson"
          type="textarea"
          :rows="8"
          :placeholder="saPlaceholder"
          :show-password="!showKey"
          class="mono"
        />
        <div class="sa-actions">
          <el-button size="small" @click="triggerFilePicker">
            <i class="fa-solid fa-upload"></i>&nbsp;Upload JSON
          </el-button>
          <el-button size="small" @click="showKey = !showKey">
            <i :class="showKey ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            &nbsp;{{ showKey ? "Hide" : "Show" }}
          </el-button>
          <el-button
            size="small"
            :disabled="!form.serviceAccountJson"
            :loading="isTesting"
            @click="handleTest"
          >
            <i class="fa-solid fa-plug"></i>&nbsp;Test
          </el-button>
        </div>
      </div>
      <input
        ref="fileInput"
        type="file"
        accept=".json,application/json"
        style="display: none"
        @change="handleFileUpload"
      />
      <div class="hint-text">
        Paste the full contents of a GA4 service-account JSON key. It is encrypted at
        rest with your user-derived key and never sent back to the browser.
      </div>
    </el-form-item>

    <div class="form-actions">
      <el-button @click="$emit('cancel')">Cancel</el-button>
      <el-button type="primary" :loading="isSubmitting" @click="handleSubmit">
        {{ isEditing ? "Update" : "Create" }}
      </el-button>
    </div>
  </el-form>
</template>

<script setup lang="ts">
import { ref, reactive, watch } from "vue";
import { ElForm, ElFormItem, ElInput, ElButton, ElMessage } from "element-plus";
import type { GoogleAnalyticsConnection } from "./GoogleAnalyticsConnectionTypes";
import { testGoogleAnalyticsConnection } from "./api";

const props = defineProps<{
  initialConnection?: GoogleAnalyticsConnection;
  isEditing: boolean;
  isSubmitting: boolean;
}>();

const emit = defineEmits<{
  (e: "submit", connection: GoogleAnalyticsConnection): void;
  (e: "cancel"): void;
}>();

const form = reactive<GoogleAnalyticsConnection>({
  connectionName: "",
  description: "",
  defaultPropertyId: "",
  serviceAccountJson: "",
});

const showKey = ref(false);
const isTesting = ref(false);
const fileInput = ref<HTMLInputElement | null>(null);

const saPlaceholder = `{"type": "service_account", "project_id": "...", ...}`;

watch(
  () => props.initialConnection,
  (value) => {
    if (value) {
      form.connectionName = value.connectionName;
      form.description = value.description ?? "";
      form.defaultPropertyId = value.defaultPropertyId ?? "";
      form.serviceAccountJson = "";
    } else {
      form.connectionName = "";
      form.description = "";
      form.defaultPropertyId = "";
      form.serviceAccountJson = "";
    }
  },
  { immediate: true },
);

function triggerFilePicker() {
  fileInput.value?.click();
}

function handleFileUpload(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    form.serviceAccountJson = String(reader.result ?? "");
  };
  reader.onerror = () => ElMessage.error("Could not read file");
  reader.readAsText(file);
  input.value = "";
}

async function handleTest() {
  if (!form.serviceAccountJson) return;
  isTesting.value = true;
  try {
    const result = await testGoogleAnalyticsConnection(form.serviceAccountJson);
    if (result.success) {
      ElMessage.success(result.message);
    } else {
      ElMessage.error(result.message);
    }
  } finally {
    isTesting.value = false;
  }
}

function handleSubmit() {
  if (!form.connectionName.trim()) {
    ElMessage.error("Connection name is required");
    return;
  }
  if (!props.isEditing && !form.serviceAccountJson?.trim()) {
    ElMessage.error("Service account JSON is required");
    return;
  }
  emit("submit", { ...form });
}
</script>

<style scoped>
.hint-text {
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  margin-top: var(--spacing-1);
}

.sa-input-row {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.sa-actions {
  display: flex;
  gap: var(--spacing-2);
}

.mono :deep(.el-textarea__inner) {
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-xs);
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  margin-top: var(--spacing-4);
}
</style>
