<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-card">
      <div class="modal-header">
        <h3>Create Kafka Sync</h3>
        <button class="close-btn" @click="$emit('close')">
          <i class="fa-solid fa-xmark"></i>
        </button>
      </div>
      <div class="modal-body">
        <p class="modal-description">
          Create a flow that streams data from a Kafka topic into a catalog table. You can further
          configure the Kafka source settings by opening the flow in the designer.
        </p>

        <div class="form-group">
          <label class="field-label">Sync name</label>
          <input v-model="syncName" class="input-field" placeholder="e.g. orders-sync" />
          <span v-if="syncName" class="flow-name-hint">
            Flow will be named: <code>sync-{{ syncName }}</code>
          </span>
        </div>

        <div class="form-group">
          <label class="field-label">Kafka connection</label>
          <select
            v-model="selectedConnectionId"
            class="input-field"
            @change="handleConnectionChange"
          >
            <option :value="null">Select a connection...</option>
            <option v-for="conn in kafkaConnections" :key="conn.id" :value="conn.id">
              {{ conn.connection_name }} ({{ conn.bootstrap_servers }})
            </option>
          </select>
          <span v-if="kafkaConnections.length === 0 && !loadingConnections" class="ns-hint">
            No Kafka connections available. Set one up in the Kafka Connection Manager first.
          </span>
        </div>

        <div class="form-group">
          <label class="field-label">Topic</label>
          <div class="topic-row">
            <select v-if="availableTopics.length > 0" v-model="topicName" class="input-field">
              <option value="">Select a topic...</option>
              <option v-for="topic in availableTopics" :key="topic.name" :value="topic.name">
                {{ topic.name }} ({{ topic.partition_count }} partitions)
              </option>
            </select>
            <input v-else v-model="topicName" class="input-field" placeholder="my-topic" />
            <button
              v-if="selectedConnectionId"
              class="btn-fetch"
              :disabled="loadingTopics"
              @click="fetchTopics"
            >
              <i :class="loadingTopics ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-refresh'"></i>
            </button>
          </div>
        </div>

        <div class="form-group">
          <label class="field-label">Catalog / Schema</label>
          <select v-model="selectedNamespaceId" class="input-field">
            <option :value="null">Default (General / sync)</option>
            <option v-for="ns in schemaNamespaces" :key="ns.id" :value="ns.id">
              {{ ns.label }}
            </option>
          </select>
        </div>

        <div class="form-group">
          <label class="field-label">Table name</label>
          <input v-model="tableName" class="input-field" placeholder="e.g. orders-latest" />
        </div>

        <div class="form-group">
          <label class="field-label">Write mode</label>
          <select v-model="writeMode" class="input-field">
            <option value="append">Append</option>
            <option value="upsert">Upsert</option>
            <option value="overwrite">Overwrite</option>
          </select>
        </div>

        <label class="checkbox-row">
          <input v-model="includeExisting" type="checkbox" />
          <span>Include existing messages</span>
          <span class="checkbox-hint">
            When checked, the first run reads all messages from the beginning of the topic.
            Otherwise only new messages (published after the first run) are consumed.
          </span>
        </label>
      </div>
      <div class="modal-footer">
        <button class="btn-secondary" @click="$emit('close')">Cancel</button>
        <button class="btn-primary" :disabled="!isValid || submitting" @click="submit">
          {{ submitting ? "Creating..." : "Create Sync" }}
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useCatalogStore } from "../../stores/catalog-store";
import {
  fetchKafkaConnections,
  fetchKafkaTopics,
  createKafkaSync,
} from "../KafkaConnectionView/api";
import type {
  KafkaConnectionOut,
  KafkaTopicInfo,
} from "../KafkaConnectionView/KafkaConnectionTypes";
import { ElMessage } from "element-plus";

const props = defineProps<{
  visible: boolean;
}>();

const emit = defineEmits<{
  (e: "close"): void;
  (e: "created", flowId: number): void;
}>();

const catalogStore = useCatalogStore();

// Form state
const syncName = ref("");
const selectedConnectionId = ref<number | null>(null);
const topicName = ref("");
const selectedNamespaceId = ref<number | null>(null);
const tableName = ref("");
const writeMode = ref<"append" | "upsert" | "overwrite">("append");
const includeExisting = ref(true);
const submitting = ref(false);

// Data
const kafkaConnections = ref<KafkaConnectionOut[]>([]);
const loadingConnections = ref(false);
const availableTopics = ref<KafkaTopicInfo[]>([]);
const loadingTopics = ref(false);

const schemaNamespaces = computed(() => {
  const result: { id: number; label: string }[] = [];
  for (const catalog of catalogStore.tree) {
    for (const schema of catalog.children) {
      result.push({ id: schema.id, label: `${catalog.name} / ${schema.name}` });
    }
  }
  return result;
});

const isValid = computed(() => {
  return (
    syncName.value.trim() &&
    selectedConnectionId.value !== null &&
    topicName.value.trim() &&
    tableName.value.trim()
  );
});

// Reset form when modal opens
watch(
  () => props.visible,
  async (val) => {
    if (val) {
      syncName.value = "";
      selectedConnectionId.value = null;
      topicName.value = "";
      selectedNamespaceId.value = null;
      tableName.value = "";
      writeMode.value = "append";
      includeExisting.value = true;
      availableTopics.value = [];
      await loadConnections();
    }
  },
);

async function loadConnections() {
  loadingConnections.value = true;
  try {
    kafkaConnections.value = await fetchKafkaConnections();
  } catch {
    ElMessage.error("Failed to load Kafka connections");
  } finally {
    loadingConnections.value = false;
  }
}

function handleConnectionChange() {
  availableTopics.value = [];
  topicName.value = "";
}

async function fetchTopics() {
  if (!selectedConnectionId.value) return;
  loadingTopics.value = true;
  try {
    availableTopics.value = await fetchKafkaTopics(selectedConnectionId.value);
  } catch {
    ElMessage.error("Failed to fetch topics");
  } finally {
    loadingTopics.value = false;
  }
}

async function submit() {
  if (!isValid.value || !selectedConnectionId.value) return;
  submitting.value = true;
  try {
    const result = await createKafkaSync({
      sync_name: syncName.value.trim(),
      kafka_connection_id: selectedConnectionId.value,
      topic_name: topicName.value.trim(),
      namespace_id: selectedNamespaceId.value,
      table_name: tableName.value.trim(),
      write_mode: writeMode.value,
      start_offset: includeExisting.value ? "earliest" : "latest",
    });
    ElMessage.success(`Sync flow "sync-${syncName.value.trim()}" created`);
    emit("close");
    emit("created", result.id);
    await Promise.all([
      catalogStore.loadTree(),
      catalogStore.loadAllFlows(),
      catalogStore.loadStats(),
    ]);
  } catch (e: any) {
    ElMessage.error(e?.message ?? "Failed to create sync");
  } finally {
    submitting.value = false;
  }
}
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.modal-card {
  background: var(--color-background-primary);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-lg);
  width: 480px;
  max-width: 90vw;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: var(--spacing-4) var(--spacing-5);
  border-bottom: 1px solid var(--color-border-light);
}

.modal-header h3 {
  margin: 0;
  font-size: var(--font-size-lg);
}

.close-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  border: none;
  background: transparent;
  color: var(--color-text-muted);
  cursor: pointer;
  border-radius: var(--border-radius-sm);
  font-size: 14px;
}

.close-btn:hover {
  color: var(--color-text-primary);
  background: var(--color-background-hover);
}

.modal-body {
  padding: var(--spacing-4) var(--spacing-5);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  overflow-y: auto;
  max-height: 60vh;
}

.modal-description {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.field-label {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.input-field {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  box-sizing: border-box;
}

.input-field:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.15);
}

select.input-field {
  appearance: auto;
}

.topic-row {
  display: flex;
  gap: var(--spacing-2);
  align-items: flex-start;
}

.topic-row .input-field {
  flex: 1;
}

.btn-fetch {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 36px;
  height: 36px;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  cursor: pointer;
  flex-shrink: 0;
}

.btn-fetch:hover {
  background: var(--color-background-hover);
}

.btn-fetch:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.flow-name-hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.flow-name-hint code {
  font-family: var(--font-family-mono);
  background: var(--color-background-muted);
  padding: 1px 4px;
  border-radius: 3px;
}

.ns-hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.modal-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-5);
  border-top: 1px solid var(--color-border-light);
}

.btn-primary {
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-primary);
  color: #fff;
  border: none;
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
  transition: opacity var(--transition-fast);
}

.btn-primary:hover {
  opacity: 0.9;
}

.btn-primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-secondary {
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.btn-secondary:hover {
  background: var(--color-background-hover);
}

.checkbox-row {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  cursor: pointer;
}

.checkbox-row input[type="checkbox"] {
  width: 16px;
  height: 16px;
  cursor: pointer;
}

.checkbox-hint {
  width: 100%;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  padding-left: calc(16px + var(--spacing-2));
}
</style>
