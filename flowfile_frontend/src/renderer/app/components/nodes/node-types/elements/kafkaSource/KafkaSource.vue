<template>
  <div v-if="dataLoaded && nodeKafkaSource" class="kafka-source-container">
    <generic-node-settings
      v-model="nodeKafkaSource"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <!-- Connection Selection -->
      <div class="listbox-wrapper">
        <div class="form-group">
          <label for="kafka-connection-select">Kafka Connection</label>
          <div v-if="connectionsAreLoading" class="loading-state">
            <div class="loading-spinner"></div>
            <p>Loading connections...</p>
          </div>
          <div v-else>
            <select
              id="kafka-connection-select"
              v-model="selectedConnectionId"
              class="form-control minimal-select"
              @change="handleConnectionChange"
            >
              <option :value="null">Select a connection...</option>
              <option v-for="conn in kafkaConnections" :key="conn.id" :value="conn.id">
                {{ conn.connection_name }} ({{ conn.bootstrap_servers }})
              </option>
            </select>
            <div v-if="!selectedConnectionId" class="helper-text">
              <i class="fa-solid fa-info-circle"></i>
              Set up a Kafka connection in the Connection Manager first
            </div>
          </div>
        </div>
      </div>

      <!-- Topic Selection -->
      <div v-if="selectedConnectionId" class="listbox-wrapper">
        <h4 class="section-subtitle">Topic</h4>

        <div class="form-group">
          <label for="topic-name">Topic Name</label>
          <div class="topic-input-row">
            <select
              v-if="availableTopics.length > 0"
              id="topic-name"
              v-model="nodeKafkaSource.kafka_settings.topic_name"
              class="form-control"
              @change="resetFields"
            >
              <option value="">Select a topic...</option>
              <option v-for="topic in availableTopics" :key="topic.name" :value="topic.name">
                {{ topic.name }} ({{ topic.partition_count }} partitions)
              </option>
            </select>
            <input
              v-else
              id="topic-name"
              v-model="nodeKafkaSource.kafka_settings.topic_name"
              type="text"
              class="form-control"
              placeholder="my-topic"
              @input="resetFields"
            />
            <button
              type="button"
              class="btn btn-secondary btn-fetch"
              :disabled="topicsAreLoading"
              @click="handleFetchTopics"
            >
              <i v-if="topicsAreLoading" class="fa-solid fa-spinner fa-spin"></i>
              <i v-else class="fa-solid fa-refresh"></i>
              Fetch Topics
            </button>
          </div>
        </div>
      </div>

      <!-- Consumer Settings -->
      <div v-if="selectedConnectionId" class="listbox-wrapper">
        <h4 class="section-subtitle">Consumer Settings</h4>

        <div class="form-group">
          <label for="start-offset">Start Offset</label>
          <select
            id="start-offset"
            v-model="nodeKafkaSource.kafka_settings.start_offset"
            class="form-control"
          >
            <option value="latest">Latest</option>
            <option value="earliest">Earliest</option>
          </select>
        </div>

        <div class="form-row">
          <div class="form-group half">
            <label for="max-messages">Max Messages</label>
            <input
              id="max-messages"
              v-model.number="nodeKafkaSource.kafka_settings.max_messages"
              type="number"
              class="form-control"
              min="1"
              placeholder="100000"
            />
          </div>

          <div class="form-group half">
            <label for="poll-timeout">Poll Timeout (seconds)</label>
            <input
              id="poll-timeout"
              v-model.number="nodeKafkaSource.kafka_settings.poll_timeout_seconds"
              type="number"
              class="form-control"
              min="1"
              step="1"
              placeholder="30"
            />
          </div>
        </div>

        <div class="form-group">
          <label for="sync-name">Sync Name (Optional)</label>
          <input
            id="sync-name"
            v-model="nodeKafkaSource.kafka_settings.sync_name"
            type="text"
            class="form-control"
            placeholder="Unique key for offset tracking between runs"
          />
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            When set, consumer offsets are tracked between flow runs for incremental reads
          </div>
        </div>

        <div class="form-group">
          <button
            type="button"
            class="btn btn-warning"
            :disabled="resettingOffsets"
            @click="handleResetOffsets"
          >
            <i v-if="resettingOffsets" class="fa-solid fa-spinner fa-spin"></i>
            <i v-else class="fa-solid fa-rotate-left"></i>
            Reset Offsets
          </button>
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            Resets tracked offsets so the next run re-reads all messages from start offset
          </div>
        </div>
      </div>

      <!-- Schema Inference -->
      <div
        v-if="selectedConnectionId && nodeKafkaSource.kafka_settings.topic_name"
        class="listbox-wrapper"
      >
        <h4 class="section-subtitle">Schema</h4>
        <div class="form-group">
          <button
            type="button"
            class="btn btn-secondary"
            :disabled="schemaIsLoading"
            @click="handleInferSchema"
          >
            <i v-if="schemaIsLoading" class="fa-solid fa-spinner fa-spin"></i>
            <i v-else class="fa-solid fa-search"></i>
            Infer Schema
          </button>
        </div>
        <div v-if="inferredSchema.length > 0" class="schema-table">
          <table>
            <thead>
              <tr>
                <th>Column</th>
                <th>Type</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="field in inferredSchema" :key="field.name">
                <td>{{ field.name }}</td>
                <td>{{ field.dtype }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </generic-node-settings>
  </div>
  <code-loader v-else />
</template>

<script lang="ts" setup>
import { CodeLoader } from "vue-content-loader";
import { ref } from "vue";
import { NodeKafkaSource } from "../../../baseNode/nodeInput";
import { createNodeKafkaSource } from "./utils";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import {
  fetchKafkaConnections,
  fetchKafkaTopics,
  inferKafkaTopicSchema,
  resetKafkaOffsets,
} from "../../../../../views/KafkaConnectionView/api";
import type {
  KafkaConnectionOut,
  KafkaTopicInfo,
} from "../../../../../views/KafkaConnectionView/KafkaConnectionTypes";
import { ElMessage, ElMessageBox } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";

interface Props {
  nodeId: number;
}

defineProps<Props>();
const nodeStore = useNodeStore();
const dataLoaded = ref<boolean>(false);
const nodeKafkaSource = ref<NodeKafkaSource | null>(null);

// Use the standardized node settings composable
const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeKafkaSource,
});

const kafkaConnections = ref<KafkaConnectionOut[]>([]);
const connectionsAreLoading = ref(false);
const selectedConnectionId = ref<number | null>(null);

const availableTopics = ref<KafkaTopicInfo[]>([]);
const topicsAreLoading = ref(false);

const inferredSchema = ref<{ name: string; dtype: string }[]>([]);
const schemaIsLoading = ref(false);
const resettingOffsets = ref(false);

const handleConnectionChange = () => {
  if (nodeKafkaSource.value && selectedConnectionId.value !== null) {
    const conn = kafkaConnections.value.find((c) => c.id === selectedConnectionId.value);
    nodeKafkaSource.value.kafka_settings.kafka_connection_id = selectedConnectionId.value;
    nodeKafkaSource.value.kafka_settings.kafka_connection_name = conn?.connection_name || null;
  } else if (nodeKafkaSource.value) {
    nodeKafkaSource.value.kafka_settings.kafka_connection_id = null;
    nodeKafkaSource.value.kafka_settings.kafka_connection_name = null;
  }
  availableTopics.value = [];
  resetFields();
};

const handleFetchTopics = async () => {
  if (!selectedConnectionId.value) return;
  topicsAreLoading.value = true;
  try {
    availableTopics.value = await fetchKafkaTopics(selectedConnectionId.value);
  } catch (error) {
    console.error("Error fetching topics:", error);
    ElMessage.error("Failed to fetch topics from Kafka broker");
  } finally {
    topicsAreLoading.value = false;
  }
};

const handleInferSchema = async () => {
  if (!selectedConnectionId.value || !nodeKafkaSource.value?.kafka_settings.topic_name) return;
  schemaIsLoading.value = true;
  try {
    inferredSchema.value = await inferKafkaTopicSchema(
      selectedConnectionId.value,
      nodeKafkaSource.value.kafka_settings.topic_name,
    );
    if (inferredSchema.value.length === 0) {
      ElMessage.warning("No messages found to infer schema from");
    }
  } catch (error) {
    console.error("Error inferring schema:", error);
    ElMessage.error("Failed to infer schema from topic");
  } finally {
    schemaIsLoading.value = false;
  }
};

const handleResetOffsets = async () => {
  if (!selectedConnectionId.value || !nodeKafkaSource.value) return;
  const topicName = nodeKafkaSource.value.kafka_settings.topic_name;
  if (!topicName) {
    ElMessage.error("Select a topic before resetting offsets.");
    return;
  }
  const effectiveSyncName =
    nodeKafkaSource.value.kafka_settings.sync_name ||
    `flowfile-${nodeKafkaSource.value.flow_id}-node-${nodeKafkaSource.value.node_id}`;
  try {
    await ElMessageBox.confirm(
      `This will reset offsets for consumer group "${effectiveSyncName}". The next run will re-read messages from the configured start offset.`,
      "Reset Offsets",
      { confirmButtonText: "Reset", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return; // user cancelled
  }
  resettingOffsets.value = true;
  try {
    await resetKafkaOffsets(effectiveSyncName, selectedConnectionId.value, topicName);
    ElMessage.success("Offsets reset successfully. Next run will start fresh.");
  } catch (error: any) {
    ElMessage.error(error.message || "Failed to reset offsets");
  } finally {
    resettingOffsets.value = false;
  }
};

const resetFields = () => {
  if (nodeKafkaSource.value) {
    nodeKafkaSource.value.fields = [];
  }
  inferredSchema.value = [];
};

const fetchConnections = async () => {
  connectionsAreLoading.value = true;
  try {
    kafkaConnections.value = await fetchKafkaConnections();
  } catch (error) {
    console.error("Error fetching Kafka connections:", error);
    ElMessage.error("Failed to load Kafka connections");
  } finally {
    connectionsAreLoading.value = false;
  }
};

const loadNodeData = async (nodeId: number) => {
  try {
    const [nodeData] = await Promise.all([
      nodeStore.getNodeData(nodeId, false),
      fetchConnections(),
    ]);
    if (nodeData) {
      const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
      nodeKafkaSource.value = hasValidSetup
        ? nodeData.setting_input
        : createNodeKafkaSource(nodeStore.flow_id, nodeId);
      if (nodeKafkaSource.value?.kafka_settings.kafka_connection_id) {
        selectedConnectionId.value = nodeKafkaSource.value.kafka_settings.kafka_connection_id;
      } else {
        selectedConnectionId.value = null;
      }
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading node data:", error);
    dataLoaded.value = false;
  }
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.kafka-source-container {
  font-family: var(--font-family-base);
  max-width: 100%;
  color: var(--color-text-primary);
}

.section-subtitle {
  margin: 0 0 0.75rem 0;
  font-size: 0.95rem;
  font-weight: 600;
  color: #4a5568;
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
  min-width: 0;
  max-width: calc(50% - 0.375rem);
}

.form-control {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 0.875rem;
  box-sizing: border-box;
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
  color: #4a5568;
}

select.form-control {
  appearance: none;
  background-image: url("data:image/svg+xml;charset=utf-8,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%234a5568' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 0.5rem center;
  background-size: 1em;
  padding-right: 2rem;
}

.topic-input-row {
  display: flex;
  gap: 0.5rem;
  align-items: flex-start;
}

.topic-input-row .form-control {
  flex: 1;
}

.btn-fetch {
  white-space: nowrap;
  flex-shrink: 0;
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

.schema-table {
  margin-top: 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  overflow: hidden;
}

.schema-table table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.8125rem;
}

.schema-table th {
  background-color: #f7fafc;
  padding: 0.5rem 0.75rem;
  text-align: left;
  font-weight: 600;
  color: #4a5568;
  border-bottom: 1px solid #e2e8f0;
}

.schema-table td {
  padding: 0.375rem 0.75rem;
  border-bottom: 1px solid #edf2f7;
  color: #4a5568;
}

.schema-table tr:last-child td {
  border-bottom: none;
}

@media (max-width: 640px) {
  .form-row {
    flex-direction: column;
    gap: 0.5rem;
  }

  .half {
    max-width: 100%;
  }

  .topic-input-row {
    flex-direction: column;
  }
}
</style>
