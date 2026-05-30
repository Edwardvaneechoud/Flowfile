<template>
  <div v-if="dataLoaded && nodeRestApi" class="rest-api-reader-container">
    <generic-node-settings
      v-model="nodeRestApi"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <!-- Request -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Request</h4>
        <div class="form-row">
          <div class="form-group method-col">
            <label for="rest-method">Method</label>
            <select id="rest-method" v-model="settings.method" class="form-control">
              <option value="GET">GET</option>
              <option value="POST">POST</option>
            </select>
          </div>
          <div class="form-group url-col">
            <label for="rest-url">URL <span class="required">*</span></label>
            <input
              id="rest-url"
              v-model="settings.url"
              type="text"
              class="form-control"
              placeholder="https://api.example.com/v1/items"
            />
          </div>
        </div>

        <div class="form-group">
          <label for="rest-record-path">Record path</label>
          <input
            id="rest-record-path"
            v-model="settings.record_path"
            type="text"
            class="form-control"
            placeholder="data.items (leave empty for top-level)"
          />
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            <span
              >Dot-path to the array of records in the JSON response. Nested objects are flattened
              into dotted column names.</span
            >
          </div>
        </div>
      </div>

      <!-- Headers -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Headers</h4>
        <div v-for="(pair, index) in headerPairs" :key="`h-${index}`" class="kv-row">
          <input v-model="pair.key" type="text" class="form-control" placeholder="Header name" />
          <input v-model="pair.value" type="text" class="form-control" placeholder="Value" />
          <button type="button" class="icon-btn" title="Remove" @click="removeHeader(index)">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
        <button type="button" class="add-btn" @click="addHeader">
          <i class="fa-solid fa-plus"></i> Add header
        </button>
      </div>

      <!-- Query params -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Query parameters</h4>
        <div v-for="(pair, index) in paramPairs" :key="`p-${index}`" class="kv-row">
          <input v-model="pair.key" type="text" class="form-control" placeholder="Param name" />
          <input v-model="pair.value" type="text" class="form-control" placeholder="Value" />
          <button type="button" class="icon-btn" title="Remove" @click="removeParam(index)">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
        <button type="button" class="add-btn" @click="addParam">
          <i class="fa-solid fa-plus"></i> Add parameter
        </button>
      </div>

      <!-- JSON body (POST only) -->
      <div v-if="settings.method === 'POST'" class="listbox-wrapper">
        <h4 class="section-subtitle">JSON body</h4>
        <div class="form-group" :class="{ 'has-error': bodyError }">
          <textarea
            v-model="bodyText"
            class="form-control code-area"
            rows="5"
            placeholder='{"query": "value"}'
          ></textarea>
          <div v-if="bodyError" class="helper-text helper-text-warning">
            <i class="fa-solid fa-triangle-exclamation"></i>
            <span>{{ bodyError }}</span>
          </div>
        </div>
      </div>

      <!-- Authentication -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Authentication</h4>
        <div class="form-group">
          <label for="rest-auth-type">Type</label>
          <select id="rest-auth-type" v-model="settings.auth.auth_type" class="form-control">
            <option value="none">None</option>
            <option value="api_key">API key</option>
            <option value="bearer">Bearer token</option>
            <option value="basic">Basic</option>
          </select>
        </div>

        <template v-if="settings.auth.auth_type === 'api_key'">
          <div class="form-row">
            <div class="form-group half">
              <label for="rest-apikey-name">Key name</label>
              <input
                id="rest-apikey-name"
                v-model="settings.auth.api_key_name"
                type="text"
                class="form-control"
                placeholder="X-API-Key"
              />
            </div>
            <div class="form-group half">
              <label for="rest-apikey-loc">Location</label>
              <select
                id="rest-apikey-loc"
                v-model="settings.auth.api_key_location"
                class="form-control"
              >
                <option value="header">Header</option>
                <option value="query">Query param</option>
              </select>
            </div>
          </div>
        </template>

        <template v-if="settings.auth.auth_type === 'basic'">
          <div class="form-group">
            <label for="rest-basic-user">Username</label>
            <input
              id="rest-basic-user"
              v-model="settings.auth.basic_username"
              type="text"
              class="form-control"
              autocomplete="off"
            />
          </div>
        </template>

        <div v-if="settings.auth.auth_type !== 'none'" class="form-group">
          <label for="rest-secret">{{ secretLabel }}</label>
          <el-select
            id="rest-secret"
            v-model="settings.auth.secret_name"
            filterable
            clearable
            placeholder="Select a secret"
            :loading="secretsLoading"
            class="secret-select"
          >
            <el-option v-for="s in secrets" :key="s.name" :label="s.name" :value="s.name">
              <i class="fa-solid fa-key secret-icon"></i> {{ s.name }}
            </el-option>
          </el-select>
          <div class="helper-text">
            <i class="fa-solid fa-info-circle"></i>
            <span v-if="!secretsLoading && secrets.length === 0">
              No secrets yet —
              <a class="hint-link" @click="openSecretsManager">create one</a> in the Secrets
              manager, then reselect here.
            </span>
            <span v-else>
              Reusable secret holding the {{ secretLabel.toLowerCase() }}.
              <a class="hint-link" @click="openSecretsManager">Manage secrets</a>.
            </span>
          </div>
        </div>
      </div>

      <!-- Pagination -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Pagination</h4>
        <div class="form-group">
          <label for="rest-pagination-type">Strategy</label>
          <select
            id="rest-pagination-type"
            v-model="settings.pagination.pagination_type"
            class="form-control"
          >
            <option value="none">None (single request)</option>
            <option value="offset">Offset / limit</option>
            <option value="page">Page number</option>
            <option value="cursor">Cursor / next-page token</option>
          </select>
        </div>

        <template v-if="settings.pagination.pagination_type === 'offset'">
          <div class="form-row">
            <div class="form-group third">
              <label>Offset param</label>
              <input v-model="settings.pagination.offset_param" type="text" class="form-control" />
            </div>
            <div class="form-group third">
              <label>Limit param</label>
              <input v-model="settings.pagination.limit_param" type="text" class="form-control" />
            </div>
            <div class="form-group third">
              <label>Page size</label>
              <input
                v-model.number="settings.pagination.page_size"
                type="number"
                class="form-control"
                min="1"
              />
            </div>
          </div>
        </template>

        <template v-if="settings.pagination.pagination_type === 'page'">
          <div class="form-row">
            <div class="form-group half">
              <label>Page param</label>
              <input v-model="settings.pagination.page_param" type="text" class="form-control" />
            </div>
            <div class="form-group half">
              <label>Start page</label>
              <input
                v-model.number="settings.pagination.start_page"
                type="number"
                class="form-control"
              />
            </div>
          </div>
        </template>

        <template v-if="settings.pagination.pagination_type === 'cursor'">
          <div class="form-row">
            <div class="form-group half">
              <label>Cursor request param</label>
              <input v-model="settings.pagination.cursor_param" type="text" class="form-control" />
            </div>
            <div class="form-group half">
              <label>Read cursor from</label>
              <select v-model="settings.pagination.cursor_location" class="form-control">
                <option value="body">Response body (dot-path)</option>
                <option value="header">Response header</option>
              </select>
            </div>
          </div>
          <div class="form-group">
            <label>{{
              settings.pagination.cursor_location === "header"
                ? "Response header name"
                : "Cursor body path"
            }}</label>
            <input
              v-model="settings.pagination.cursor_response_path"
              type="text"
              class="form-control"
              :placeholder="
                settings.pagination.cursor_location === 'header'
                  ? 'X-Next-Cursor'
                  : 'meta.next_cursor'
              "
            />
          </div>
        </template>

        <div v-if="settings.pagination.pagination_type !== 'none'" class="form-row">
          <div class="form-group half">
            <label>Max pages</label>
            <input
              v-model.number="settings.pagination.max_pages"
              type="number"
              class="form-control"
              min="1"
            />
          </div>
          <div class="form-group half">
            <label>Max records</label>
            <input
              v-model.number="maxRecordsModel"
              type="number"
              class="form-control"
              placeholder="unlimited"
              min="1"
            />
          </div>
        </div>
      </div>

      <!-- Advanced -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Advanced</h4>
        <div class="form-row">
          <div class="form-group half">
            <label>Timeout (seconds)</label>
            <input
              v-model.number="settings.timeout_seconds"
              type="number"
              class="form-control"
              min="1"
            />
          </div>
          <div class="form-group half">
            <label>Max retries</label>
            <input
              v-model.number="settings.max_retries"
              type="number"
              class="form-control"
              min="0"
            />
          </div>
        </div>
      </div>

      <!-- Schema / Fetch sample -->
      <div class="listbox-wrapper">
        <h4 class="section-subtitle">Output schema</h4>
        <button
          type="button"
          class="sample-btn"
          :disabled="sampling || !settings.url"
          @click="fetchSample"
        >
          <i v-if="sampling" class="fa-solid fa-spinner fa-spin"></i>
          <i v-else class="fa-solid fa-vial"></i>
          {{ sampling ? "Fetching sample..." : "Fetch sample" }}
        </button>
        <div v-if="sampleError" class="helper-text helper-text-warning">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>{{ sampleError }}</span>
        </div>
        <div v-if="nodeRestApi.fields && nodeRestApi.fields.length" class="schema-preview">
          <div v-for="field in nodeRestApi.fields" :key="field.name" class="schema-row">
            <span class="schema-name">{{ field.name }}</span>
            <span class="schema-type">{{ field.data_type }}</span>
          </div>
        </div>
        <div v-else class="helper-text">
          <i class="fa-solid fa-info-circle"></i>
          <span
            >Fetch a sample to preview columns. Otherwise they are inferred on the first run.</span
          >
        </div>
      </div>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import axios from "axios";
import { computed, ref } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElOption, ElSelect } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { createNodeRestApiReader } from "./utils";
import { SecretsApi } from "../../../../../api";
import type { Secret } from "../../../../../types";
import type { NodeRestApiReader } from "../../../../../types/node.types";

interface Props {
  nodeId: number;
}

defineProps<Props>();

const nodeStore = useNodeStore();
const router = useRouter();
const dataLoaded = ref<boolean>(false);
const nodeRestApi = ref<NodeRestApiReader | null>(null);

interface KvPair {
  key: string;
  value: string;
}
const headerPairs = ref<KvPair[]>([]);
const paramPairs = ref<KvPair[]>([]);
const bodyText = ref<string>("");
const bodyError = ref<string>("");
const sampling = ref<boolean>(false);
const sampleError = ref<string>("");

// settings is a stable accessor used throughout the template.
const settings = computed(() => nodeRestApi.value!.rest_api_settings);

const secretLabel = computed(() => {
  switch (settings.value.auth.auth_type) {
    case "api_key":
      return "API key";
    case "bearer":
      return "Bearer token";
    case "basic":
      return "Password";
    default:
      return "Secret";
  }
});

// Reusable secrets: the dropdown selects a stored secret by name. The secret
// value itself is never sent to or from the node — only the reference name.
const secrets = ref<Secret[]>([]);
const secretsLoading = ref<boolean>(false);

const loadSecrets = async () => {
  secretsLoading.value = true;
  try {
    secrets.value = await SecretsApi.getAll();
  } catch (error) {
    console.error("Failed to load secrets:", error);
    secrets.value = [];
  } finally {
    secretsLoading.value = false;
  }
};

const openSecretsManager = () => {
  // Navigate in-app to the Secrets tab of the Connections manager (same tab).
  router.push({ name: "connections", query: { tab: "secrets" } });
};

// max_records: empty/0 -> null (unlimited).
const maxRecordsModel = computed({
  get: () => settings.value.pagination.max_records ?? null,
  set: (v: number | null) => {
    settings.value.pagination.max_records =
      v === null || Number.isNaN(v) || v === 0 ? null : Number(v);
  },
});

const recordToPairs = (rec: Record<string, string> | undefined | null): KvPair[] =>
  Object.entries(rec ?? {}).map(([key, value]) => ({ key, value: String(value) }));

const pairsToRecord = (pairs: KvPair[]): Record<string, string> => {
  const out: Record<string, string> = {};
  for (const p of pairs) {
    const k = p.key.trim();
    if (k) out[k] = p.value;
  }
  return out;
};

const addHeader = () => headerPairs.value.push({ key: "", value: "" });
const removeHeader = (index: number) => headerPairs.value.splice(index, 1);
const addParam = () => paramPairs.value.push({ key: "", value: "" });
const removeParam = (index: number) => paramPairs.value.splice(index, 1);

/**
 * Sync the editable widgets (header/param pairs, JSON body text) back into the
 * model. Returns false if the JSON body is invalid so callers can abort.
 */
const syncToModel = (): boolean => {
  if (!nodeRestApi.value) return false;
  settings.value.headers = pairsToRecord(headerPairs.value);
  settings.value.query_params = pairsToRecord(paramPairs.value);
  bodyError.value = "";
  if (settings.value.method === "POST" && bodyText.value.trim()) {
    try {
      settings.value.json_body = JSON.parse(bodyText.value);
    } catch (e) {
      bodyError.value = `Invalid JSON: ${(e as Error).message}`;
      return false;
    }
  } else {
    settings.value.json_body = null;
  }
  return true;
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeRestApi,
  onBeforeSave: () => {
    if (!settings.value.url.trim()) {
      ElMessage.error("A request URL is required.");
      return false;
    }
    if (!syncToModel()) {
      ElMessage.error(bodyError.value || "Invalid request body.");
      return false;
    }
  },
});

const fetchSample = async () => {
  if (!nodeRestApi.value) return;
  if (!syncToModel()) {
    ElMessage.error(bodyError.value || "Invalid request body.");
    return;
  }
  sampling.value = true;
  sampleError.value = "";
  try {
    const response = await axios.post("/rest_api/sample", nodeRestApi.value, {
      params: { sample_size: 50 },
    });
    const inferred = response.data.fields ?? [];
    nodeRestApi.value.fields = inferred;
    if (!inferred.length) {
      sampleError.value = "The sample returned no columns. Check the URL and record path.";
    } else {
      ElMessage.success(`Inferred ${inferred.length} column(s).`);
    }
  } catch (error: any) {
    const detail = error?.response?.data?.detail ?? error?.message ?? "Unknown error";
    sampleError.value = `Failed to fetch sample: ${detail}`;
  } finally {
    sampling.value = false;
  }
};

const initEditors = () => {
  if (!nodeRestApi.value) return;
  const s = nodeRestApi.value.rest_api_settings;
  headerPairs.value = recordToPairs(s.headers);
  paramPairs.value = recordToPairs(s.query_params);
  bodyText.value =
    s.json_body !== null && s.json_body !== undefined ? JSON.stringify(s.json_body, null, 2) : "";
};

const loadNodeData = async (nodeId: number) => {
  try {
    void loadSecrets();
    const nodeData = await nodeStore.getNodeData(nodeId, false);
    if (nodeData) {
      const hasValidSetup = Boolean(nodeData.setting_input?.is_setup);
      nodeRestApi.value = hasValidSetup
        ? (nodeData.setting_input as NodeRestApiReader)
        : createNodeRestApiReader(nodeStore.flow_id, nodeId);
      initEditors();
    }
    dataLoaded.value = true;
  } catch (error) {
    console.error("Error loading REST API reader node data:", error);
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
.rest-api-reader-container {
  font-family: var(--font-family-base);
  max-width: 100%;
  color: var(--color-text-primary);
}

.section-subtitle {
  margin: 0 0 10px;
  font-size: 13px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.listbox-wrapper {
  margin-bottom: 18px;
}

.form-group {
  margin-bottom: 12px;
  display: flex;
  flex-direction: column;
}

.form-group label {
  margin-bottom: 4px;
  font-size: 12px;
  font-weight: 500;
}

.form-row {
  display: flex;
  gap: 10px;
}

.form-group.half {
  flex: 1;
}

.form-group.third {
  flex: 1;
}

.method-col {
  width: 110px;
  flex: 0 0 110px;
}

.url-col {
  flex: 1;
}

.form-control {
  width: 100%;
  padding: 6px 8px;
  border: 1px solid var(--color-border, #d0d0d0);
  border-radius: 4px;
  font-size: 13px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  box-sizing: border-box;
}

.code-area {
  font-family: var(--font-family-mono, monospace);
}

.kv-row {
  display: flex;
  gap: 8px;
  margin-bottom: 6px;
  align-items: center;
}

.icon-btn,
.add-btn,
.sample-btn {
  cursor: pointer;
  border: 1px solid var(--color-border, #d0d0d0);
  border-radius: 4px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  padding: 6px 10px;
  font-size: 12px;
}

.icon-btn {
  flex: 0 0 auto;
}

.add-btn:hover,
.sample-btn:hover:not(:disabled) {
  background: var(--color-background-hover);
  border-color: var(--color-border-focus);
}

.sample-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.required {
  color: var(--color-danger, #d9534f);
}

.helper-text {
  display: flex;
  gap: 6px;
  align-items: flex-start;
  font-size: 11px;
  color: var(--color-text-secondary, #888);
  margin-top: 4px;
}

.helper-text-warning {
  color: var(--color-warning, #d9822b);
}

.hint-link {
  color: var(--color-accent, #3b82f6);
  cursor: pointer;
  text-decoration: underline;
}

.secret-icon {
  color: var(--color-text-secondary, #888);
  font-size: 11px;
  margin-right: 4px;
}

.secret-select {
  width: 100%;
}

.has-error .form-control {
  border-color: var(--color-danger, #d9534f);
}

.schema-preview {
  margin-top: 10px;
  border: 1px solid var(--color-border, #e0e0e0);
  border-radius: 4px;
  overflow: hidden;
}

.schema-row {
  display: flex;
  justify-content: space-between;
  padding: 4px 10px;
  font-size: 12px;
  border-bottom: 1px solid var(--color-border, #eee);
}

.schema-row:last-child {
  border-bottom: none;
}

.schema-name {
  font-family: var(--font-family-mono, monospace);
}

.schema-type {
  color: var(--color-text-secondary, #888);
}
</style>
