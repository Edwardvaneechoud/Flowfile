<template>
  <div v-if="dataLoaded && nodeRestApi" class="rest-api-reader-container">
    <generic-node-settings
      v-model="nodeRestApi"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="request-bar">
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
      </div>

      <section class="reader-section" data-testid="rest-section-request">
        <h4 class="reader-section-title">Parameters and headers</h4>
        <div class="kv-head">
          <label class="group-label">Query parameters</label>
          <button type="button" class="link-btn" @click="addParam">
            <i class="fa-solid fa-plus"></i> Add parameter
          </button>
        </div>
        <div v-for="(pair, index) in paramPairs" :key="`p-${index}`" class="kv-row">
          <input v-model="pair.key" type="text" class="form-control" placeholder="Name" />
          <input v-model="pair.value" type="text" class="form-control" placeholder="Value" />
          <button type="button" class="icon-btn" title="Remove" @click="removeParam(index)">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>

        <div class="kv-head kv-head-spaced">
          <label class="group-label">Headers</label>
          <button type="button" class="link-btn" @click="addHeader">
            <i class="fa-solid fa-plus"></i> Add header
          </button>
        </div>
        <div v-for="(pair, index) in headerPairs" :key="`h-${index}`" class="kv-row">
          <input v-model="pair.key" type="text" class="form-control" placeholder="Name" />
          <input v-model="pair.value" type="text" class="form-control" placeholder="Value" />
          <button type="button" class="icon-btn" title="Remove" @click="removeHeader(index)">
            <i class="fa-solid fa-trash"></i>
          </button>
        </div>
      </section>

      <section
        v-if="settings.method === 'POST'"
        class="reader-section"
        data-testid="rest-section-body"
      >
        <h4 class="reader-section-title">JSON body</h4>
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
      </section>

      <section class="reader-section" data-testid="rest-section-auth">
        <h4 class="reader-section-title">Authentication</h4>
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
          <div class="field-label">
            <label for="rest-secret">{{ secretLabel }}</label>
            <field-info :help="FIELD_HELP.secret" />
          </div>
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
            <span v-if="!secretsLoading && secrets.length === 0">
              No secrets yet.
              <a class="hint-link" @click="openSecretsManager">Create one</a>, then select it here.
            </span>
            <a v-else class="hint-link" @click="openSecretsManager">Manage secrets</a>
          </div>
        </div>
      </section>

      <div class="test-bar">
        <div class="test-row">
          <span class="test-hint">{{ testHint }}</span>
          <button
            type="button"
            class="sample-btn"
            data-testid="rest-test-request"
            :disabled="sampling || !settings.url"
            @click="fetchSample()"
          >
            <i v-if="sampling" class="fa-solid fa-spinner fa-spin"></i>
            <i v-else class="fa-solid fa-vial"></i>
            {{ sampling ? "Sending..." : "Test request" }}
          </button>
        </div>
        <div v-if="sampleError" class="test-error" data-testid="rest-test-error">
          <i class="fa-solid fa-triangle-exclamation"></i>
          <span>{{ sampleError }}</span>
        </div>
        <request-trace
          v-if="sentRequest"
          :entry="sentRequest"
          :secret-name="settings.auth.secret_name"
          :response-text="sample?.response_text ?? ''"
          :truncated="sample?.response_truncated ?? false"
        />
      </div>

      <section class="reader-section" data-testid="rest-section-records">
        <h4 class="reader-section-title">Records</h4>
        <template v-if="sample?.response_tree">
          <response-tree
            :tree="sample.response_tree"
            :record-path="settings.record_path"
            :cursor-path="settings.pagination.cursor_response_path"
            :pick-cursor="cursorFromBody"
            @pick-records="useRecordPath"
            @pick-cursor="useCursorPath"
          />
          <div class="helper-text tree-hint">
            <i class="fa-solid fa-info-circle"></i>
            <span>{{
              detectedRecords
                ? "Record list detected. Click another list to change it."
                : "Click a list to use it as the record path."
            }}</span>
          </div>
        </template>
        <div class="form-group">
          <div class="field-label">
            <label for="rest-record-path">Record path</label>
            <field-info :help="FIELD_HELP.recordPath" />
          </div>
          <input
            id="rest-record-path"
            v-model="settings.record_path"
            type="text"
            class="form-control"
            placeholder="data.items"
          />
          <div
            v-if="!sample?.response_tree && recordSuggestions.length"
            class="path-suggestions"
            data-testid="record-path-suggestions"
          >
            <span>Found in the response:</span>
            <button
              v-for="path in recordSuggestions"
              :key="path"
              type="button"
              class="path-chip"
              @click="useRecordPath(path)"
            >
              {{ path }}
            </button>
          </div>
        </div>
      </section>

      <section class="reader-section" data-testid="rest-section-pagination">
        <h4 class="reader-section-title">Pagination</h4>
        <div
          v-if="settings.pagination.pagination_type === 'none' && detectedCursor"
          class="detected-paging"
          data-testid="rest-detected-paging"
        >
          <span
            >Next-page token found at <code>{{ detectedCursor }}</code
            >.</span
          >
          <button type="button" class="add-btn" @click="followCursor(detectedCursor)">
            Read all pages
          </button>
        </div>

        <div class="form-group">
          <div class="field-label">
            <label for="rest-pagination-type">Type</label>
            <field-info :help="FIELD_HELP.paginationType" />
          </div>
          <select
            id="rest-pagination-type"
            v-model="settings.pagination.pagination_type"
            class="form-control"
          >
            <option value="none">None</option>
            <option value="cursor">Cursor (next-page token)</option>
            <option value="page">Page number</option>
            <option value="offset">Offset and limit</option>
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
              <label>First page</label>
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
              <div class="field-label">
                <label for="rest-cursor-location">Token location</label>
                <field-info :help="FIELD_HELP.tokenLocation" />
              </div>
              <select
                id="rest-cursor-location"
                v-model="settings.pagination.cursor_location"
                class="form-control"
              >
                <option value="body">Response body</option>
                <option value="header">Response header</option>
              </select>
            </div>
            <div class="form-group half">
              <div class="field-label">
                <label for="rest-cursor-path">{{
                  cursorFromBody ? "Token path" : "Header name"
                }}</label>
                <field-info
                  :help="cursorFromBody ? FIELD_HELP.tokenPath : FIELD_HELP.tokenHeader"
                />
              </div>
              <input
                id="rest-cursor-path"
                v-model="settings.pagination.cursor_response_path"
                type="text"
                class="form-control"
                :placeholder="cursorFromBody ? 'meta.next_cursor' : 'X-Next-Cursor'"
              />
            </div>
          </div>
          <div
            v-if="!settings.pagination.cursor_response_path"
            class="helper-text helper-text-warning cursor-hint"
            data-testid="cursor-path-missing"
          >
            <i class="fa-solid fa-triangle-exclamation"></i>
            <span>Required for pages after the first.</span>
          </div>
          <div
            v-if="cursorFromBody && cursorSuggestions.length"
            class="path-suggestions"
            data-testid="cursor-path-suggestions"
          >
            <span>Found in the response:</span>
            <button
              v-for="path in cursorSuggestions"
              :key="path"
              type="button"
              class="path-chip"
              @click="useCursorPath(path)"
            >
              {{ path }}
            </button>
          </div>

          <div class="form-row">
            <div class="form-group half">
              <div class="field-label">
                <label for="rest-cursor-send">Send as</label>
                <field-info :help="FIELD_HELP.sendAs" />
              </div>
              <select id="rest-cursor-send" v-model="cursorSendLocation" class="form-control">
                <option value="query">Query parameter</option>
                <option value="body" :disabled="settings.method !== 'POST'">
                  {{
                    settings.method === "POST" ? "JSON body field" : "JSON body field (POST only)"
                  }}
                </option>
              </select>
            </div>
            <div class="form-group half">
              <div class="field-label">
                <label for="rest-cursor-param">{{
                  cursorInBody ? "Field name" : "Parameter name"
                }}</label>
                <field-info
                  :help="cursorInBody ? FIELD_HELP.tokenField : FIELD_HELP.tokenParameter"
                />
              </div>
              <input
                id="rest-cursor-param"
                v-model="settings.pagination.cursor_param"
                type="text"
                class="form-control"
                :placeholder="cursorInBody ? 'data_id' : 'cursor'"
              />
            </div>
          </div>
          <div
            v-if="tokenExample"
            class="helper-text cursor-hint token-example"
            data-testid="rest-cursor-example"
          >
            <i class="fa-solid fa-arrow-right"></i>
            <span
              >Sent as <code>{{ tokenExample.code }}</code> in {{ tokenExample.place }}.</span
            >
          </div>
          <div
            v-else
            class="helper-text helper-text-warning cursor-hint"
            data-testid="cursor-param-missing"
          >
            <i class="fa-solid fa-triangle-exclamation"></i>
            <span>Required to send the token.</span>
          </div>
        </template>
      </section>

      <CollapsibleSection
        title="Advanced"
        :summary="limitsSummary(settings)"
        :default-open="false"
        persist-key="restApiReader.advanced"
        nested
        class="advanced-fold"
        data-testid="rest-section-advanced"
      >
        <div v-if="settings.pagination.pagination_type !== 'none'" class="form-row">
          <div class="form-group third">
            <label>Max pages</label>
            <input
              v-model.number="settings.pagination.max_pages"
              type="number"
              class="form-control"
              min="1"
            />
          </div>
          <div class="form-group third">
            <label>Max records</label>
            <input
              v-model.number="maxRecordsModel"
              type="number"
              class="form-control"
              placeholder="unlimited"
              min="1"
            />
          </div>
          <div class="form-group third">
            <label>Wait between pages (s)</label>
            <input
              v-model.number="settings.pagination.page_delay_seconds"
              type="number"
              class="form-control"
              min="0"
              step="0.1"
            />
          </div>
        </div>
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
            <div class="field-label">
              <label for="rest-max-retries">Max retries</label>
              <field-info :help="FIELD_HELP.maxRetries" />
            </div>
            <input
              id="rest-max-retries"
              v-model.number="settings.max_retries"
              type="number"
              class="form-control"
              min="0"
            />
          </div>
        </div>
        <div v-if="settings.pagination.pagination_type === 'cursor'" class="form-group">
          <div class="field-label">
            <label for="rest-initial-cursor">Initial token</label>
            <field-info :help="FIELD_HELP.initialToken" />
          </div>
          <input
            id="rest-initial-cursor"
            v-model="settings.pagination.initial_cursor"
            type="text"
            class="form-control"
            placeholder="none"
          />
        </div>
      </CollapsibleSection>

      <section class="reader-section" data-testid="rest-section-columns">
        <div class="columns-header">
          <h4 class="reader-section-title">Columns</h4>
          <span v-if="columnCount" class="columns-count">{{ columnCount }}</span>
          <button
            v-if="hasColumnGroups"
            type="button"
            class="columns-fold-all"
            data-testid="rest-columns-fold-all"
            @click="toggleAllColumnGroups"
          >
            {{ allColumnGroupsOpen ? "Fold all" : "Unfold all" }}
          </button>
        </div>
        <div v-if="columnCount" class="schema-preview" data-testid="rest-columns">
          <div
            v-for="row in columnTree"
            :key="row.path"
            class="schema-row"
            :class="{ 'is-group': row.group }"
            :style="{ paddingLeft: `${6 + row.depth * 14}px` }"
            :title="row.group ? undefined : `${row.path}: ${row.dataType}`"
            @click="row.group && toggleColumnGroup(row.path)"
          >
            <button
              v-if="row.group"
              type="button"
              class="schema-toggle"
              :aria-expanded="row.open"
              :aria-label="`${row.open ? 'Fold' : 'Unfold'} ${row.path}`"
              @click.stop="toggleColumnGroup(row.path)"
            >
              <i class="fa-solid fa-chevron-right" :class="{ 'is-open': row.open }"></i>
            </button>
            <span v-else class="schema-toggle"></span>
            <span class="schema-name">{{ row.key }}</span>
            <span v-if="row.group" class="schema-type">{{ row.count }} columns</span>
            <span v-else class="schema-type">{{ row.dataType }}</span>
          </div>
        </div>
        <div v-else class="helper-text">
          <i class="fa-solid fa-info-circle"></i>
          <span
            >Test the request to see the columns. Otherwise they are found on the first run.</span
          >
        </div>
      </section>
    </generic-node-settings>
  </div>
</template>

<script lang="ts" setup>
import axios from "axios";
import { computed, ref } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElOption, ElSelect } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { CollapsibleSection } from "../../../../common";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import { createNodeRestApiReader } from "./utils";
import RequestTrace from "./RequestTrace.vue";
import ResponseTree from "./ResponseTree.vue";
import FieldInfo from "./FieldInfo.vue";
import {
  FIELD_HELP,
  columnGroupPaths,
  columnRows,
  detectedCursorPath,
  guessCursorParam,
  limitsSummary,
  nextRequestExample,
  testErrorText,
  type SampleDebug,
} from "./restApiView";
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

const settings = computed(() => nodeRestApi.value!.rest_api_settings);

// What the last test request sent and got back; never saved with the node.
const sample = ref<SampleDebug | null>(null);
const detectedRecords = ref<boolean>(false);
const sentRequest = computed(() => sample.value?.trace?.[0] ?? null);

const recordSuggestions = computed(() =>
  (sample.value?.suggested_paths?.record_paths ?? []).filter(
    (p) => p !== settings.value.record_path,
  ),
);
const cursorSuggestions = computed(() =>
  (sample.value?.suggested_paths?.cursor_paths ?? []).filter(
    (p) => p !== settings.value.pagination.cursor_response_path,
  ),
);
const detectedCursor = computed(() =>
  detectedCursorPath(sample.value?.suggested_paths?.cursor_paths ?? []),
);

const cursorInBody = computed(
  () =>
    settings.value.method === "POST" &&
    settings.value.pagination.cursor_request_location === "body",
);
const cursorFromBody = computed(
  () =>
    settings.value.pagination.pagination_type === "cursor" &&
    settings.value.pagination.cursor_location === "body",
);
/** A GET shows the token as a query parameter and keeps the stored choice for a switch back to POST. */
const cursorSendLocation = computed({
  get: () => (cursorInBody.value ? "body" : "query"),
  set: (value: "query" | "body") => {
    settings.value.pagination.cursor_request_location = value;
  },
});
const tokenExample = computed(() =>
  nextRequestExample(settings.value.pagination.cursor_param, cursorInBody.value),
);

const columnCount = computed(() => nodeRestApi.value?.fields?.length ?? 0);

// Which column groups the reader unfolded; a view preference, never saved with the node.
const openColumnGroups = ref<Set<string>>(new Set());
const allColumnGroups = computed(() => columnGroupPaths(nodeRestApi.value?.fields ?? []));
const hasColumnGroups = computed(() => allColumnGroups.value.size > 0);
const allColumnGroupsOpen = computed(
  () =>
    hasColumnGroups.value &&
    [...allColumnGroups.value].every((path) => openColumnGroups.value.has(path)),
);
const columnTree = computed(() =>
  columnRows(nodeRestApi.value?.fields ?? [], openColumnGroups.value),
);

const toggleColumnGroup = (path: string) => {
  const open = new Set(openColumnGroups.value);
  if (open.has(path)) open.delete(path);
  else open.add(path);
  openColumnGroups.value = open;
};

const toggleAllColumnGroups = () => {
  openColumnGroups.value = allColumnGroupsOpen.value ? new Set() : new Set(allColumnGroups.value);
};

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

const DEFAULTS = createNodeRestApiReader(0, 0).rest_api_settings;

const testHint = computed(() => {
  if (sampling.value) return "Sending one request...";
  return "Sends one request. Nothing is saved.";
});

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
  // A token in the JSON body needs a POST, so a GET is saved sending it as a query parameter.
  if (settings.value.method === "GET") settings.value.pagination.cursor_request_location = "query";
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

const fetchSample = async (afterDetection = false) => {
  if (!nodeRestApi.value) return;
  if (!syncToModel()) {
    ElMessage.error(bodyError.value || "Invalid request body.");
    return;
  }
  const firstTest = !columnCount.value;
  sampling.value = true;
  sampleError.value = "";
  try {
    const response = await axios.post("/rest_api/sample", nodeRestApi.value, {
      params: { sample_size: 50 },
    });
    const { fields, ...debug } = response.data;
    nodeRestApi.value.fields = fields ?? [];
    sample.value = debug as SampleDebug;
    if (!afterDetection) detectedRecords.value = false;
    const found = sample.value.suggested_paths?.record_paths?.[0];
    const responseIsRecords = Boolean(sample.value.response_tree?.records);
    // The record path is filled in once, on a node's first test, and only while it is empty.
    if (
      !afterDetection &&
      firstTest &&
      !settings.value.record_path &&
      found &&
      !responseIsRecords
    ) {
      settings.value.record_path = found;
      detectedRecords.value = true;
      sampling.value = false;
      await fetchSample(true);
      return;
    }
    if (!nodeRestApi.value.fields?.length) {
      sampleError.value = "The request worked, but no columns were found. Check the record path.";
    }
  } catch (error: any) {
    const detail = error?.response?.data?.detail ?? error?.message ?? "Unknown error";
    sample.value = null;
    sampleError.value = testErrorText(String(detail), settings.value.auth.secret_name);
  } finally {
    sampling.value = false;
  }
};

// The columns depend on the record path, so picking one sends the test request again.
const useRecordPath = (path: string) => {
  if (path === settings.value.record_path) return;
  settings.value.record_path = path;
  detectedRecords.value = false;
  void fetchSample(true);
};

const useCursorPath = (path: string) => {
  const pagination = settings.value.pagination;
  pagination.cursor_response_path = path;
  if (pagination.cursor_param === DEFAULTS.pagination.cursor_param) {
    pagination.cursor_param = guessCursorParam(path);
  }
};

const followCursor = (path: string) => {
  settings.value.pagination.pagination_type = "cursor";
  settings.value.pagination.cursor_location = "body";
  useCursorPath(path);
};

const initEditors = () => {
  if (!nodeRestApi.value) return;
  const s = nodeRestApi.value.rest_api_settings;
  headerPairs.value = recordToPairs(s.headers);
  paramPairs.value = recordToPairs(s.query_params);
  if (!paramPairs.value.length) addParam();
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

.reader-section {
  margin-bottom: 18px;
}

.advanced-fold {
  margin-bottom: 18px;
}

.reader-section-title {
  margin: 0 0 10px;
  font-size: 13px;
  font-weight: 600;
  color: var(--color-text-primary);
}

.request-bar {
  margin-bottom: 6px;
}

.test-bar {
  margin-bottom: 18px;
  padding-top: 12px;
  border-top: 1px solid var(--color-border-primary, #e0e0e0);
}

.kv-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.kv-head-spaced {
  margin-top: 12px;
}

.link-btn {
  cursor: pointer;
  border: none;
  background: none;
  padding: 0;
  font-size: 12px;
  color: var(--color-accent, #3b82f6);
}

.test-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}

.test-hint {
  font-size: 11px;
  color: var(--color-text-secondary, #888);
}

.test-error {
  display: flex;
  gap: 6px;
  align-items: flex-start;
  margin-top: 8px;
  padding: 6px 8px;
  border-radius: 4px;
  background: var(--color-danger-light);
  color: var(--color-danger-dark, var(--color-danger));
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-word;
}

.group-label {
  display: block;
  margin-bottom: 4px;
  font-size: 12px;
  font-weight: 500;
}

.field-label {
  display: flex;
  align-items: center;
  gap: 5px;
  margin-bottom: 4px;
}

.form-group .field-label label {
  margin-bottom: 0;
}

.tree-hint {
  margin: 4px 0 12px;
}

.detected-paging {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 12px;
  padding: 6px 8px;
  border-radius: 4px;
  background: var(--color-info-light);
  font-size: 12px;
}

.detected-paging code {
  font-family: var(--font-family-mono, monospace);
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

.sample-btn {
  border-color: var(--color-primary);
  color: var(--color-primary);
  font-weight: 600;
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

.cursor-hint {
  margin: -6px 0 12px;
}

.helper-text code {
  font-family: var(--font-family-mono, monospace);
}

.token-example code {
  padding: 0 4px;
  border-radius: 3px;
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.path-suggestions {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  margin: -4px 0 12px;
  font-size: 11px;
  color: var(--color-text-secondary, #888);
}

.path-chip {
  cursor: pointer;
  border: 1px solid var(--color-border, #d0d0d0);
  border-radius: 10px;
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  padding: 1px 8px;
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
}

.path-chip:hover {
  border-color: var(--color-border-focus);
  background: var(--color-background-hover);
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

.columns-header {
  display: flex;
  align-items: baseline;
  gap: 8px;
}

.columns-count {
  font-size: 11px;
  color: var(--color-text-secondary, #888);
}

.columns-fold-all {
  padding: 0;
  border: none;
  background: none;
  font-size: 11px;
  cursor: pointer;
  color: var(--color-accent, #3b82f6);
}

.schema-preview {
  margin-top: 10px;
  border: 1px solid var(--color-border, #e0e0e0);
  border-radius: 4px;
  max-height: 360px;
  overflow: auto;
}

.schema-row {
  display: flex;
  align-items: baseline;
  gap: 6px;
  padding: 4px 10px;
  font-size: 12px;
  border-bottom: 1px solid var(--color-border, #eee);
}

.schema-row:last-child {
  border-bottom: none;
}

.schema-row.is-group {
  cursor: pointer;
}

.schema-row.is-group:hover {
  background: var(--color-background-hover);
}

.schema-toggle {
  flex: 0 0 14px;
  width: 14px;
  padding: 0;
  border: none;
  background: none;
  font-size: 9px;
  color: var(--color-text-secondary, #888);
  cursor: pointer;
}

.schema-toggle i {
  transition: transform 0.15s;
}

.schema-toggle i.is-open {
  transform: rotate(90deg);
}

.schema-name {
  font-family: var(--font-family-mono, monospace);
  white-space: nowrap;
}

.schema-type {
  margin-left: auto;
  padding-left: 12px;
  text-align: right;
  color: var(--color-text-secondary, #888);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
