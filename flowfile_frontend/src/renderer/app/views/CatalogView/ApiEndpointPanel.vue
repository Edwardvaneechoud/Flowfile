<template>
  <CollapsibleSection
    title="Expose as API"
    icon="fa-solid fa-plug"
    persist-key="flow.api"
    :summary="endpoint ? 'Published' : 'Not published'"
  >
    <template v-if="endpoint" #actions>
      <el-button size="small" type="danger" plain :loading="busy" @click="unpublish">
        <i class="fa-solid fa-xmark btn-icon" /> Unpublish
      </el-button>
    </template>

    <div v-if="loading" class="api-loading">Loading…</div>

    <!-- Not published -->
    <div v-else-if="!endpoint" class="api-publish">
      <p class="api-hint">
        Publish this flow as a read-only HTTP endpoint. A <code>GET</code> runs the flow and returns
        the data flowing into its <strong>API response</strong> node. The flow must contain exactly
        one API response node.
      </p>
      <div class="api-row">
        <el-input
          v-model="slug"
          placeholder="url-slug (e.g. sales)"
          size="small"
          class="slug-input"
        />
        <el-button
          type="primary"
          size="small"
          :loading="busy"
          :disabled="!flow.file_exists || !flow.is_api_compatible || !slug"
          @click="publish"
        >
          <i class="fa-solid fa-plus btn-icon" /> Publish
        </el-button>
      </div>
      <p v-if="!flow.file_exists" class="api-warning">
        The flow file is missing on disk, so it cannot be published.
      </p>
      <p v-else-if="!flow.is_api_compatible" class="api-warning">
        This flow isn't API-compatible yet — add exactly one API response node and save the flow.
      </p>
    </div>

    <!-- Published -->
    <div v-else class="api-config">
      <div class="api-row api-status-row">
        <el-switch v-model="enabled" :loading="busy" size="small" @change="saveEndpoint" />
        <span class="status-label">{{ enabled ? "Enabled" : "Disabled" }}</span>
        <span class="endpoint-method">GET</span>
        <code class="code-chip">{{ fullUrl }}</code>
        <el-button size="small" text @click="copy(fullUrl)">
          <i class="fa-solid fa-copy" />
        </el-button>
      </div>

      <details class="curl-example">
        <summary>Example request</summary>
        <pre>{{ curlExample }}</pre>
      </details>

      <!-- Parameters -->
      <CollapsibleSection
        nested
        title="Query parameters"
        persist-key="flow.api.params"
        :count="params.length"
      >
        <template #actions>
          <el-button size="small" @click="addParam"
            ><i class="fa-solid fa-plus btn-icon" /> Add</el-button
          >
        </template>
        <p class="api-hint">
          Each parameter name must match a <code>${name}</code> reference in the flow. Values are
          validated by type before being substituted.
        </p>
        <el-table v-if="params.length" :data="params" size="small" class="param-table">
          <el-table-column label="Name" min-width="120">
            <template #default="{ row }">
              <el-input v-model="row.name" size="small" placeholder="region" />
            </template>
          </el-table-column>
          <el-table-column label="Type" width="120">
            <template #default="{ row }">
              <el-select v-model="row.type" size="small">
                <el-option v-for="t in PARAM_TYPES" :key="t" :label="t" :value="t" />
              </el-select>
            </template>
          </el-table-column>
          <el-table-column label="Required" width="90">
            <template #default="{ row }">
              <el-switch v-model="row.required" size="small" />
            </template>
          </el-table-column>
          <el-table-column label="Default" min-width="110">
            <template #default="{ row }">
              <el-input v-model="row.default" size="small" placeholder="(none)" />
            </template>
          </el-table-column>
          <el-table-column label="Enum values (comma-sep)" min-width="160">
            <template #default="{ row }">
              <el-input
                v-model="row.enum_values_text"
                size="small"
                :disabled="row.type !== 'enum'"
                placeholder="a,b,c"
              />
            </template>
          </el-table-column>
          <el-table-column width="56" align="center">
            <template #default="{ $index }">
              <el-button
                class="param-delete-btn"
                size="small"
                text
                type="danger"
                title="Remove parameter"
                @click="params.splice($index, 1)"
              >
                <i class="fa-solid fa-trash" />
              </el-button>
            </template>
          </el-table-column>
        </el-table>
        <el-button
          type="primary"
          size="small"
          :loading="busy"
          class="save-btn"
          @click="saveEndpoint"
        >
          Save changes
        </el-button>
      </CollapsibleSection>

      <!-- API keys -->
      <CollapsibleSection
        nested
        title="API keys"
        persist-key="flow.api.keys"
        :default-open="false"
        :count="keys.length"
      >
        <template #actions>
          <el-button size="small" :loading="busy" @click="createKey">
            <i class="fa-solid fa-key btn-icon" /> Create key
          </el-button>
        </template>

        <div v-if="newKey" class="new-key-box">
          <p>
            <strong>Copy this key now</strong> — it is shown only once. Send it in the
            <code>X-API-Key</code> header.
          </p>
          <div class="api-row">
            <code class="code-chip">{{ newKey }}</code>
            <el-button size="small" @click="copy(newKey)"><i class="fa-solid fa-copy" /></el-button>
            <el-button size="small" text @click="newKey = null">Dismiss</el-button>
          </div>
        </div>

        <EmptyState v-if="keys.length === 0" icon="fa-solid fa-key" description="No API keys yet" />
        <el-table v-else :data="keys" size="small">
          <el-table-column prop="name" label="Name" min-width="120" />
          <el-table-column prop="key_prefix" label="Prefix" width="140" />
          <el-table-column label="Last used" min-width="140">
            <template #default="{ row }">
              {{ row.last_used_at ? formatDate(row.last_used_at) : "Never" }}
            </template>
          </el-table-column>
          <el-table-column width="96" align="center">
            <template #default="{ row }">
              <el-button
                class="key-revoke-btn"
                size="small"
                text
                type="danger"
                @click="revokeKey(row.id)"
                >Revoke</el-button
              >
            </template>
          </el-table-column>
        </el-table>
      </CollapsibleSection>

      <!-- Try it -->
      <CollapsibleSection nested title="Try it" persist-key="flow.api.tryit" :default-open="false">
        <template #actions>
          <el-button size="small" type="primary" :loading="testing" @click="runTest">
            <i class="fa-solid fa-play btn-icon" /> Run test
          </el-button>
        </template>
        <p class="api-hint">
          Runs the flow as you (no key needed) with the values below, exactly as the public endpoint
          would.
        </p>
        <div v-for="p in endpoint.parameters" :key="p.name" class="api-row test-param-row">
          <label class="test-param-label">{{ p.name }}<span v-if="p.required"> *</span></label>
          <el-input
            v-model="testValues[p.name]"
            size="small"
            :placeholder="p.default ?? p.type"
            class="test-param-input"
          />
        </div>
        <div class="test-request">
          <span class="test-request-label">Request</span>
          <code class="code-chip">{{ testRequestUrl }}</code>
          <el-button size="small" text @click="copy(testRequestUrl.replace(/^GET /, ''))">
            <i class="fa-solid fa-copy" />
          </el-button>
        </div>
        <p v-if="testError" class="api-warning">{{ testError }}</p>
        <div v-if="testResult" class="test-result">
          <span class="test-result-meta">{{ testResult.row_count }} rows</span>
          <pre>{{ JSON.stringify(testResult.data, null, 2) }}</pre>
        </div>
      </CollapsibleSection>
    </div>
  </CollapsibleSection>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { flowfileCorebaseURL } from "../../../config/constants";
import {
  FlowApiApi,
  type ApiEndpoint,
  type ApiKey,
  type ApiParamType,
  type ApiTestResult,
  type FlowParamInfo,
} from "../../api/flowApi.api";
import type { FlowRegistration } from "../../types";
import { formatDate } from "./catalog-formatters";
import { CollapsibleSection, EmptyState } from "../../components/common";

interface ParamRow {
  name: string;
  type: ApiParamType;
  required: boolean;
  default: string;
  enum_values_text: string;
}

const PARAM_TYPES: ApiParamType[] = ["string", "integer", "float", "boolean", "enum"];

const props = defineProps<{ flow: FlowRegistration }>();

const loading = ref(false);
const busy = ref(false);
const endpoint = ref<ApiEndpoint | null>(null);
const keys = ref<ApiKey[]>([]);
const slug = ref("");
const enabled = ref(true);
const params = ref<ParamRow[]>([]);
const newKey = ref<string | null>(null);

const testValues = ref<Record<string, string>>({});
const testResult = ref<ApiTestResult | null>(null);
const testError = ref<string | null>(null);
const testing = ref(false);

const fullUrl = computed(() => `${flowfileCorebaseURL}api/data/${endpoint.value?.slug ?? ""}`);
const curlExample = computed(() => `curl -H "X-API-Key: <your-key>" "${fullUrl.value}"`);

// The equivalent public GET request for the current "Try it" values.
const testRequestUrl = computed(() => {
  const qs = new URLSearchParams();
  for (const p of endpoint.value?.parameters ?? []) {
    const v = testValues.value[p.name];
    if (v !== undefined && v !== "") qs.append(p.name, v);
  }
  const query = qs.toString();
  return `GET ${fullUrl.value}${query ? `?${query}` : ""}`;
});

/** Ensure every flow ${name} parameter appears as a row, preserving existing edits. */
function mergeFlowParams(rows: ParamRow[], flowParams: FlowParamInfo[]): ParamRow[] {
  const existing = new Set(rows.map((r) => r.name));
  const added: ParamRow[] = flowParams
    .filter((p) => !existing.has(p.name))
    .map((p) => ({
      name: p.name,
      type: "string" as ApiParamType,
      required: false,
      default: p.default ?? "",
      enum_values_text: "",
    }));
  return [...rows, ...added];
}

function toRows(ep: ApiEndpoint): ParamRow[] {
  return ep.parameters.map((p) => ({
    name: p.name,
    type: p.type,
    required: p.required,
    default: p.default ?? "",
    enum_values_text: (p.enum_values ?? []).join(","),
  }));
}

function fromRows(rows: ParamRow[]) {
  return rows
    .filter((r) => r.name.trim())
    .map((r) => ({
      name: r.name.trim(),
      type: r.type,
      required: r.required,
      default: r.default === "" ? null : r.default,
      enum_values:
        r.type === "enum"
          ? r.enum_values_text
              .split(",")
              .map((v) => v.trim())
              .filter(Boolean)
          : null,
    }));
}

function syncFromEndpoint(ep: ApiEndpoint | null) {
  endpoint.value = ep;
  if (ep) {
    slug.value = ep.slug;
    enabled.value = ep.enabled;
    params.value = toRows(ep);
  }
}

async function load() {
  loading.value = true;
  newKey.value = null;
  testResult.value = null;
  testError.value = null;
  try {
    const [ep, flowParams] = await Promise.all([
      FlowApiApi.getEndpointForFlow(props.flow.id),
      FlowApiApi.getFlowParameters(props.flow.id).catch(() => [] as FlowParamInfo[]),
    ]);
    syncFromEndpoint(ep);
    if (ep) {
      keys.value = await FlowApiApi.listKeys(ep.id);
    } else {
      keys.value = [];
      slug.value = "";
      params.value = [];
    }
    // Pre-populate any flow ${name} parameter that isn't already configured.
    params.value = mergeFlowParams(params.value, flowParams);
  } catch {
    ElMessage.error("Failed to load API endpoint");
  } finally {
    loading.value = false;
  }
}

async function publish() {
  busy.value = true;
  try {
    const ep = await FlowApiApi.publishEndpoint({
      registration_id: props.flow.id,
      slug: slug.value,
      enabled: true,
      parameters: fromRows(params.value),
    });
    syncFromEndpoint(ep);
    keys.value = [];
    ElMessage.success("Flow published as API");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to publish");
  } finally {
    busy.value = false;
  }
}

async function saveEndpoint() {
  if (!endpoint.value) return;
  busy.value = true;
  try {
    const ep = await FlowApiApi.updateEndpoint(endpoint.value.id, {
      slug: slug.value,
      enabled: enabled.value,
      parameters: fromRows(params.value),
    });
    syncFromEndpoint(ep);
    ElMessage.success("Saved");
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail ?? "Failed to save");
  } finally {
    busy.value = false;
  }
}

async function unpublish() {
  if (!endpoint.value) return;
  try {
    await ElMessageBox.confirm(
      "Unpublish this API? Its keys will be deleted and the URL will stop working.",
      "Unpublish API",
      { type: "warning" },
    );
  } catch {
    return;
  }
  busy.value = true;
  try {
    await FlowApiApi.deleteEndpoint(endpoint.value.id);
    endpoint.value = null;
    keys.value = [];
    slug.value = "";
    params.value = [];
    ElMessage.success("Unpublished");
  } catch {
    ElMessage.error("Failed to unpublish");
  } finally {
    busy.value = false;
  }
}

function addParam() {
  params.value.push({
    name: "",
    type: "string",
    required: false,
    default: "",
    enum_values_text: "",
  });
}

async function createKey() {
  if (!endpoint.value) return;
  let name: string;
  try {
    const res = await ElMessageBox.prompt("Name this key", "Create API key", {
      inputPlaceholder: "e.g. production",
    });
    name = (res.value ?? "").trim() || "key";
  } catch {
    return;
  }
  busy.value = true;
  try {
    const created = await FlowApiApi.createKey(endpoint.value.id, name);
    newKey.value = created.api_key;
    keys.value = await FlowApiApi.listKeys(endpoint.value.id);
  } catch {
    ElMessage.error("Failed to create key");
  } finally {
    busy.value = false;
  }
}

async function revokeKey(keyId: number) {
  if (!endpoint.value) return;
  try {
    await ElMessageBox.confirm("Revoke this key? It will stop working immediately.", "Revoke key", {
      type: "warning",
    });
  } catch {
    return;
  }
  try {
    await FlowApiApi.deleteKey(endpoint.value.id, keyId);
    keys.value = keys.value.filter((k) => k.id !== keyId);
    ElMessage.success("Key revoked");
  } catch {
    ElMessage.error("Failed to revoke key");
  }
}

async function runTest() {
  if (!endpoint.value) return;
  testing.value = true;
  testError.value = null;
  testResult.value = null;
  try {
    const sent: Record<string, string> = {};
    for (const p of endpoint.value.parameters) {
      const v = testValues.value[p.name];
      if (v !== undefined && v !== "") sent[p.name] = v;
    }
    testResult.value = await FlowApiApi.testEndpoint(endpoint.value.id, sent);
  } catch (e: any) {
    testError.value = e?.response?.data?.detail ?? "Test run failed";
  } finally {
    testing.value = false;
  }
}

async function copy(text: string) {
  try {
    await navigator.clipboard.writeText(text);
    ElMessage.success("Copied");
  } catch {
    ElMessage.error("Copy failed");
  }
}

watch(() => props.flow.id, load, { immediate: true });
</script>

<style scoped>
.api-loading {
  color: var(--color-text-secondary);
  font-size: 13px;
}
.api-hint {
  font-size: 12px;
  color: var(--color-text-secondary);
  line-height: 1.5;
  margin: 8px 0;
}
.api-warning {
  font-size: 12px;
  color: var(--color-danger, #c0392b);
}
.api-row {
  display: flex;
  align-items: center;
  gap: 8px;
}
.slug-input {
  max-width: 240px;
}
.api-status-row {
  flex-wrap: wrap;
  margin: 8px 0;
}
.status-label {
  font-size: 13px;
}
.endpoint-method {
  font-weight: 600;
  font-size: 12px;
  color: #2c7be5;
  border: 1px solid #2c7be5;
  border-radius: 4px;
  padding: 1px 6px;
}
.curl-example {
  margin: 8px 0;
  font-size: 12px;
}
.curl-example pre {
  background-color: var(--color-background-secondary);
  padding: 8px;
  border-radius: 4px;
  overflow-x: auto;
}
.save-btn {
  margin-top: 10px;
}
.param-table {
  margin-top: 8px;
}
/* Compact table action buttons: drop the global small-button padding so they
   don't overflow/clip inside their narrow action columns. */
.param-delete-btn.el-button--small {
  padding: 4px;
  min-width: 0;
}
.key-revoke-btn.el-button--small {
  padding: 4px 6px;
  min-width: 0;
}
/* Spacing between a leading icon and its button label. */
.btn-icon {
  margin-right: 6px;
}
.new-key-box {
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  padding: 10px 12px;
  margin: 8px 0;
  font-size: 12px;
}
.test-param-row {
  margin-top: 6px;
}
.test-param-label {
  min-width: 120px;
  font-size: 13px;
}
.test-param-input {
  max-width: 260px;
}
.test-result {
  margin-top: 10px;
}
.test-result-meta {
  font-size: 12px;
  color: var(--color-text-secondary);
}
.test-request {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 10px;
}
.test-request-label {
  font-size: 12px;
  color: var(--color-text-secondary);
}
.test-result pre {
  background-color: var(--color-background-secondary);
  padding: 8px;
  border-radius: 4px;
  max-height: 320px;
  overflow: auto;
  font-size: 12px;
}
</style>
