<template>
  <div class="apis-panel">
    <div class="apis-subnav">
      <el-radio-group v-model="view" size="small">
        <el-radio-button value="endpoints">Endpoints</el-radio-button>
        <el-radio-button value="consumers">Consumers</el-radio-button>
      </el-radio-group>
    </div>

    <!-- Published endpoints -->
    <div v-if="view === 'endpoints'">
      <div class="panel-header">
        <h2>Published APIs</h2>
        <div class="header-actions">
          <el-button size="small" type="primary" @click="openCreate">
            <i class="fa-solid fa-plus" /> Create API
          </el-button>
          <el-button size="small" :loading="loading" @click="load">
            <i class="fa-solid fa-arrows-rotate" /> Refresh
          </el-button>
        </div>
      </div>
      <p class="panel-hint">
        Flows published as HTTP data APIs. Open a flow to manage its parameters, keys, or to test
        it.
      </p>

      <EmptyState
        v-if="!loading && endpoints.length === 0"
        icon="fa-solid fa-plug"
        title="No published APIs"
        description="Open a flow in the catalog and use 'Expose as API' to publish one."
      />

      <el-table
        v-else
        :data="endpoints"
        size="small"
        @row-click="(row: ApiEndpoint) => emit('view-flow', row.registration_id)"
      >
        <el-table-column label="Flow" min-width="160">
          <template #default="{ row }">{{ row.flow_name ?? `#${row.registration_id}` }}</template>
        </el-table-column>
        <el-table-column label="URL" min-width="240">
          <template #default="{ row }">
            <!-- Stop propagation so selecting/copying the URL doesn't trigger the row's navigation. -->
            <div class="api-url-cell" @click.stop>
              <code class="api-url">{{ baseUrl }}{{ row.path }}</code>
              <el-button
                size="small"
                text
                title="Copy URL"
                @click.stop="copyUrl(baseUrl + row.path)"
              >
                <i class="fa-solid fa-copy" />
              </el-button>
            </div>
          </template>
        </el-table-column>
        <el-table-column label="Status" width="110">
          <template #default="{ row }">
            <span :class="['status-pill', row.enabled ? 'on' : 'off']">
              {{ row.enabled ? "Enabled" : "Disabled" }}
            </span>
          </template>
        </el-table-column>
        <el-table-column label="Params" width="90">
          <template #default="{ row }">{{ row.parameters.length }}</template>
        </el-table-column>
        <el-table-column width="100">
          <template #default="{ row }">
            <el-button
              size="small"
              text
              type="primary"
              @click.stop="emit('view-flow', row.registration_id)"
            >
              Manage
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <!-- Create API dialog -->
      <el-dialog v-model="createOpen" title="Create API" width="520px" align-center>
        <div v-loading="createLoading">
          <EmptyState
            v-if="!createLoading && publishable.length === 0"
            icon="fa-solid fa-plug"
            title="No flows ready to publish"
            description="All your API-ready flows are already published. Add an API response node to a flow and save it to make it publishable."
          />
          <el-form v-else label-position="top">
            <el-form-item label="Flow">
              <el-select
                v-model="selectedRegId"
                filterable
                placeholder="Select an API-ready flow"
                class="full"
                @change="onFlowSelected"
              >
                <el-option
                  v-for="f in publishable"
                  :key="f.registration_id"
                  :label="f.file_exists ? f.name : `${f.name} (file missing)`"
                  :value="f.registration_id"
                  :disabled="!f.file_exists"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="URL slug">
              <el-input v-model="slug" placeholder="e.g. sales">
                <template #prepend>/api/data/</template>
              </el-input>
            </el-form-item>
          </el-form>
        </div>
        <template #footer>
          <el-button @click="createOpen = false">Cancel</el-button>
          <el-button
            type="primary"
            :loading="publishing"
            :disabled="!selectedRegId || !slug.trim() || publishable.length === 0"
            @click="publish"
          >
            Publish
          </el-button>
        </template>
      </el-dialog>
    </div>

    <!-- API consumers (service accounts) -->
    <ApiConsumersPanel v-else />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useRoute } from "vue-router";
import { ElMessage } from "element-plus";
import { flowfileCorebaseURL } from "../../../config/constants";
import { FlowApiApi, type ApiEndpoint, type PublishableFlow } from "../../api/flowApi.api";
import { EmptyState } from "../../components/common";
import ApiConsumersPanel from "./ApiConsumersPanel.vue";

const emit = defineEmits<{ (e: "view-flow", registrationId: number): void }>();

const route = useRoute();
// Deep-link support: ?apiView=consumers opens the Consumers sub-view directly
// (used by a flow's API panel "Manage consumers" link).
const view = ref<"endpoints" | "consumers">(
  route.query.apiView === "consumers" ? "consumers" : "endpoints",
);

const endpoints = ref<ApiEndpoint[]>([]);
const loading = ref(false);
const baseUrl = flowfileCorebaseURL.replace(/\/$/, "");

async function load() {
  loading.value = true;
  try {
    endpoints.value = await FlowApiApi.listAllEndpoints();
  } catch {
    ElMessage.error("Failed to load APIs");
  } finally {
    loading.value = false;
  }
}

async function copyUrl(url: string) {
  try {
    await navigator.clipboard.writeText(url);
    ElMessage.success("URL copied");
  } catch {
    ElMessage.error("Copy failed");
  }
}

// --- Create API dialog ---
const createOpen = ref(false);
const createLoading = ref(false);
const publishing = ref(false);
const publishable = ref<PublishableFlow[]>([]);
const selectedRegId = ref<number | null>(null);
const slug = ref("");

/** Turn a flow name into a url-safe slug (the backend re-validates/normalizes). */
function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

async function openCreate() {
  createOpen.value = true;
  createLoading.value = true;
  selectedRegId.value = null;
  slug.value = "";
  try {
    publishable.value = await FlowApiApi.listPublishableFlows();
  } catch {
    ElMessage.error("Failed to load publishable flows");
  } finally {
    createLoading.value = false;
  }
}

function onFlowSelected(regId: number) {
  const flow = publishable.value.find((f) => f.registration_id === regId);
  if (flow) slug.value = slugify(flow.name);
}

async function publish() {
  if (!selectedRegId.value || !slug.value.trim()) return;
  publishing.value = true;
  try {
    const ep = await FlowApiApi.publishEndpoint({
      registration_id: selectedRegId.value,
      slug: slug.value.trim(),
    });
    createOpen.value = false;
    ElMessage.success("API published");
    // A fresh endpoint has no key yet (calls return 401), so drop the user into the
    // flow's API panel to mint a key and test it.
    emit("view-flow", ep.registration_id);
  } catch (e: unknown) {
    const detail = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
    ElMessage.error(detail ?? "Failed to publish");
  } finally {
    publishing.value = false;
  }
}

onMounted(load);
</script>

<style scoped>
.apis-panel {
  padding: 16px;
}
.apis-subnav {
  margin-bottom: 12px;
}
.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.panel-header h2 {
  font-size: 18px;
  margin: 0;
}
.header-actions {
  display: flex;
  gap: 8px;
}
.full {
  width: 100%;
}
.panel-hint {
  font-size: 13px;
  color: var(--color-text-secondary);
  margin: 4px 0 16px;
}
.api-url-cell {
  display: flex;
  align-items: center;
  gap: 6px;
}
.api-url {
  font-family: var(--font-mono, monospace);
  font-size: 12px;
  word-break: break-all;
  cursor: text;
  user-select: text;
}
.status-pill {
  font-size: 12px;
  padding: 1px 8px;
  border-radius: 10px;
}
.status-pill.on {
  background-color: var(--color-success-bg, #e6f4ea);
  color: var(--color-success, #1e7e34);
}
.status-pill.off {
  background-color: var(--color-background-secondary);
  color: var(--color-text-secondary);
}
</style>
