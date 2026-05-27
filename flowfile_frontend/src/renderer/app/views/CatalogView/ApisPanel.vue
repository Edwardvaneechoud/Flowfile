<template>
  <div class="apis-panel">
    <div class="panel-header">
      <h2><i class="fa-solid fa-plug"></i> APIs</h2>
      <el-button size="small" :loading="loading" @click="load">
        <i class="fa-solid fa-arrows-rotate" /> Refresh
      </el-button>
    </div>
    <p class="panel-hint">
      Flows published as HTTP data APIs. Open a flow to manage its parameters, keys, or to test it.
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
            <el-button size="small" text title="Copy URL" @click.stop="copyUrl(baseUrl + row.path)">
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
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from "vue";
import { ElMessage } from "element-plus";
import { flowfileCorebaseURL } from "../../../config/constants";
import { FlowApiApi, type ApiEndpoint } from "../../api/flowApi.api";
import { EmptyState } from "../../components/common";

const emit = defineEmits<{ (e: "view-flow", registrationId: number): void }>();

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

onMounted(load);
</script>

<style scoped>
.apis-panel {
  padding: 16px;
}
.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
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
