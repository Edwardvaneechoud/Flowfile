<template>
  <div class="consumers-panel">
    <div class="header">
      <div>
        <h2>Consumers</h2>
        <p class="panel-hint">
          Service accounts that hold reusable API keys. Grant a consumer one or more published flows
          and a single key it holds can call all of them.
        </p>
      </div>
      <el-button size="small" :loading="loading" @click="loadConsumers">
        <i class="fa-solid fa-arrows-rotate" /> Refresh
      </el-button>
    </div>

    <!-- Create consumer -->
    <el-card class="create-card" shadow="never">
      <template #header><span>New consumer</span></template>
      <div class="create-row">
        <el-input v-model="newConsumer.name" placeholder="Name (e.g. partner-acme)" class="grow" />
        <el-input
          v-model="newConsumer.description"
          placeholder="Description (optional)"
          class="grow"
        />
        <el-button
          type="primary"
          :loading="creating"
          :disabled="!newConsumer.name.trim()"
          @click="createConsumer"
        >
          <i class="fa-solid fa-plus btn-icon" /> Create
        </el-button>
      </div>
    </el-card>

    <!-- Consumers table -->
    <el-table v-loading="loading" :data="consumers" class="consumers-table">
      <el-table-column prop="name" label="Name" min-width="160">
        <template #default="{ row }">
          <strong>{{ row.name }}</strong>
          <div v-if="row.description" class="muted">{{ row.description }}</div>
        </template>
      </el-table-column>
      <el-table-column label="Enabled" width="100" align="center">
        <template #default="{ row }">
          <el-switch
            :model-value="row.enabled"
            size="small"
            @change="(v: string | number | boolean) => toggleConsumer(row, Boolean(v))"
          />
        </template>
      </el-table-column>
      <el-table-column label="Flows" width="80" align="center">
        <template #default="{ row }">{{ row.endpoint_count }}</template>
      </el-table-column>
      <el-table-column label="Keys" width="80" align="center">
        <template #default="{ row }">{{ row.key_count }}</template>
      </el-table-column>
      <el-table-column label="Actions" width="180" align="right">
        <template #default="{ row }">
          <el-button size="small" @click="openManage(row)">Manage</el-button>
          <el-button size="small" type="danger" plain @click="deleteConsumer(row)"
            >Delete</el-button
          >
        </template>
      </el-table-column>
      <template #empty>
        <span class="muted">No consumers yet. Create one above to share keys across flows.</span>
      </template>
    </el-table>

    <!-- Manage drawer -->
    <el-drawer
      v-model="drawerOpen"
      :title="active?.name ?? 'Consumer'"
      size="520px"
      destroy-on-close
    >
      <div v-if="active" class="drawer-body">
        <!-- Granted flows -->
        <section class="drawer-section">
          <h3>Granted flows</h3>
          <p class="muted">The published flows this consumer's keys may call.</p>
          <el-select
            v-model="grantedIds"
            multiple
            filterable
            placeholder="Select flows to grant"
            class="full"
            :loading="drawerBusy"
            @change="syncGrants"
          >
            <el-option
              v-for="ep in availableEndpoints"
              :key="ep.id"
              :label="`${ep.flow_name ?? ep.slug} (/api/data/${ep.slug})`"
              :value="ep.id"
            />
          </el-select>
          <p v-if="availableEndpoints.length === 0" class="muted">
            No published flows yet — publish a flow's API first.
          </p>
        </section>

        <!-- Keys -->
        <section class="drawer-section">
          <div class="section-head">
            <h3>API keys</h3>
            <el-button size="small" :loading="drawerBusy" @click="createKey">
              <i class="fa-solid fa-key btn-icon" /> Create key
            </el-button>
          </div>

          <div v-if="newKey" class="new-key-box">
            <p>
              <strong>Copy this key now</strong> — it is shown only once. Send it in the
              <code>X-API-Key</code> header.
            </p>
            <div class="row">
              <code class="code-chip">{{ newKey }}</code>
              <el-button size="small" @click="copy(newKey)"
                ><i class="fa-solid fa-copy"
              /></el-button>
              <el-button size="small" text @click="newKey = null">Dismiss</el-button>
            </div>
          </div>

          <el-table :data="keys" size="small">
            <el-table-column prop="name" label="Name" min-width="120" />
            <el-table-column prop="key_prefix" label="Prefix" width="140" />
            <el-table-column label="Enabled" width="90" align="center">
              <template #default="{ row }">
                <el-switch
                  :model-value="row.enabled"
                  size="small"
                  @change="(v: string | number | boolean) => toggleKey(row, Boolean(v))"
                />
              </template>
            </el-table-column>
            <el-table-column label="Last used" min-width="130">
              <template #default="{ row }">{{
                row.last_used_at ? formatDate(row.last_used_at) : "Never"
              }}</template>
            </el-table-column>
            <el-table-column width="90" align="center">
              <template #default="{ row }">
                <el-button size="small" text type="danger" @click="revokeKey(row.id)"
                  >Revoke</el-button
                >
              </template>
            </el-table-column>
            <template #empty><span class="muted">No keys yet</span></template>
          </el-table>
        </section>
      </div>
    </el-drawer>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { ApiConsumersApi, type ApiConsumer } from "../../api/apiConsumers.api";
import type { ApiEndpoint, ApiKey } from "../../api/flowApi.api";
import { formatDate } from "./catalog-formatters";

const loading = ref(false);
const creating = ref(false);
const consumers = ref<ApiConsumer[]>([]);
const newConsumer = ref<{ name: string; description: string }>({ name: "", description: "" });

const drawerOpen = ref(false);
const drawerBusy = ref(false);
const active = ref<ApiConsumer | null>(null);
const availableEndpoints = ref<ApiEndpoint[]>([]);
const grantedIds = ref<number[]>([]);
const keys = ref<ApiKey[]>([]);
const newKey = ref<string | null>(null);

function detail(e: unknown, fallback: string): string {
  return (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? fallback;
}

async function loadConsumers() {
  loading.value = true;
  try {
    consumers.value = await ApiConsumersApi.listConsumers();
  } catch (e) {
    ElMessage.error(detail(e, "Failed to load consumers"));
  } finally {
    loading.value = false;
  }
}

async function createConsumer() {
  creating.value = true;
  try {
    await ApiConsumersApi.createConsumer({
      name: newConsumer.value.name.trim(),
      description: newConsumer.value.description.trim() || null,
    });
    newConsumer.value = { name: "", description: "" };
    ElMessage.success("Consumer created");
    await loadConsumers();
  } catch (e) {
    ElMessage.error(detail(e, "Failed to create consumer"));
  } finally {
    creating.value = false;
  }
}

async function toggleConsumer(row: ApiConsumer, enabled: boolean) {
  try {
    await ApiConsumersApi.updateConsumer(row.id, { enabled });
    row.enabled = enabled;
    ElMessage.success(enabled ? "Enabled" : "Disabled");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to update consumer"));
    await loadConsumers();
  }
}

async function deleteConsumer(row: ApiConsumer) {
  try {
    await ElMessageBox.confirm(
      `Delete consumer "${row.name}"? Its keys are revoked and its grants removed.`,
      "Delete consumer",
      { type: "warning" },
    );
  } catch {
    return;
  }
  try {
    await ApiConsumersApi.deleteConsumer(row.id);
    ElMessage.success("Consumer deleted");
    await loadConsumers();
  } catch (e) {
    ElMessage.error(detail(e, "Failed to delete consumer"));
  }
}

async function openManage(row: ApiConsumer) {
  active.value = row;
  drawerOpen.value = true;
  newKey.value = null;
  drawerBusy.value = true;
  try {
    const [available, granted, keyList] = await Promise.all([
      ApiConsumersApi.listAvailableEndpoints(row.id),
      ApiConsumersApi.listGrantedEndpoints(row.id),
      ApiConsumersApi.listKeys(row.id),
    ]);
    availableEndpoints.value = available;
    grantedIds.value = granted.map((e) => e.id);
    keys.value = keyList;
  } catch (e) {
    ElMessage.error(detail(e, "Failed to load consumer"));
  } finally {
    drawerBusy.value = false;
  }
}

/** Diff the multiselect against the server's grants and apply the delta. */
async function syncGrants(selected: number[]) {
  if (!active.value) return;
  const current = new Set(grantedIds.value);
  drawerBusy.value = true;
  try {
    // grantedIds was already mutated by v-model to `selected`; recompute from the server set.
    const granted = await ApiConsumersApi.listGrantedEndpoints(active.value.id);
    const serverIds = new Set(granted.map((e) => e.id));
    const toGrant = selected.filter((id) => !serverIds.has(id));
    const toRevoke = [...serverIds].filter((id) => !selected.includes(id));
    for (const id of toGrant) await ApiConsumersApi.grantEndpoint(active.value.id, id);
    for (const id of toRevoke) await ApiConsumersApi.revokeEndpoint(active.value.id, id);
    const refreshed = await ApiConsumersApi.listGrantedEndpoints(active.value.id);
    grantedIds.value = refreshed.map((e) => e.id);
    if (active.value) active.value.endpoint_count = refreshed.length;
  } catch (e) {
    ElMessage.error(detail(e, "Failed to update grants"));
    grantedIds.value = [...current];
  } finally {
    drawerBusy.value = false;
  }
}

async function createKey() {
  if (!active.value) return;
  let name: string;
  try {
    const res = await ElMessageBox.prompt("Name this key", "Create API key", {
      inputPlaceholder: "e.g. production",
    });
    name = (res.value ?? "").trim() || "key";
  } catch {
    return;
  }
  drawerBusy.value = true;
  try {
    const created = await ApiConsumersApi.createKey(active.value.id, name);
    newKey.value = created.api_key;
    keys.value = await ApiConsumersApi.listKeys(active.value.id);
    if (active.value) active.value.key_count = keys.value.length;
  } catch (e) {
    ElMessage.error(detail(e, "Failed to create key"));
  } finally {
    drawerBusy.value = false;
  }
}

async function toggleKey(row: ApiKey, enabled: boolean) {
  if (!active.value) return;
  try {
    await ApiConsumersApi.updateKey(active.value.id, row.id, { enabled });
    row.enabled = enabled;
  } catch (e) {
    ElMessage.error(detail(e, "Failed to update key"));
    if (active.value) keys.value = await ApiConsumersApi.listKeys(active.value.id);
  }
}

async function revokeKey(keyId: number) {
  if (!active.value) return;
  try {
    await ElMessageBox.confirm("Revoke this key? It stops working immediately.", "Revoke key", {
      type: "warning",
    });
  } catch {
    return;
  }
  try {
    await ApiConsumersApi.deleteKey(active.value.id, keyId);
    keys.value = keys.value.filter((k) => k.id !== keyId);
    if (active.value) active.value.key_count = keys.value.length;
    ElMessage.success("Key revoked");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to revoke key"));
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

onMounted(loadConsumers);
</script>

<style scoped>
.consumers-panel {
  /* Rendered inside the already-padded APIs panel. */
  padding: 0;
}
.header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 16px;
}
.header h2 {
  margin: 0;
  font-size: 18px;
}
.panel-hint {
  color: var(--color-text-secondary);
  font-size: 13px;
  line-height: 1.5;
  margin: 4px 0 0;
}
.create-card {
  margin-bottom: 16px;
}
.create-row {
  display: flex;
  gap: 8px;
  align-items: center;
}
.grow {
  flex: 1;
}
.consumers-table {
  width: 100%;
}
.muted {
  color: var(--color-text-secondary);
  font-size: 12px;
}
.drawer-body {
  display: flex;
  flex-direction: column;
  gap: 24px;
}
.drawer-section h3 {
  margin: 0 0 4px;
  font-size: 15px;
}
.section-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.full {
  width: 100%;
}
.row {
  display: flex;
  align-items: center;
  gap: 8px;
}
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
.code-chip {
  font-family: var(--font-mono, monospace);
  font-size: 12px;
  word-break: break-all;
}
</style>
