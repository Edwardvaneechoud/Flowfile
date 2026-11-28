<template>
  <div class="delta-sharing-panel" data-testid="delta-sharing-panel">
    <div class="header">
      <div>
        <h2>Delta Sharing</h2>
        <p class="panel-hint">
          Let other tools (pandas, Spark, Power BI, DuckDB, Databricks) read catalog tables over the
          open Delta Sharing protocol. A share holds tables; a consumer granted the share reads them
          with a profile file.
        </p>
      </div>
      <el-button size="small" :loading="loading" @click="load">
        <i class="fa-solid fa-arrows-rotate" /> Refresh
      </el-button>
    </div>

    <el-alert
      v-if="status && !status.enabled"
      type="warning"
      :closable="false"
      show-icon
      title="Delta Sharing is turned off on this server (FLOWFILE_DELTA_SHARING)."
      class="block"
    />

    <el-card v-else class="create-card" shadow="never">
      <template #header><span>New share</span></template>
      <div class="create-row">
        <el-input
          v-model="newShare.name"
          placeholder="Name (e.g. retail)"
          class="grow"
          data-testid="delta-share-name"
          @keyup.enter="createShare"
        />
        <el-input
          v-model="newShare.description"
          placeholder="Description (optional)"
          class="grow"
        />
        <el-button
          type="primary"
          :loading="creating"
          :disabled="!!newShareError"
          data-testid="delta-share-create"
          @click="createShare"
        >
          <i class="fa-solid fa-plus btn-icon" /> Create
        </el-button>
      </div>
      <p v-if="newShare.name && newShareError" class="field-error">{{ newShareError }}</p>
    </el-card>

    <el-table v-loading="loading" :data="shares" class="shares-table" data-testid="delta-shares">
      <el-table-column label="Share" min-width="160">
        <template #default="{ row }">
          <strong>{{ row.name }}</strong>
          <div v-if="row.description" class="muted">{{ row.description }}</div>
        </template>
      </el-table-column>
      <el-table-column label="Tables" width="90" align="center">
        <template #default="{ row }">{{ row.tables.length }}</template>
      </el-table-column>
      <el-table-column label="Recipients" width="110" align="center">
        <template #default="{ row }">{{ row.recipients.length }}</template>
      </el-table-column>
      <el-table-column label="Actions" width="180" align="right">
        <template #default="{ row }">
          <el-button size="small" @click="openManage(row)">Manage</el-button>
          <el-button size="small" type="danger" plain @click="deleteShare(row)">Delete</el-button>
        </template>
      </el-table-column>
      <template #empty>
        <span class="muted">No shares yet. Create one above, then add tables to it.</span>
      </template>
    </el-table>

    <el-drawer
      v-model="drawerOpen"
      :title="active ? `Share: ${active.name}` : 'Share'"
      size="600px"
      destroy-on-close
    >
      <div v-if="active" class="drawer-body" data-testid="delta-share-drawer">
        <section class="drawer-section">
          <h3>Tables</h3>
          <p class="muted">
            Recipients see each table as
            <code>{{ active.name }}.&lt;schema&gt;.&lt;table&gt;</code>. Only tables you manage can
            be added; a table stops being served if the share's owner loses that right.
          </p>
          <div class="row">
            <el-select
              v-model="tableToAdd"
              filterable
              placeholder="Add a catalog table"
              class="grow"
              :loading="eligibleLoading"
              data-testid="delta-share-table-select"
              @visible-change="(open: boolean) => open && loadEligible()"
            >
              <el-option
                v-for="t in offeredTables"
                :key="t.table_id"
                :label="t.catalog_name"
                :value="t.table_id"
                :disabled="!!t.refusal"
              >
                <span :title="t.refusal ?? ''">{{ t.catalog_name }}</span>
                <span v-if="t.refusal" class="option-note">cannot be shared</span>
              </el-option>
            </el-select>
            <el-button
              type="primary"
              :disabled="tableToAdd === null"
              :loading="busy"
              data-testid="delta-share-table-add"
              @click="addTable"
            >
              Add
            </el-button>
          </div>
          <el-table :data="active.tables" size="small" class="section-table">
            <el-table-column label="Catalog table" min-width="150">
              <template #default="{ row }">
                <span>{{ row.catalog_name }}</span>
                <div v-if="!row.served" class="refusal">{{ row.refusal }}</div>
              </template>
            </el-table-column>
            <el-table-column label="Shared as" min-width="150">
              <template #default="{ row }">
                <code class="code-chip">{{ row.schema_name }}.{{ row.table_name }}</code>
              </template>
            </el-table-column>
            <el-table-column label="History" width="80" align="center">
              <template #default="{ row }">
                <el-switch
                  :model-value="row.share_history"
                  size="small"
                  title="Let recipients read earlier versions"
                  @change="(v: string | number | boolean) => setHistory(row, Boolean(v))"
                />
              </template>
            </el-table-column>
            <el-table-column width="60" align="right">
              <template #default="{ row }">
                <el-button size="small" text type="danger" title="Remove" @click="removeTable(row)">
                  <i class="fa-solid fa-xmark" />
                </el-button>
              </template>
            </el-table-column>
            <template #empty><span class="muted">No tables in this share yet.</span></template>
          </el-table>
        </section>

        <section class="drawer-section">
          <h3>Recipients</h3>
          <p class="muted">
            API consumers that may read this share. Manage consumers under APIs → Consumers.
          </p>
          <div class="row">
            <el-select
              v-model="consumerToGrant"
              filterable
              placeholder="Grant to a consumer"
              class="grow"
              data-testid="delta-share-consumer-select"
            >
              <el-option
                v-for="c in grantableConsumers"
                :key="c.id"
                :label="c.enabled ? c.name : `${c.name} (disabled)`"
                :value="c.id"
              />
            </el-select>
            <el-button
              :disabled="consumerToGrant === null"
              :loading="busy"
              data-testid="delta-share-grant"
              @click="grant"
            >
              Grant
            </el-button>
          </div>
          <el-table :data="active.recipients" size="small" class="section-table">
            <el-table-column label="Consumer" min-width="160">
              <template #default="{ row }">
                {{ row.name }}
                <span v-if="!row.enabled" class="muted">(disabled)</span>
              </template>
            </el-table-column>
            <el-table-column width="250" align="right">
              <template #default="{ row }">
                <el-button
                  size="small"
                  :data-testid="`delta-share-profile-${row.consumer_id}`"
                  @click="openProfile(row)"
                >
                  <i class="fa-solid fa-file-arrow-down btn-icon" /> Profile file
                </el-button>
                <el-button size="small" text type="danger" @click="revoke(row)">Revoke</el-button>
              </template>
            </el-table-column>
            <template #empty><span class="muted">Nobody can read this share yet.</span></template>
          </el-table>
        </section>
      </div>
    </el-drawer>

    <el-dialog
      v-model="profileOpen"
      :title="profileFor ? `Profile file for ${profileFor.name}` : 'Profile file'"
      width="620px"
      align-center
      @closed="profile = null"
    >
      <div v-if="!profile" class="profile-form">
        <p class="muted">
          A profile file holds a new API key for this consumer. Anyone with the file can read every
          share the consumer is granted, until the key is disabled or expires.
        </p>
        <el-form label-position="top">
          <el-form-item label="Endpoint recipients reach this server at">
            <el-input
              v-model="profileEndpoint"
              :disabled="!!status?.endpoint"
              data-testid="delta-share-endpoint"
            >
              <template #append>/delta-sharing</template>
            </el-input>
            <p v-if="status?.endpoint" class="muted">Set by the server: {{ status.endpoint }}</p>
            <p v-else-if="endpointIsLoopback" class="field-warning">
              This address only works on this machine. Give the address other machines use (or set
              FLOWFILE_DELTA_SHARING_ENDPOINT on the server).
            </p>
          </el-form-item>
          <el-form-item label="Key expires after (days, empty = never)">
            <el-input-number v-model="profileDays" :min="1" :max="3650" controls-position="right" />
          </el-form-item>
        </el-form>
      </div>
      <div v-else class="profile-result" data-testid="delta-share-profile-result">
        <p>
          <strong>Save this file now</strong>: the key in it is shown only once (prefix
          <code>{{ profile.key_prefix }}</code
          >).
        </p>
        <pre class="code-block">{{ profileText }}</pre>
        <p class="muted">Read it from Python:</p>
        <pre class="code-block">{{ snippet }}</pre>
      </div>
      <template #footer>
        <template v-if="!profile">
          <el-button @click="profileOpen = false">Cancel</el-button>
          <el-button
            type="primary"
            :loading="busy"
            data-testid="delta-share-profile-create"
            @click="createProfile"
          >
            Create profile
          </el-button>
        </template>
        <template v-else>
          <el-button @click="copy(snippet)">Copy snippet</el-button>
          <el-button @click="copy(profileText)">Copy file</el-button>
          <el-button type="primary" data-testid="delta-share-profile-save" @click="saveProfile">
            <i class="fa-solid fa-download btn-icon" /> Save {{ profile.file_name }}
          </el-button>
        </template>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { flowfileCorebaseURL } from "../../../config/constants";
import { ApiConsumersApi, type ApiConsumer } from "../../api/apiConsumers.api";
import {
  DeltaSharingApi,
  type DeltaShare,
  type DeltaShareEligibleTable,
  type DeltaShareProfile,
  type DeltaShareRecipient,
  type DeltaShareTable,
  type DeltaSharingStatus,
} from "../../api/deltaSharing.api";
import { copyTextEverywhere } from "../../utils/clipboardUtils";
import { saveFile } from "../../utils/tableExport";
import {
  apiBase,
  isLoopbackEndpoint,
  pandasSnippet,
  profileFileText,
  shareNameError,
  tablesToOffer,
} from "./deltaSharing";

const status = ref<DeltaSharingStatus | null>(null);
const shares = ref<DeltaShare[]>([]);
const consumers = ref<ApiConsumer[]>([]);
const loading = ref(false);
const creating = ref(false);
const busy = ref(false);
const newShare = reactive({ name: "", description: "" });
const newShareError = computed(() => shareNameError(newShare.name));

const drawerOpen = ref(false);
const active = ref<DeltaShare | null>(null);
const eligible = ref<DeltaShareEligibleTable[]>([]);
const eligibleLoading = ref(false);
const tableToAdd = ref<number | null>(null);
const consumerToGrant = ref<number | null>(null);

const profileOpen = ref(false);
const profileFor = ref<DeltaShareRecipient | null>(null);
const profileEndpoint = ref(apiBase(flowfileCorebaseURL));
const profileDays = ref<number | undefined>(undefined);
const profile = ref<DeltaShareProfile | null>(null);

const offeredTables = computed(() =>
  tablesToOffer(eligible.value, active.value?.tables.map((t) => t.table_id) ?? []),
);
const grantableConsumers = computed(() => {
  const granted = new Set(active.value?.recipients.map((r) => r.consumer_id) ?? []);
  return consumers.value.filter((c) => !granted.has(c.id));
});
const endpointIsLoopback = computed(() => isLoopbackEndpoint(profileEndpoint.value));
const profileText = computed(() => (profile.value ? profileFileText(profile.value) : ""));
const snippet = computed(() => (profile.value ? pandasSnippet(profile.value) : ""));

function detailOf(e: unknown, fallback: string): string {
  const detail = (e as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  return typeof detail === "string" ? detail : fallback;
}

async function load() {
  loading.value = true;
  try {
    status.value = await DeltaSharingApi.status();
    if (!status.value.enabled) {
      shares.value = [];
      return;
    }
    const [list, consumerList] = await Promise.all([
      DeltaSharingApi.listShares(),
      ApiConsumersApi.listConsumers(),
    ]);
    shares.value = list;
    consumers.value = consumerList;
    if (active.value) {
      active.value = list.find((s) => s.id === active.value?.id) ?? null;
    }
  } catch (e) {
    ElMessage.error(detailOf(e, "Failed to load shares"));
  } finally {
    loading.value = false;
  }
}

function replaceShare(updated: DeltaShare) {
  active.value = updated;
  shares.value = shares.value.map((s) => (s.id === updated.id ? updated : s));
}

async function createShare() {
  if (newShareError.value) return;
  creating.value = true;
  try {
    const share = await DeltaSharingApi.createShare(
      newShare.name.trim(),
      newShare.description.trim() || null,
    );
    shares.value = [...shares.value, share].sort((a, b) => a.name.localeCompare(b.name));
    newShare.name = "";
    newShare.description = "";
    openManage(share);
  } catch (e) {
    ElMessage.error(detailOf(e, "Failed to create the share"));
  } finally {
    creating.value = false;
  }
}

async function deleteShare(share: DeltaShare) {
  try {
    await ElMessageBox.confirm(
      `Delete share "${share.name}"? Its recipients lose access at once; the tables are not touched.`,
      "Delete share",
      { type: "warning", confirmButtonText: "Delete" },
    );
  } catch {
    return;
  }
  try {
    await DeltaSharingApi.deleteShare(share.id);
    shares.value = shares.value.filter((s) => s.id !== share.id);
    if (active.value?.id === share.id) drawerOpen.value = false;
  } catch (e) {
    ElMessage.error(detailOf(e, "Failed to delete the share"));
  }
}

function openManage(share: DeltaShare) {
  active.value = share;
  tableToAdd.value = null;
  consumerToGrant.value = null;
  drawerOpen.value = true;
  loadEligible();
}

async function loadEligible() {
  eligibleLoading.value = true;
  try {
    eligible.value = await DeltaSharingApi.eligibleTables();
  } catch (e) {
    ElMessage.error(detailOf(e, "Failed to load catalog tables"));
  } finally {
    eligibleLoading.value = false;
  }
}

async function run(action: () => Promise<DeltaShare>, failure: string) {
  busy.value = true;
  try {
    replaceShare(await action());
    return true;
  } catch (e) {
    ElMessage.error(detailOf(e, failure));
    return false;
  } finally {
    busy.value = false;
  }
}

async function addTable() {
  const share = active.value;
  const tableId = tableToAdd.value;
  if (!share || tableId === null) return;
  if (
    await run(
      () => DeltaSharingApi.addTable(share.id, { table_id: tableId }),
      "Failed to add the table",
    )
  ) {
    tableToAdd.value = null;
  }
}

async function removeTable(row: DeltaShareTable) {
  const share = active.value;
  if (!share) return;
  await run(() => DeltaSharingApi.removeTable(share.id, row.id), "Failed to remove the table");
}

async function setHistory(row: DeltaShareTable, value: boolean) {
  const share = active.value;
  if (!share) return;
  await run(
    () => DeltaSharingApi.updateTable(share.id, row.id, { share_history: value }),
    "Failed to update the table",
  );
}

async function grant() {
  const share = active.value;
  const consumerId = consumerToGrant.value;
  if (!share || consumerId === null) return;
  if (await run(() => DeltaSharingApi.grant(share.id, consumerId), "Failed to grant the share")) {
    consumerToGrant.value = null;
  }
}

async function revoke(row: DeltaShareRecipient) {
  const share = active.value;
  if (!share) return;
  await run(() => DeltaSharingApi.revoke(share.id, row.consumer_id), "Failed to revoke");
}

function openProfile(row: DeltaShareRecipient) {
  profileFor.value = row;
  profile.value = null;
  profileDays.value = undefined;
  profileOpen.value = true;
}

async function createProfile() {
  const share = active.value;
  const recipient = profileFor.value;
  if (!share || !recipient) return;
  busy.value = true;
  try {
    profile.value = await DeltaSharingApi.createProfile(share.id, {
      consumer_id: recipient.consumer_id,
      endpoint: status.value?.endpoint ? undefined : profileEndpoint.value,
      expires_in_days: profileDays.value ?? null,
    });
  } catch (e) {
    ElMessage.error(detailOf(e, "Failed to create the profile"));
  } finally {
    busy.value = false;
  }
}

async function saveProfile() {
  if (!profile.value) return;
  const blob = new Blob([profileText.value], { type: "application/json" });
  await saveFile(blob, profile.value.file_name, {
    name: "Delta Sharing profile",
    extensions: ["share"],
  });
}

async function copy(text: string) {
  if (await copyTextEverywhere(text)) ElMessage.success("Copied");
  else ElMessage.error("Copy failed");
}

onMounted(load);
</script>

<style scoped>
.delta-sharing-panel {
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
.block,
.create-card {
  margin-bottom: 16px;
}
.create-row,
.row {
  display: flex;
  gap: 8px;
  align-items: center;
}
.grow {
  flex: 1;
}
.shares-table,
.section-table {
  width: 100%;
}
.section-table {
  margin-top: 8px;
}
.muted {
  color: var(--color-text-secondary);
  font-size: 12px;
}
.field-error,
.refusal {
  color: var(--color-danger, #c0392b);
  font-size: 12px;
  margin: 4px 0 0;
}
.field-warning {
  color: var(--color-warning, #b7791f);
  font-size: 12px;
  margin: 4px 0 0;
  line-height: 1.4;
}
.option-note {
  float: right;
  color: var(--color-text-secondary);
  font-size: 12px;
  margin-left: 12px;
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
.btn-icon {
  margin-right: 6px;
}
.code-chip {
  font-family: var(--font-mono, monospace);
  font-size: 12px;
  word-break: break-all;
}
.code-block {
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: 6px;
  padding: 10px 12px;
  font-family: var(--font-mono, monospace);
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-all;
  margin: 6px 0 12px;
}
</style>
