<template>
  <GenericNodeSettings
    v-if="settings"
    v-model="settings"
    @update:model-value="handleGenericSettingsUpdate"
    @request-save="saveSettings"
  >
    <el-form-item label="Saved connection">
      <div class="connection-row">
        <el-select
          v-model="settings.connection_id"
          :loading="loading"
          placeholder="Select a connection"
        >
          <el-option
            v-for="connection in connections"
            :key="connection.id"
            :label="connection.connection_name"
            :value="connection.id"
          />
        </el-select>
        <el-button :disabled="busy" @click="showCreate = true">New SharePoint connection</el-button>
      </div>
    </el-form-item>
    <p v-if="loadError" role="alert">Could not load saved SharePoint connections.</p>
    <p v-else-if="!loading && !connections.length">
      No saved SharePoint connections are available.
    </p>
    <fieldset class="browse-section" :disabled="!settings.connection_id">
      <legend>Browse SharePoint</legend>
      <el-form-item label="Site URL">
        <el-input
          v-model="siteUrl"
          placeholder="https://example.sharepoint.com/sites/Finance"
          @input="resetBrowse"
        />
      </el-form-item>
      <el-button :disabled="!settings.connection_id || !siteUrl.trim()" @click="loadLibraries">
        Find libraries
      </el-button>
      <el-form-item v-if="libraries.length" label="Document library">
        <el-select v-model="browseDrive" placeholder="Choose a library" @change="openLibrary">
          <el-option
            v-for="library in libraries"
            :key="library.id"
            :label="library.name"
            :value="library.id"
          />
        </el-select>
      </el-form-item>
      <div v-if="browseDrive">
        <el-button :disabled="!folders.length || browsing" @click="loadFolder(folders.slice(0, -1))"
          >Up one folder</el-button
        >
        <span>{{ folders.map(({ name }) => name).join(" / ") || "Library root" }}</span>
        <ul>
          <li v-for="entry in entries" :key="entry.id">
            <el-button link :disabled="browsing" @click="pickEntry(entry)"
              >{{ entry.name }}{{ entry.is_folder ? "/" : "" }}</el-button
            >
          </li>
        </ul>
      </div>
      <p v-if="browsing" role="status">Loading SharePoint…</p>
      <p v-if="browseMessage" role="status">{{ browseMessage }}</p>
    </fieldset>
    <el-form-item label="File path">
      <el-input v-model="settings.file_path" placeholder="reports/sales.xlsx" />
    </el-form-item>
    <el-form-item v-if="/\.csv$/i.test(settings.file_path.trim())" label="CSV delimiter">
      <el-input :model-value="settings.csv_delimiter ?? ','" maxlength="1"
        @update:model-value="settings.csv_delimiter = $event || ','" />
    </el-form-item>
    <el-form-item v-if="isExcel" label="Sheet name">
      <DropDown
        :key="sheetRequest"
        :model-value="settings.sheet_name ?? ''"
        label="Sheet name"
        placeholder="First sheet (default)"
        :column-options="sheetNames"
        :is-loading="loadingSheets"
        @update:model-value="settings.sheet_name = $event || null"
      />
      <el-button
        :disabled="!settings.connection_id || !validDrive || loadingSheets"
        @click="loadSheets"
      >
        Refresh File Metadata
      </el-button>
      <span v-if="fileMetadata">{{ fileMetadata.file_path }}</span>
      <span v-if="fileMetadata && !sheetNames.length" role="status">
        Worksheet names are unavailable from file metadata. Enter a sheet name or run the node.
      </span>
      <span v-if="missingSheet"
        >The selected worksheet is missing. Choose a worksheet to replace it.</span
      >
    </el-form-item>
    <details class="advanced-settings">
      <summary>Advanced connection settings</summary>
      <el-form-item label="Drive ID">
        <el-input v-model="settings.drive_id" />
      </el-form-item>
      <el-button :disabled="!settings.connection_id || !validDrive || busy" @click="testConnection">
        Test connection
      </el-button>
    </details>
  </GenericNodeSettings>
  <el-dialog v-model="showCreate" title="New SharePoint connection" @closed="clearConnectionDraft">
    <el-form label-position="top" @submit.prevent="createConnection">
      <el-form-item label="Connection name"><el-input v-model="connectionName" /></el-form-item>
      <el-form-item label="Tenant ID"><el-input v-model="tenantId" /></el-form-item>
      <el-form-item label="Application (client) ID"><el-input v-model="clientId" /></el-form-item>
      <el-form-item label="Client secret">
        <el-input v-model="clientSecret" type="password" autocomplete="new-password" />
      </el-form-item>
      <el-form-item label="Default site URL (optional)">
        <el-input v-model="defaultSiteUrl" maxlength="2048" />
      </el-form-item>
      <el-button type="primary" native-type="submit" :disabled="!canCreate || busy">
        Save connection
      </el-button>
    </el-form>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { isAxiosError } from "axios";
import { ElMessage } from "element-plus";
import DropDown from "../../../baseNode/page_objects/dropDown.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import axios from "../../../../../services/axios.config";
import { useNodeStore } from "../../../../../stores/node-store";
import type { NodeBase } from "../../../../../types/node.types";

interface SharePointSettings extends NodeBase {
  csv_delimiter?: string;
  connection_id: number | null;
  drive_id: string;
  file_path: string;
  sheet_name: string | null;
}

interface SavedConnection {
  id: number;
  connection_name: string;
  default_site_url: string | null;
}

const store = useNodeStore();
const settings = ref<SharePointSettings | null>(null);
const connections = ref<SavedConnection[]>([]);
const loading = ref(false);
const loadError = ref(false);
const busy = ref(false);
const showCreate = ref(false);
const connectionName = ref("");
const tenantId = ref("");
const clientId = ref("");
const clientSecret = ref("");
const defaultSiteUrl = ref("");
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const canCreate = computed(() =>
  Boolean(
    connectionName.value.trim() &&
    clientSecret.value &&
    uuid.test(tenantId.value.trim()) &&
    uuid.test(clientId.value.trim()),
  ),
);
const validDrive = computed(() => {
  const drive = settings.value?.drive_id.trim() ?? "";
  return /^[A-Za-z0-9_!.-]{1,512}$/.test(drive) && drive !== "." && drive !== "..";
});
const isExcel = computed(() => /\.xlsx$/i.test(settings.value?.file_path.trim() ?? ""));
interface FileMetadata {
  file_name: string;
  file_path: string;
  worksheet_names: string[] | null;
  version: [string, string] | null;
}
const fileMetadata = ref<FileMetadata | null>(null);
const sheetNames = ref<string[]>([]);
const loadingSheets = ref(false);
const sheetRequest = ref(0);
watch(
  () =>
    [
      settings.value,
      settings.value?.connection_id,
      settings.value?.drive_id,
      settings.value?.file_path,
    ] as const,
  ([draft], [previousDraft]) => {
    sheetRequest.value++;
    sheetNames.value = [];
    fileMetadata.value = null;
    loadingSheets.value = false;
    // A replacement draft carries its own saved sheet; edits to its target invalidate it.
    if (draft && draft === previousDraft) draft.sheet_name = null;
  },
  { flush: "sync" },
);

const missingSheet = computed(() => {
  const name = settings.value?.sheet_name;
  return name && sheetNames.value.length > 0 && !sheetNames.value.includes(name);
});

async function loadSheets() {
  const draft = settings.value;
  if (!draft?.connection_id || !validDrive.value || !isExcel.value || loadingSheets.value) return;
  const request = sheetRequest.value;
  loadingSheets.value = true;
  try {
    const { data } = await axios.get<FileMetadata>(
      `/sharepoint_connections/${draft.connection_id}/drives/${encodeURIComponent(draft.drive_id.trim())}/worksheets`,
      {
        params: {
          file_path: draft.file_path.trim(),
        },
      },
    );
    if (request === sheetRequest.value) {
      const previous = fileMetadata.value?.version;
      const unchanged = previous && data.version &&
        previous[0] === data.version[0] && previous[1] === data.version[1];
      const names = data.worksheet_names ?? (unchanged ? fileMetadata.value?.worksheet_names : null) ?? null;
      fileMetadata.value = { ...data, worksheet_names: names };
      sheetNames.value = names ?? [];
    }
  } catch (error) {
    if (request === sheetRequest.value) {
      fileMetadata.value = null;
      sheetNames.value = [];
      const detail = isAxiosError<{ detail?: unknown }>(error) && error.response?.data?.detail;
      ElMessage.error(
        typeof detail === "string" ? detail : "Could not refresh file metadata. Try again.",
      );
    }
  } finally {
    if (request === sheetRequest.value) loadingSheets.value = false;
  }
}

const supportedFile = /\.(csv|xlsx|parquet|json)$/i;

interface Library {
  id: string;
  name: string;
}
interface DriveEntry extends Library {
  is_folder: boolean;
}
const deploymentSiteUrl = ref("");
const siteOverride = ref<string | null>(null);
const siteUrl = computed({
  get: () =>
    siteOverride.value ??
    connections.value.find(({ id }) => id === settings.value?.connection_id)?.default_site_url ??
    deploymentSiteUrl.value,
  set: (value: string) => {
    siteOverride.value = value;
  },
});
const libraries = ref<Library[]>([]);
const browseDrive = ref("");
const folders = ref<DriveEntry[]>([]);
const entries = ref<DriveEntry[]>([]);
const browsing = ref(false);
const browseMessage = ref("");
let browseRequest = 0;

function resetBrowse() {
  browseRequest++;
  libraries.value = [];
  browseDrive.value = "";
  folders.value = [];
  entries.value = [];
  browsing.value = false;
  browseMessage.value = "";
}
watch(
  () => [settings.value, settings.value?.connection_id] as const,
  ([draft, connectionId], [previousDraft, previousConnectionId]) => {
    siteOverride.value = null;
    resetBrowse();
    if (draft && draft === previousDraft && connectionId !== previousConnectionId) {
      draft.drive_id = "";
      draft.file_path = "";
    }
  },
  { flush: "sync" },
);

async function loadLibraries() {
  const id = settings.value?.connection_id;
  if (!id || !siteUrl.value.trim()) return;
  resetBrowse();
  const request = browseRequest;
  browsing.value = true;
  try {
    const { data } = await axios.get<Library[]>(`/sharepoint_connections/${id}/libraries`, {
      params: { site_url: siteUrl.value.trim() },
    });
    if (request !== browseRequest) return;
    libraries.value = data;
    if (!data.length) browseMessage.value = "No document libraries found.";
  } catch {
    if (request === browseRequest)
      browseMessage.value =
        "Could not list libraries. Check the site URL and connection permissions.";
  } finally {
    if (request === browseRequest) browsing.value = false;
  }
}

function openLibrary() {
  folders.value = [];
  entries.value = [];
  void loadFolder([]);
}

async function loadFolder(path: DriveEntry[]) {
  const id = settings.value?.connection_id;
  const drive = browseDrive.value;
  if (!id || !drive) return;
  const request = ++browseRequest;
  browsing.value = true;
  browseMessage.value = "";
  try {
    const { data } = await axios.get<DriveEntry[]>(
      `/sharepoint_connections/${id}/drives/${encodeURIComponent(drive)}/children`,
      {
        params: { folder_id: path[path.length - 1]?.id },
      },
    );
    if (request !== browseRequest) return;
    folders.value = path;
    entries.value = data.filter((entry) => entry.is_folder || supportedFile.test(entry.name));
    if (!entries.value.length) browseMessage.value = "No folders or supported files found.";
  } catch {
    if (request === browseRequest)
      browseMessage.value = "Could not list the folder. Check the connection permissions.";
  } finally {
    if (request === browseRequest) browsing.value = false;
  }
}

function pickEntry(entry: DriveEntry) {
  const path = [...folders.value, entry];
  if (entry.is_folder) {
    void loadFolder(path);
  } else if (settings.value) {
    const filePath = path.map(({ name }) => name).join("/");
    settings.value.drive_id = browseDrive.value;
    settings.value.file_path = filePath;
  }
}

function clearConnectionDraft() {
  connectionName.value =
    tenantId.value =
    clientId.value =
    clientSecret.value =
    defaultSiteUrl.value =
      "";
}

async function createConnection() {
  const draft = settings.value;
  if (!draft || !canCreate.value || busy.value) return;
  busy.value = true;
  try {
    const { data } = await axios.post<SavedConnection>("/sharepoint_connections", {
      connection_name: connectionName.value.trim(),
      tenant_id: tenantId.value.trim(),
      client_id: clientId.value.trim(),
      client_secret: clientSecret.value,
      default_site_url: defaultSiteUrl.value.trim() || null,
    });
    connections.value = [...connections.value, data];
    if (settings.value === draft) draft.connection_id = data.id;
    showCreate.value = false;
    ElMessage.success("SharePoint connection saved.");
  } catch {
    ElMessage.error("Could not save the SharePoint connection.");
  } finally {
    clientSecret.value = "";
    busy.value = false;
  }
}

async function testConnection() {
  const draft = settings.value;
  if (!draft?.connection_id || !validDrive.value || busy.value) return;
  busy.value = true;
  try {
    await axios.post("/sharepoint_connections/test", {
      connection_id: draft.connection_id,
      drive_id: draft.drive_id.trim(),
    });
    ElMessage.success("SharePoint drive is accessible.");
  } catch {
    ElMessage.error(
      "Could not access the SharePoint drive. Check the drive ID and app permissions.",
    );
  } finally {
    busy.value = false;
  }
}

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: settings,
  onBeforeSave() {
    const draft = settings.value;
    if (!draft || !connections.value.some(({ id }) => id === draft.connection_id)) {
      ElMessage.error("Select a saved SharePoint connection.");
      return false;
    }
    if (!validDrive.value || !supportedFile.test(draft.file_path.trim())) {
      ElMessage.error("Enter a drive ID and the path to a CSV, Excel, Parquet, or JSON file.");
      return false;
    }
    draft.drive_id = draft.drive_id.trim();
    draft.file_path = draft.file_path.trim();
    draft.sheet_name = isExcel.value && draft.sheet_name ? draft.sheet_name : null;
    return true;
  },
});

async function loadNodeData(nodeId: number) {
  resetBrowse();
  siteOverride.value = null;
  deploymentSiteUrl.value = "";
  const node = await store.getNodeData(nodeId, false);
  settings.value = node?.setting_input?.is_setup
    ? (node.setting_input as SharePointSettings)
    : {
        flow_id: store.flow_id,
        node_id: nodeId,
        cache_results: false,
        pos_x: 0,
        pos_y: 0,
        connection_id: null,
        drive_id: "",
        file_path: "",
        sheet_name: null,
      };
  const draft = settings.value;
  const { connection_id, drive_id, file_path } = draft;
  await nextTick();
  if (
    settings.value === draft &&
    draft.connection_id === connection_id &&
    draft.drive_id === drive_id &&
    draft.file_path === file_path
  ) {
    sheetNames.value = node?.sharepoint_worksheet_names ?? [];
  }
  void axios
    .get<{ site_url: string | null }>("/sharepoint_connections/browse_defaults")
    .then(({ data }) => {
      if (settings.value === draft) deploymentSiteUrl.value = data.site_url ?? "";
    })
    .catch(() => {
      /* Manual site entry remains available. */
    });
  loading.value = true;
  loadError.value = false;
  try {
    connections.value = (await axios.get<SavedConnection[]>("/sharepoint_connections")).data;
  } catch {
    connections.value = [];
    loadError.value = true;
  } finally {
    loading.value = false;
  }
}

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>

<style scoped>
.connection-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  width: 100%;
}
.connection-row .el-select {
  flex: 1 1 140px;
}
.browse-section {
  border: 0;
  margin: 0 0 16px;
  min-inline-size: 0;
  padding: 0 0 12px;
  border-bottom: 1px solid var(--el-border-color-light);
}
.browse-section legend,
.advanced-settings summary {
  padding: 0;
  margin-bottom: 10px;
  font-weight: 600;
  color: var(--el-text-color-primary);
}
.browse-section > .el-button {
  margin-bottom: 10px;
}
.advanced-settings summary {
  cursor: pointer;
}
</style>
