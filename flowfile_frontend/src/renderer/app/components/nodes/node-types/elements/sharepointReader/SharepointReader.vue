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
        <el-button v-if="selectedManageableConnection" :disabled="busy" @click="openEdit"
          >Edit</el-button
        >
        <el-button v-if="selectedManageableConnection" :disabled="busy" @click="deleteConnection"
          >Delete</el-button
        >
        <el-button
          v-if="!canShare && selectedDelegatedConnection"
          :disabled="busy"
          @click="openReconnect"
          >Reconnect</el-button
        >
        <el-button
          v-if="canShare && selectedOwnedConnection"
          @click="shareTarget = selectedOwnedConnection"
          >Share</el-button
        >
      </div>
    </el-form-item>
    <p v-if="loadError" role="alert">Could not load saved SharePoint connections.</p>
    <p v-else-if="!loading && settings.connection_id && !selectedConnection" role="alert">
      The saved SharePoint connection is unavailable. Select another connection to run this node.
    </p>
    <p v-else-if="!loading && !connections.length">
      No saved SharePoint connections are available.
    </p>
    <el-form-item label="Source">
      <el-radio-group :model-value="listMode" @change="changeSource">
        <el-radio-button :value="false">Document library file</el-radio-button>
        <el-radio-button :value="true">SharePoint list</el-radio-button>
      </el-radio-group>
    </el-form-item>
    <fieldset class="browse-section" :disabled="!selectedConnection">
      <legend>Browse SharePoint</legend>
      <el-form-item label="Site URL">
        <el-input
          v-model="siteUrl"
          placeholder="https://example.sharepoint.com/sites/Finance"
          @input="resetBrowse"
        />
      </el-form-item>
      <template v-if="listMode">
        <el-button :disabled="!siteUrl.trim() || browsing" @click="loadLists">Find lists</el-button>
        <el-form-item v-if="lists.length || listDraft.list_id" label="List">
          <el-select :model-value="listDraft.list_id" @change="chooseList">
            <el-option
              v-for="item in listOptions"
              :key="item.id"
              :label="item.name"
              :value="item.id"
            />
          </el-select>
          <el-button v-if="listDraft.list_id" :disabled="browsing" @click="loadColumns"
            >Refresh columns</el-button
          >
        </el-form-item>
        <el-form-item v-if="listDraft.list_id" label="Columns">
          <el-select v-model="listDraft.columns" multiple filterable :multiple-limit="100">
            <el-option
              v-for="item in columnOptions"
              :key="item.id"
              :label="item.name"
              :value="item.id"
            />
          </el-select>
        </el-form-item>
        <el-form-item label="OData filter (optional)">
          <el-input
            v-model="listDraft.odata_filter"
            maxlength="2048"
            placeholder="fields/Quantity gt 0"
          />
        </el-form-item>
      </template>
      <el-button
        v-else
        :disabled="!settings.connection_id || !siteUrl.trim()"
        @click="loadLibraries()"
      >
        Find libraries
      </el-button>
      <template v-if="!listMode && selectedConnection">
        <el-form-item
          v-if="selectedConnection.auth_method !== 'delegated'"
          label="Microsoft Entra user object ID"
        >
          <el-input v-model="oneDriveUser" @input="resetBrowse" />
        </el-form-item>
        <el-button
          :disabled="
            browsing || (selectedConnection.auth_method !== 'delegated' && !oneDriveUser.trim())
          "
          @click="loadLibraries(true)"
          >Find OneDrive for Business</el-button
        >
      </template>
      <el-form-item v-if="libraries.length" label="Drive">
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
    <template v-if="!listMode">
      <el-form-item label="File path or glob">
        <el-input v-model="settings.file_path" placeholder="reports/sales.xlsx" />
        <small>Use * or ? in the filename only, for example reports/sales-*.csv.</small>
      </el-form-item>
      <el-form-item v-if="/\.csv$/i.test(settings.file_path?.trim() ?? '')" label="CSV delimiter">
        <el-input
          :model-value="settings.csv_delimiter ?? ','"
          maxlength="1"
          @update:model-value="settings.csv_delimiter = $event || ','"
        />
      </el-form-item>
      <el-form-item v-if="supportsDelta" label="Incremental read">
        <el-checkbox
          :model-value="settings.delta_generation != null"
          @change="setDeltaEnabled($event === true)"
        >
          Only download when Microsoft Graph reports a change
        </el-checkbox>
        <small
          >Save the flow first. The first run reads the whole file; an unchanged file returns zero
          rows.</small
        >
        <el-button
          v-if="settings.delta_generation != null"
          link
          type="primary"
          @click="setDeltaEnabled(true)"
          >Reset checkpoint on save</el-button
        >
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
          v-if="!isGlob"
          :disabled="!settings.connection_id || !validDrive || loadingSheets"
          @click="loadSheets"
        >
          Refresh File Metadata
        </el-button>
        <span v-if="fileMetadata">{{ fileMetadata.file_path }}</span>
        <span v-if="fileMetadata && !sheetNames.length" role="status">
          File metadata does not list worksheets. A successful run populates the sheet choices.
        </span>
        <span v-if="missingSheet" role="status">
          The selected worksheet is missing. Enter an existing sheet name or clear Sheet name to use
          the first sheet.
        </span>
      </el-form-item>
      <details class="advanced-settings">
        <summary>Advanced connection settings</summary>
        <el-form-item label="Drive ID">
          <el-input v-model="settings.drive_id" />
        </el-form-item>
        <el-button
          :disabled="!settings.connection_id || !validDrive || busy"
          @click="testConnection"
        >
          Test connection
        </el-button>
      </details>
    </template>
  </GenericNodeSettings>
  <el-dialog
    v-model="showEdit"
    title="Edit SharePoint connection"
    :show-close="!busy"
    :close-on-click-modal="!busy"
    :close-on-press-escape="!busy"
  >
    <el-form label-position="top" @submit.prevent="saveConnectionEdit">
      <el-form-item label="Connection name">
        <el-input v-model="editName" maxlength="255" :disabled="busy" />
      </el-form-item>
      <p>To change the default site, create a new connection and authenticate again.</p>
    </el-form>
    <template #footer>
      <el-button :disabled="busy" @click="showEdit = false">Cancel</el-button>
      <el-button
        type="primary"
        :loading="busy"
        :disabled="!editName.trim()"
        @click="saveConnectionEdit"
        >Save changes</el-button
      >
    </template>
  </el-dialog>
  <ShareDialog
    v-if="shareTarget"
    :model-value="true"
    resource-type="sharepoint_connection"
    :resource-id="shareTarget.id"
    :resource-name="shareTarget.connection_name"
    @update:model-value="shareTarget = null"
  />
  <el-dialog
    v-model="showCreate"
    :show-close="!savingDevice"
    :close-on-click-modal="!savingDevice"
    :close-on-press-escape="!savingDevice"
    :title="reconnectTarget ? 'Reconnect SharePoint' : 'New SharePoint connection'"
    @closed="clearConnectionDraft"
  >
    <el-form label-position="top" @submit.prevent="createConnection">
      <p v-if="reconnectTarget">
        Reconnect {{ reconnectTarget.connection_name }}. The Microsoft account you choose determines
        what existing flows can read.
      </p>
      <template v-else>
        <el-form-item label="Connection name"
          ><el-input v-model="connectionName" :disabled="deviceDraftLocked"
        /></el-form-item>
        <el-form-item label="Tenant ID"
          ><el-input v-model="tenantId" :disabled="deviceDraftLocked"
        /></el-form-item>
        <el-form-item label="Application (client) ID"
          ><el-input v-model="clientId" :disabled="deviceDraftLocked"
        /></el-form-item>
        <el-form-item label="Authentication">
          <el-select v-model="authMethod" :disabled="deviceDraftLocked" @change="clientSecret = ''">
            <el-option label="Client secret" value="client_secret" />
            <el-option label="Certificate" value="certificate" />
            <el-option v-if="!canShare" label="Microsoft sign-in" value="delegated" />
          </el-select>
        </el-form-item>
        <el-form-item
          v-if="authMethod === 'certificate'"
          label="PEM certificate and RSA private key"
        >
          <input type="file" accept=".pem" @change="loadCertificate" />
        </el-form-item>
        <el-form-item v-else-if="authMethod === 'client_secret'" label="Client secret">
          <el-input v-model="clientSecret" type="password" autocomplete="new-password" />
        </el-form-item>
        <el-form-item label="Default site URL (optional)">
          <el-input v-model="defaultSiteUrl" :disabled="deviceDraftLocked" maxlength="2048" />
        </el-form-item>
      </template>
      <el-form-item v-if="authMethod === 'delegated'" label="Optional Excel column preview">
        <el-checkbox v-model="excelColumnPreview" :disabled="deviceDraftLocked">
          Preview Excel column names before running
        </el-checkbox>
        <small
          >Microsoft Graph requires delegated Files.ReadWrite for this preview. Flowfile only reads
          cells; it does not edit workbooks.</small
        >
      </el-form-item>
      <p v-if="devicePrompt && !deviceReady">
        Open
        <a :href="devicePrompt.verification_uri" target="_blank" rel="noopener noreferrer"
          >Microsoft sign-in</a
        >
        and enter <strong>{{ devicePrompt.user_code }}</strong
        >.
      </p>
      <p v-if="deviceDraftLocked && !reconnectTarget">
        To change these settings, cancel and start again.
      </p>
      <el-button
        v-if="deviceDraftLocked || reconnectTarget"
        :disabled="savingDevice"
        @click="showCreate = false"
      >
        Cancel
      </el-button>
      <el-button
        type="primary"
        native-type="submit"
        :disabled="(!canCreate && !reconnectTarget) || busy"
      >
        {{
          authMethod === "delegated" && !deviceReady
            ? "Start sign-in"
            : reconnectTarget
              ? "Confirm reconnect"
              : "Save connection"
        }}
      </el-button>
    </el-form>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { isAxiosError } from "axios";
import { ElMessage, ElMessageBox } from "element-plus";
import DropDown from "../../../baseNode/page_objects/dropDown.vue";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import ShareDialog from "../../../../sharing/ShareDialog.vue";
import authService from "../../../../../services/auth.service";
import axios from "../../../../../services/axios.config";
import { useNodeStore } from "../../../../../stores/node-store";
import type { NodeBase } from "../../../../../types/node.types";

interface SharePointSettings extends NodeBase {
  csv_delimiter?: string;
  delta_generation?: number | null;
  connection_id: number | null;
  drive_id: string | null;
  file_path: string | null;
  list_source?: ListSource | null;
  sheet_name: string | null;
}

interface SavedConnection {
  id: number;
  connection_name: string;
  default_site_url: string | null;
  is_owner: boolean;
  can_manage: boolean;
  auth_method: string;
}

const store = useNodeStore();
const settings = ref<SharePointSettings | null>(null);
const connections = ref<SavedConnection[]>([]);
const canShare = !authService.isInDesktopMode();
const shareTarget = ref<SavedConnection | null>(null);
const selectedOwnedConnection = computed(() =>
  connections.value.find(
    (connection) =>
      connection.id === settings.value?.connection_id &&
      connection.is_owner &&
      connection.auth_method !== "delegated",
  ),
);
const selectedConnection = computed(() =>
  connections.value.find(({ id }) => id === settings.value?.connection_id),
);
const selectedManageableConnection = computed(() =>
  selectedConnection.value?.can_manage ? selectedConnection.value : null,
);
const showEdit = ref(false);
const editTarget = ref<SavedConnection | null>(null);
const editName = ref("");

function openEdit() {
  const target = selectedManageableConnection.value;
  if (!target) return;
  editTarget.value = target;
  editName.value = target.connection_name;
  showEdit.value = true;
}

async function saveConnectionEdit() {
  const target = editTarget.value;
  if (!target || busy.value || !editName.value.trim()) return;
  busy.value = true;
  try {
    const { data } = await axios.patch<SavedConnection>(`/sharepoint_connections/${target.id}`, {
      connection_name: editName.value.trim(),
    });
    connections.value = connections.value.map((connection) =>
      connection.id === data.id ? data : connection,
    );
    showEdit.value = false;
    ElMessage.success("SharePoint connection updated.");
  } catch (error) {
    const detail = isAxiosError(error) ? error.response?.data?.detail : undefined;
    ElMessage.error(
      typeof detail === "string" ? detail : "Could not update the SharePoint connection.",
    );
  } finally {
    busy.value = false;
  }
}

async function deleteConnection() {
  const target = selectedManageableConnection.value;
  if (!target || busy.value) return;
  try {
    await ElMessageBox.confirm(
      `Delete ${target.connection_name}? Flows that use it will need another connection before they can run.`,
      "Delete SharePoint connection",
      { type: "warning", confirmButtonText: "Delete" },
    );
  } catch {
    return;
  }
  busy.value = true;
  try {
    await axios.delete(`/sharepoint_connections/${target.id}`);
    connections.value = connections.value.filter(({ id }) => id !== target.id);
    if (settings.value?.connection_id === target.id) resetBrowse();
    ElMessage.success("SharePoint connection deleted.");
  } catch {
    ElMessage.error("Could not delete the SharePoint connection.");
  } finally {
    busy.value = false;
  }
}

const loading = ref(false);
const loadError = ref(false);
const busy = ref(false);
const showCreate = ref(false);
const reconnectTarget = ref<SavedConnection | null>(null);
const selectedDelegatedConnection = computed(() =>
  connections.value.find(
    (connection) =>
      connection.id === settings.value?.connection_id &&
      connection.is_owner &&
      connection.auth_method === "delegated",
  ),
);

function openReconnect() {
  if (!selectedDelegatedConnection.value) return;
  reconnectTarget.value = selectedDelegatedConnection.value;
  authMethod.value = "delegated";
  showCreate.value = true;
}
const connectionName = ref("");
const tenantId = ref("");
const clientId = ref("");
const clientSecret = ref("");
const authMethod = ref("client_secret");
const excelColumnPreview = ref(false);
interface DevicePrompt {
  flow_id: string;
  user_code: string;
  verification_uri: string;
  interval: number;
  expires_in: number;
}
const deviceReady = ref(false);
const savingDevice = ref(false);
const devicePrompt = ref<DevicePrompt | null>(null);
const deviceDraftLocked = computed(
  () => authMethod.value === "delegated" && (busy.value || deviceReady.value),
);

async function loadCertificate(event: Event) {
  const file = (event.target as HTMLInputElement).files?.[0];
  clientSecret.value = "";
  if (!file || file.size > 65536) return;
  try {
    const pem = await file.text();
    if (authMethod.value === "certificate" && showCreate.value) clientSecret.value = pem;
  } catch {
    ElMessage.error("Could not read the certificate file.");
  }
}
const defaultSiteUrl = ref("");
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const canCreate = computed(() =>
  Boolean(
    connectionName.value.trim() &&
    (authMethod.value === "delegated" || clientSecret.value) &&
    uuid.test(tenantId.value.trim()) &&
    uuid.test(clientId.value.trim()),
  ),
);
const validDrive = computed(() => {
  const drive = settings.value?.drive_id?.trim() ?? "";
  return /^[A-Za-z0-9_!.-]{1,512}$/.test(drive) && drive !== "." && drive !== "..";
});
const isExcel = computed(() => /\.xlsx?$/i.test(settings.value?.file_path?.trim() ?? ""));
const isGlob = computed(() => /[*?]/.test(settings.value?.file_path ?? ""));
const supportsDelta = computed(
  () => !isGlob.value && /\.(csv|json|parquet)$/i.test(settings.value?.file_path?.trim() ?? ""),
);

function setDeltaEnabled(enabled: boolean) {
  const draft = settings.value;
  if (!draft) return;
  draft.delta_generation = enabled ? Math.max(Date.now(), (draft.delta_generation ?? 0) + 1) : null;
}
interface FileMetadata {
  file_path: string;
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
  if (
    !draft?.connection_id ||
    !validDrive.value ||
    !isExcel.value ||
    isGlob.value ||
    loadingSheets.value
  )
    return;
  const request = sheetRequest.value;
  loadingSheets.value = true;
  try {
    const { data } = await axios.get<FileMetadata>(
      `/sharepoint_connections/${draft.connection_id}/drives/${encodeURIComponent(draft.drive_id?.trim() ?? "")}/worksheets`,
      {
        params: {
          file_path: draft.file_path?.trim(),
        },
      },
    );
    if (request === sheetRequest.value) {
      fileMetadata.value = data;
      sheetNames.value = [];
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

const supportedFile = /\.(csv|xlsx?|parquet|json)$/i;

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
type ListSource = {
  site_id: string;
  list_id: string;
  columns: string[];
  odata_filter?: string | null;
};
type BrowseOption = { id: string; name: string };
const emptyList = (): ListSource => ({ site_id: "", list_id: "", columns: [] });
const listMode = computed(() => !!settings.value?.list_source);
const listDraft = computed<ListSource>({
  get: () => settings.value?.list_source ?? emptyList(),
  set: (source) => {
    if (settings.value) settings.value.list_source = source;
  },
});
function changeSource(value: unknown) {
  if (!settings.value) return;
  settings.value.list_source = value === true ? emptyList() : null;
  resetBrowse();
  listColumns.value = [];
}
const lists = ref<BrowseOption[]>([]);
const listColumns = ref<BrowseOption[]>([]);
const listSite = ref("");
const listOptions = computed(() => withSavedOptions(lists.value, [listDraft.value.list_id]));
const columnOptions = computed(() => withSavedOptions(listColumns.value, listDraft.value.columns));
function withSavedOptions(options: BrowseOption[], ids: string[]) {
  return [
    ...options,
    ...ids
      .filter((id) => id && !options.some((option) => option.id === id))
      .map((id) => ({ id, name: id })),
  ];
}

const browsing = ref(false);
const browseMessage = ref("");
let browseRequest = 0;

function resetBrowse() {
  browseRequest++;
  libraries.value = [];
  lists.value = [];
  listSite.value = "";
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
      if (listMode.value) listDraft.value = emptyList();
      listColumns.value = [];
    }
  },
  { flush: "sync" },
);

async function loadListMetadata<T>(
  path: string,
  apply: (data: T) => void,
  params?: { site_url: string },
) {
  const connectionId = settings.value?.connection_id;
  if (!connectionId) return;
  const request = ++browseRequest;
  browsing.value = true;
  browseMessage.value = "";
  try {
    const { data } = await axios.get<T>(`/sharepoint_connections/${connectionId}/${path}`, {
      params,
    });
    if (request === browseRequest) apply(data);
  } catch {
    if (request === browseRequest) browseMessage.value = "Could not load SharePoint list metadata.";
  } finally {
    if (request === browseRequest) browsing.value = false;
  }
}

function loadLists() {
  return loadListMetadata<{ site_id: string; lists: BrowseOption[] }>(
    "lists",
    (data) => {
      listSite.value = data.site_id;
      lists.value = data.lists;
      if (!data.lists.length) browseMessage.value = "No lists found.";
    },
    { site_url: siteUrl.value.trim() },
  );
}

function chooseList(id: string) {
  if (
    id !== listDraft.value.list_id ||
    (listSite.value && listSite.value !== listDraft.value.site_id)
  ) {
    listDraft.value = { ...emptyList(), site_id: listSite.value, list_id: id };
    listColumns.value = [];
  }
  void loadColumns();
}

function loadColumns() {
  const { site_id, list_id } = listDraft.value;
  return loadListMetadata<BrowseOption[]>(
    `lists/${encodeURIComponent(site_id)}/${encodeURIComponent(list_id)}/columns`,
    (data) => {
      listColumns.value = data;
      if (!data.length) browseMessage.value = "No selectable columns found.";
    },
  );
}

const oneDriveUser = ref("");

async function loadLibraries(oneDrive = false) {
  const id = settings.value?.connection_id;
  if (!id || (!oneDrive && !siteUrl.value.trim())) return;
  resetBrowse();
  const request = browseRequest;
  browsing.value = true;
  try {
    const resource = oneDrive ? "onedrive" : "libraries";
    const { data } = await axios.get<Library[]>(`/sharepoint_connections/${id}/${resource}`, {
      params: oneDrive
        ? {
            user_id:
              selectedConnection.value?.auth_method === "delegated"
                ? undefined
                : oneDriveUser.value.trim(),
          }
        : { site_url: siteUrl.value.trim() },
    });
    if (request !== browseRequest) return;
    libraries.value = data;
    if (!data.length) browseMessage.value = "No document libraries found.";
  } catch {
    if (request === browseRequest)
      browseMessage.value = oneDrive
        ? "Could not discover OneDrive for Business. Check the user and connection permissions."
        : "Could not list libraries. Check the site URL and connection permissions.";
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

function cancelDeviceSignIn(flowId: string) {
  void axios.delete(`/sharepoint_connections/device/${flowId}`).catch(() => {
    // The attempt may already have expired or been consumed by Save.
  });
}

function clearConnectionDraft() {
  reconnectTarget.value = null;
  if (devicePrompt.value) cancelDeviceSignIn(devicePrompt.value.flow_id);
  deviceReady.value = false;
  devicePrompt.value = null;
  authMethod.value = "client_secret";
  excelColumnPreview.value = false;
  connectionName.value =
    tenantId.value =
    clientId.value =
    clientSecret.value =
    defaultSiteUrl.value =
      "";
}

async function createConnection() {
  const draft = settings.value;
  const reconnect = reconnectTarget.value;
  if (!draft || (!canCreate.value && !reconnect) || busy.value) return;
  busy.value = true;
  try {
    const target = {
      connection_name: connectionName.value.trim(),
      tenant_id: tenantId.value.trim(),
      client_id: clientId.value.trim(),
      default_site_url: defaultSiteUrl.value.trim() || null,
    };
    let data: SavedConnection;
    if (deviceReady.value && devicePrompt.value) {
      savingDevice.value = true;
      ({ data } = await axios.post<SavedConnection>(
        `/sharepoint_connections/device/${devicePrompt.value.flow_id}/save`,
      ));
    } else if (authMethod.value === "delegated") {
      const started = reconnect
        ? await axios.post<DevicePrompt>(
            `/sharepoint_connections/${reconnect.id}/reconnect`,
            null,
            {
              params: { excel_column_preview: excelColumnPreview.value },
            },
          )
        : await axios.post<DevicePrompt>("/sharepoint_connections/device/start", {
            ...target,
            excel_column_preview: excelColumnPreview.value,
          });
      const prompt = started.data;
      if (!showCreate.value || settings.value !== draft) {
        cancelDeviceSignIn(prompt.flow_id);
        return;
      }
      devicePrompt.value = prompt;
      const deadline = Date.now() + prompt.expires_in * 1000;
      let interval = prompt.interval;
      for (;;) {
        await new Promise<void>((resolve) => setTimeout(resolve, interval * 1000));
        if (
          !showCreate.value ||
          settings.value !== draft ||
          devicePrompt.value?.flow_id !== prompt.flow_id
        )
          return;
        if (Date.now() >= deadline) throw new Error("Sign-in expired");
        const result = await axios.post<
          { status: "pending"; interval: number } | { status: "ready" }
        >(`/sharepoint_connections/device/${prompt.flow_id}`);
        if (result.data.status === "ready") {
          if (
            !showCreate.value ||
            settings.value !== draft ||
            devicePrompt.value?.flow_id !== prompt.flow_id
          )
            return;
          deviceReady.value = true;
          return;
        }
        interval = result.data.interval;
      }
    } else {
      ({ data } = await axios.post<SavedConnection>("/sharepoint_connections", {
        ...target,
        auth_method: authMethod.value,
        client_secret: clientSecret.value,
      }));
    }
    if (reconnect) {
      connections.value = connections.value.map((connection) =>
        connection.id === data.id ? data : connection,
      );
    } else {
      connections.value = [...connections.value, data];
      if (settings.value === draft) draft.connection_id = data.id;
    }
    devicePrompt.value = null;
    showCreate.value = false;
    ElMessage.success(
      reconnect ? "SharePoint connection reconnected." : "SharePoint connection saved.",
    );
  } catch {
    deviceReady.value = false;
    if (showCreate.value) ElMessage.error("Could not save the SharePoint connection. Try again.");
  } finally {
    savingDevice.value = false;
    if (!deviceReady.value && devicePrompt.value) {
      cancelDeviceSignIn(devicePrompt.value.flow_id);
      devicePrompt.value = null;
    }
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
      drive_id: draft.drive_id?.trim(),
    });
    ElMessage.success("SharePoint drive is accessible.");
  } catch (error) {
    const detail = isAxiosError<{ detail?: unknown }>(error) && error.response?.data?.detail;
    ElMessage.error(
      typeof detail === "string"
        ? detail
        : "Could not access the SharePoint drive. Check the drive ID and app permissions.",
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
    if (listMode.value) {
      const source = listDraft.value;
      if (!source.site_id || !source.list_id || !source.columns.length) {
        ElMessage.error("Choose a SharePoint list and at least one column.");
        return false;
      }
      draft.list_source = { ...source, odata_filter: source.odata_filter?.trim() || null };
      draft.drive_id = draft.file_path = draft.sheet_name = null;
      draft.delta_generation = null;
      return true;
    }
    const filePath = draft.file_path?.trim() ?? "";
    if (!validDrive.value || !supportedFile.test(filePath)) {
      ElMessage.error("Enter a drive ID and the path to a CSV, Excel, Parquet, or JSON file.");
      return false;
    }
    const parts = filePath.split("/");
    const filename = parts.pop() ?? "";
    if (parts.some((folder) => /[*?]/.test(folder)) || filename.includes("**")) {
      ElMessage.error(
        "SharePoint wildcards are supported only in the filename, without recursion.",
      );
      return false;
    }
    draft.drive_id = draft.drive_id?.trim() ?? null;
    draft.file_path = filePath;
    draft.list_source = null;
    draft.sheet_name = isExcel.value && draft.sheet_name ? draft.sheet_name : null;
    if (!supportsDelta.value) draft.delta_generation = null;
    if (draft.delta_generation != null) draft.cache_results = false;
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
  listColumns.value = [];
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
