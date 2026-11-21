<template>
  <GenericNodeSettings
    v-if="settings"
    v-model="settings"
    @update:model-value="handleGenericSettingsUpdate"
    @request-save="saveSettings"
  >
    <el-form-item label="Saved connection">
      <el-select
        v-model="settings.connection_id"
        placeholder="Select an app connection"
        @change="changeConnection"
      >
        <el-option
          v-for="item in connections"
          :key="item.id"
          :label="item.connection_name"
          :value="item.id"
        />
      </el-select>
      <small>Create an app connection in the SharePoint reader if none is available.</small>
    </el-form-item>
    <el-form-item label="Destination">
      <el-select v-model="listMode" @change="resetBrowse">
        <el-option label="Document library file" :value="false" />
        <el-option label="SharePoint list (append items)" :value="true" />
      </el-select>
    </el-form-item>
    <fieldset v-if="listMode" :disabled="!settings.connection_id">
      <legend>Append to a SharePoint list</legend>
      <el-form-item label="Site URL">
        <el-input v-model="siteUrl" @input="resetBrowse" />
      </el-form-item>
      <el-button :disabled="!siteUrl.trim() || browsing" @click="loadLists">Find lists</el-button>
      <el-form-item label="List">
        <el-select :model-value="listTarget.list_id" @change="chooseList">
          <el-option
            v-if="listTarget.list_id && !lists.some(({ id }) => id === listTarget.list_id)"
            :label="listTarget.list_id"
            :value="listTarget.list_id"
          />
          <el-option v-for="item in lists" :key="item.id" :label="item.name" :value="item.id" />
        </el-select>
      </el-form-item>
      <el-form-item label="Column mapping (JSON)">
        <el-input v-model="mapping" type="textarea" placeholder='{"input_column": "Title"}' />
        <small
          >Map input column names to internal list column names. Each run appends new items.</small
        >
      </el-form-item>
      <el-button :disabled="!listTarget.list_id || browsing" @click="loadColumns"
        >Show writable columns</el-button
      >
      <ul v-if="columns.length">
        <li v-for="item in columns" :key="item.id">{{ item.name }}: {{ item.id }}</li>
      </ul>
    </fieldset>
    <template v-else>
      <fieldset :disabled="!settings.connection_id">
        <legend>Browse SharePoint or OneDrive for Business folders</legend>
        <el-form-item label="Site URL">
          <el-input
            v-model="siteUrl"
            placeholder="https://example.sharepoint.com/sites/Finance"
            @input="resetBrowse"
          />
        </el-form-item>
        <el-button
          :disabled="!settings.connection_id || !siteUrl.trim() || browsing"
          @click="loadLibraries()"
        >
          Find libraries
        </el-button>
        <el-form-item label="Microsoft Entra user object ID">
          <el-input v-model="oneDriveUser" @input="resetBrowse" />
        </el-form-item>
        <el-button
          :disabled="!settings.connection_id || !oneDriveUser.trim() || browsing"
          @click="loadLibraries(true)"
        >
          Find OneDrive for Business
        </el-button>
        <el-form-item v-if="libraries.length" label="Drive">
          <el-select
            :model-value="settings.drive_id"
            placeholder="Choose a library"
            @change="chooseLibrary"
          >
            <el-option
              v-for="item in libraries"
              :key="item.id"
              :label="item.name"
              :value="item.id"
            />
          </el-select>
        </el-form-item>
        <div v-if="browsedDrive">
          <el-button
            :disabled="!folders.length || browsing"
            @click="loadFolder(folders.slice(0, -1))"
          >
            Up one folder
          </el-button>
          <span>{{ folders.map(({ name }) => name).join(" / ") || "Library root" }}</span>
          <el-button :disabled="browsing" @click="useFolder">Use this folder</el-button>
          <ul>
            <li v-for="item in children" :key="item.id">
              <el-button link :disabled="browsing" @click="loadFolder([...folders, item])"
                >{{ item.name }}/</el-button
              >
            </li>
          </ul>
        </div>
      </fieldset>
      <el-form-item label="Drive ID">
        <el-input v-model="settings.drive_id" @input="resetBrowse" />
      </el-form-item>
      <el-form-item label="Output file path">
        <el-input v-model="settings.file_path" placeholder="Reports/output.csv" maxlength="2048" />
        <small
          >Enter the filename and extension relative to the library. The folder must already
          exist.</small
        >
      </el-form-item>
      <el-form-item label="Format">
        <el-select v-model="settings.format" @change="resetExcelOptions">
          <el-option label="CSV" value="csv" />
          <el-option label="Excel (.xlsx)" value="xlsx" />
          <el-option label="Parquet" value="parquet" />
        </el-select>
      </el-form-item>
      <el-form-item v-if="settings.format === 'xlsx'" label="Sheet name">
        <el-input v-model="settings.sheet_name" placeholder="Sheet1" maxlength="31" />
      </el-form-item>
      <el-form-item label="If the file already exists">
        <el-select v-model="settings.conflict_mode">
          <el-option label="Create (error if the file exists)" value="error" />
          <el-option label="Overwrite" value="overwrite" />
          <el-option v-if="settings.format === 'xlsx'" label="Update sheet" value="update" />
          <el-option v-if="settings.format === 'xlsx'" label="Append rows" value="append" />
          <el-option label="Create a new file with a suffix" value="append_suffix" />
        </el-select>
        <small v-if="settings.conflict_mode === 'update'">
          Replaces or adds the named sheet, or creates a missing workbook. Other sheets are kept,
          but embedded images may be lost. This does not append rows.
        </small>
        <small v-else-if="settings.conflict_mode === 'append'">
          Adds rows below the existing data without repeating headers. Headers must match in the same
          order. Creates a missing workbook or sheet. Embedded images may be lost when saving.
        </small>
      </el-form-item>
      <el-form-item v-if="settings.conflict_mode === 'append'">
        <small>Append rows leaves the destination unchanged when there are no rows.</small>
      </el-form-item>
      <el-form-item v-else label="Write output when there are no rows">
        <el-switch v-model="settings.write_when_empty" />
        <small>{{ emptyOutputHelp }}</small>
      </el-form-item>
    </template>
    <p v-if="error" role="alert">{{ error }}</p>
  </GenericNodeSettings>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import axios from "../../../../../services/axios.config";
import { useNodeStore } from "../../../../../stores/node-store";
import type { NodeBase } from "../../../../../types/node.types";

interface Destination extends NodeBase {
  connection_id: number | null;
  drive_id: string;
  file_path: string;
  format: "csv" | "xlsx" | "parquet";
  conflict_mode: "error" | "overwrite" | "append_suffix" | "update" | "append";
  sheet_name?: string | null;
  write_when_empty?: boolean;
  list_target?: { site_id: string; list_id: string; column_mapping: Record<string, string> } | null;
}
interface Entry {
  id: string;
  name: string;
}
interface Connection {
  id: number;
  connection_name: string;
  default_site_url: string | null;
  auth_method: string;
}

const store = useNodeStore();
const settings = ref<Destination | null>(null);
const connections = ref<Connection[]>([]);
const siteUrl = ref("");
const oneDriveUser = ref("");
const deploymentSite = ref("");
const libraries = ref<Entry[]>([]);
const folders = ref<Entry[]>([]);
const children = ref<Entry[]>([]);
const browsedDrive = ref("");
const browsing = ref(false);
const error = ref("");
const listMode = ref(false);
const listTarget = ref({ site_id: "", list_id: "" });
const mapping = ref("");
const lists = ref<Entry[]>([]);
const columns = ref<Entry[]>([]);
const browseSite = ref("");
let browseRequest = 0;

function resetBrowse() {
  browseRequest++;
  libraries.value = [];
  lists.value = columns.value = [];
  browseSite.value = "";
  folders.value = [];
  children.value = [];
  browsedDrive.value = "";
  browsing.value = false;
  error.value = "";
}

function changeConnection() {
  resetBrowse();
  oneDriveUser.value = "";
  const draft = settings.value;
  if (!draft) return;
  draft.drive_id = "";
  draft.file_path = "";
  listTarget.value = { site_id: "", list_id: "" };
  mapping.value = "";
  siteUrl.value =
    connections.value.find(({ id }) => id === draft.connection_id)?.default_site_url ||
    deploymentSite.value;
}

async function browse<T>(url: string, params: object, apply: (data: T) => void) {
  const id = ++browseRequest;
  browsing.value = true;
  error.value = "";
  try {
    const { data } = await axios.get<T>(url, { params });
    if (id === browseRequest) apply(data);
  } catch {
    if (id === browseRequest)
      error.value = "Could not browse SharePoint. Check the site and app permissions.";
  } finally {
    if (id === browseRequest) browsing.value = false;
  }
}

function loadLibraries(oneDrive = false) {
  resetBrowse();
  const resource = oneDrive ? "onedrive" : "libraries";
  const params = oneDrive
    ? { user_id: oneDriveUser.value.trim() }
    : { site_url: siteUrl.value.trim() };
  return browse<Entry[]>(
    `/sharepoint_connections/${settings.value?.connection_id}/${resource}`,
    params,
    (entries) => {
      libraries.value = entries;
    },
  );
}

function loadLists() {
  return browse<{ site_id: string; lists: Entry[] }>(
    `/sharepoint_connections/${settings.value?.connection_id}/lists`,
    { site_url: siteUrl.value.trim() },
    (data) => {
      browseSite.value = data.site_id;
      lists.value = data.lists;
      if (listTarget.value.site_id && listTarget.value.site_id !== data.site_id) {
        listTarget.value = { site_id: "", list_id: "" };
        mapping.value = "";
      }
    },
  );
}

function chooseList(id: string) {
  if (!browseSite.value || id === listTarget.value.list_id) return;
  listTarget.value = { site_id: browseSite.value, list_id: id };
  mapping.value = "";
  columns.value = [];
  return loadColumns();
}

function loadColumns() {
  const { site_id, list_id } = listTarget.value;
  return browse<Entry[]>(
    `/sharepoint_connections/${settings.value?.connection_id}/lists/${encodeURIComponent(site_id)}/${encodeURIComponent(list_id)}/columns`,
    { writable: true },
    (entries) => {
      columns.value = entries;
    },
  );
}

function chooseLibrary(drive: string) {
  if (!settings.value) return;
  settings.value.drive_id = drive;
  browsedDrive.value = "";
  return loadFolder([]);
}

function loadFolder(path: Entry[]) {
  const draft = settings.value;
  if (!draft?.drive_id) return;
  const url = `/sharepoint_connections/${draft.connection_id}/drives/${encodeURIComponent(draft.drive_id)}/children`;
  return browse<(Entry & { is_folder: boolean })[]>(
    url,
    { folder_id: path.at(-1)?.id },
    (entries) => {
      browsedDrive.value = draft.drive_id;
      folders.value = path;
      children.value = entries.filter((entry) => entry.is_folder);
    },
  );
}

const emptyOutputHelp = computed(() => {
  const draft = settings.value;
  if (!draft?.write_when_empty) return "Skips the file write and leaves existing files unchanged.";
  switch (draft.conflict_mode) {
    case "update":
      return "Replaces the named sheet with headers only, or creates it if missing.";
    case "overwrite":
      return "Replaces the entire file with a zero-row output.";
    case "append_suffix":
      return "Writes a zero-row file under a new name if the destination already exists.";
    default:
      return "Creates a zero-row file. If the destination exists, the write fails.";
  }
});

function resetExcelOptions() {
  const draft = settings.value;
  if (!draft || draft.format === "xlsx") return;
  delete draft.sheet_name;
  if (draft.conflict_mode === "update" || draft.conflict_mode === "append") {
    draft.conflict_mode = "error";
  }
}

function useFolder() {
  const draft = settings.value;
  if (!draft) return;
  const filename = draft.file_path.split("/").at(-1) || `output.${draft.format}`;
  draft.file_path = [...folders.value.map(({ name }) => name), filename].join("/");
}

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: settings,
  onBeforeSave() {
    const draft = settings.value;
    if (!draft || !connections.value.some(({ id }) => id === draft.connection_id)) {
      ElMessage.error("Select a saved SharePoint app connection.");
      return false;
    }
    if (listMode.value) {
      try {
        const value = JSON.parse(mapping.value);
        if (
          !listTarget.value.list_id ||
          !value ||
          typeof value !== "object" ||
          Array.isArray(value)
        )
          throw Error();
        const pairs = Object.entries(value);
        if (
          !pairs.length ||
          pairs.length > 100 ||
          pairs.some(
            ([key, name]) =>
              !key || typeof name !== "string" || !/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(name),
          ) ||
          new Set(Object.values(value)).size !== pairs.length
        )
          throw Error();
        draft.list_target = { ...listTarget.value, column_mapping: value };
        const listDraft = draft as Partial<Destination>;
        delete listDraft.drive_id;
        delete listDraft.file_path;
        delete listDraft.format;
        delete draft.sheet_name;
        delete draft.write_when_empty;
        draft.conflict_mode = "error";
        return true;
      } catch {
        ElMessage.error(
          "Select a list and map input columns to unique list columns in a JSON object.",
        );
        return false;
      }
    }
    const drive = draft.drive_id?.trim() ?? "";
    const path = draft.file_path?.trim() ?? "";
    if (
      !/^[A-Za-z0-9_!.-]{1,512}$/.test(drive) ||
      drive === "." ||
      drive === ".." ||
      !path.toLowerCase().endsWith(`.${draft.format}`) ||
      path
        .split("/")
        .some((part) => !part || part === "." || part === ".." || /["*:<>?\\|]/.test(part))
    ) {
      ElMessage.error(
        "Enter a valid drive ID and an output path with the selected format's extension.",
      );
      return false;
    }
    draft.drive_id = drive;
    draft.file_path = path;
    draft.sheet_name = draft.format === "xlsx" ? draft.sheet_name?.trim() || null : null;
    draft.list_target = null;
    return true;
  },
});

async function loadNodeData(nodeId: number) {
  resetBrowse();
  oneDriveUser.value = "";
  const id = browseRequest;
  const node = await store.getNodeData(nodeId, false);
  if (id !== browseRequest) return;
  const draft: Destination = node?.setting_input?.is_setup
    ? (node.setting_input as Destination)
    : {
        flow_id: store.flow_id,
        node_id: nodeId,
        cache_results: false,
        pos_x: 0,
        pos_y: 0,
        connection_id: null,
        drive_id: "",
        file_path: "",
        format: "csv",
        conflict_mode: "error",
        write_when_empty: false,
      };
  listMode.value = !!draft.list_target;
  listTarget.value = {
    site_id: draft.list_target?.site_id ?? "",
    list_id: draft.list_target?.list_id ?? "",
  };
  mapping.value = draft.list_target
    ? JSON.stringify(draft.list_target.column_mapping, null, 2)
    : "";
  draft.format ??= "csv";
  draft.file_path ??= "";
  draft.drive_id ??= "";
  settings.value = draft;
  connections.value = [];
  siteUrl.value = deploymentSite.value = "";
  const [available, defaults] = await Promise.allSettled([
    axios.get<Connection[]>("/sharepoint_connections"),
    axios.get<{ site_url: string | null }>("/sharepoint_connections/browse_defaults"),
  ]);
  if (id !== browseRequest) return;
  deploymentSite.value =
    defaults.status === "fulfilled" ? (defaults.value.data.site_url ?? "") : "";
  if (available.status === "rejected") {
    error.value = "Could not load saved SharePoint connections.";
    return;
  }
  connections.value = available.value.data.filter(({ auth_method }) => auth_method !== "delegated");
  siteUrl.value =
    connections.value.find(({ id }) => id === draft.connection_id)?.default_site_url ||
    deploymentSite.value;
}

defineExpose({ loadNodeData, pushNodeData, saveSettings });
</script>
