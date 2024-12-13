<template>
  <div class="listbox-subtitle">
    <img src="/images/google.svg" alt="Google Sheets icon" class="file-icon" />
    <span>Google Sheets</span>
    <hr />
    <div v-if="!isAuthenticated" class="listbox-wrapper">
      <div class="buttons-container">
        <button for="file-upload" class="file-upload-label" @click="signIn">
          Sign in
          <span class="material-icons file-icon">login</span>
        </button>
      </div>
    </div>

    <div v-else>
      <div class="buttons-container">
        <button @click="signOut">
          <span class="material-icons">exit_to_app</span>
          Sign Out
        </button>
        <button @click="doTokenRefresh">
          <span class="material-icons">refresh</span>
          refresh login
        </button>
      </div>
    </div>
    <div class="buttons-container">
      <button @click="openDrawer">
        <span class="material-icons">folder_open</span>
        Select file
      </button>
    </div>
  </div>
  <div v-if="selectedFile">
    <div class="listbox-title">Selected File</div>
    <div class="file-item">
      <img
        v-if="selectedFile.mimeType === 'application/vnd.google-apps.spreadsheet'"
        src="/images/sheets.png"
        alt="Google Sheets icon"
        class="file-icon"
      />
      {{ selectedFile.name }}
    </div>
    <div class="sheet-selection">
      select a sheet
      <el-select
        v-model="googleSheetSettings.worksheet_name"
        placeholder="Select a sheet"
        size="small"
      >
        <el-option
          v-for="sheet in selectedGoogleSheet?.sheets"
          :key="sheet.name"
          :label="sheet.name"
          :value="sheet.name"
        />
      </el-select>

      <el-popover placement="top-start" width="200" trigger="hover">
        <p>Number of Rows: {{ selectedSheet?.rows }}</p>
        <p>Number of Columns: {{ selectedSheet?.columns }}</p>
        <template #reference>
          <span class="material-icons info-icon">info</span>
        </template>
      </el-popover>
    </div>
  </div>
  <!-- Modal Starts Here -->
  <div v-if="showFiles" class="modal">
    <div class="modal-content">
      <span class="close" @click="closeDrawer">&times;</span>
      <div class="listbox-title">Google Drive Files</div>
      <div class="breadcrumb">
        <span v-if="selectedFolders.length === 0">.</span>
        <span v-else>
          <a href="#" @click="goToRoot">.</a>
          <span v-for="(folder, index) in selectedFolders" :key="folder.id">
            /
            <a href="#" @click="goToFolder(index)">
              {{ folder.name }}
            </a>
          </span>
        </span>
      </div>
      <div class="grid-container">
        <div
          v-for="file in displayedFiles"
          :key="file.id"
          :class="{ selected: selectedFolderId === file.id }"
          @dblclick="handleDoubleClick(file)"
          @click="selectedFolderId = file.id"
        >
          <img
            v-if="file.mimeType === 'application/vnd.google-apps.spreadsheet'"
            src="/images/sheets.png"
            alt="Google Sheets icon"
            class="file-icon"
          />
          <span v-else class="material-icons">
            {{
              file.mimeType === "application/vnd.google-apps.folder"
                ? "folder"
                : "insert_drive_file"
            }}
          </span>
          {{ file.name }}
        </div>
      </div>
      <div class="pagination-buttons">
        <button v-if="currentPage > 1" @click="changePage(-1)">Previous page</button>
        <button v-if="nextPageAvailable" @click="changePage(1)">Next page</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed, defineProps } from "vue";
import { gapi } from "gapi-script";
import { GoogleSheet } from "../../../baseNode/nodeInput";
const files = ref<DriveFile[]>([]);
const isAuthenticated = ref(false);
const dataLoaded = ref(false);
const id_token = ref("");
const accessToken = ref(""); // Will store the access token
const data = ref(null);
const nextPageToken = ref<string | null>(null);
const showFiles = ref(false);
const currentPage = ref<number>(1);
const selectedFolderId = ref<string>("");
const selectedFolders = ref<DriveFile[]>([]);
const selectedFile = ref<DriveFile | null>(null);
const selectedGoogleSheet = ref<GoogleSheetInfo | null>(null);
const tokenAvailableInBackend = ref(false);
const openDrawer = () => {
  fetchInitialFiles();
  showFiles.value = true;
};
const props = defineProps({
  modelValue: {
    type: Object as () => GoogleSheet,
    required: true,
  },
});

const emit = defineEmits(["update:modelValue"]);
const googleSheetSettings = ref(props.modelValue);

const closeDrawer = () => {
  showFiles.value = false;
  nextPageToken.value = null;
  files.value = [];
};

type PageResults = {
  [pageNumber: number]: DriveFile[];
};

const pageResults = ref<PageResults>({});

interface DriveFile {
  id: string;
  name: string;
  mimeType: string;
  parents?: string[]; // parents might not always be present, hence optional
}

interface GoogleSheetInfo {
  id: string;
  name: string;
  sheets: SheetInfo[];
}

interface SheetInfo {
  id: string;
  name: string;
  rows?: number;
  columns?: number;
}

const googleSheetDetails = ref<GoogleSheetInfo | null>(null);
const selectedSheet = computed(() => {
  return googleSheetDetails.value?.sheets.find(
    (sheet) => sheet.name === googleSheetSettings.value.worksheet_name,
  );
});

const fetchSheetDetails = async (spreadsheetId: string) => {
  if (!accessToken.value) return;
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${spreadsheetId}`;

  // Set up the authorization header with the access token
  const headers = new Headers({
    Authorization: `Bearer ${accessToken.value}`,
  });
  console.log("fetching sheet details");
  try {
    const response = await fetch(url, { headers });
    if (!response.ok) {
      throw new Error("Failed to fetch spreadsheet details");
    }
    const spreadsheetData = await response.json();
    const sheetsInfo: SheetInfo[] = spreadsheetData.sheets.map((sheet: any) => ({
      id: sheet.properties.sheetId, // The Sheets API uses 'sheetId' to refer to a sheet's unique identifier
      name: sheet.properties.title,
      rows: sheet.properties.gridProperties.rowCount,
      columns: sheet.properties.gridProperties.columnCount,
    }));

    googleSheetDetails.value = {
      id: spreadsheetData.spreadsheetId,
      name: spreadsheetData.properties.title,
      sheets: sheetsInfo,
    };
    selectedGoogleSheet.value = googleSheetDetails.value;
    // Show the modal or component that displays the sheet details
  } catch (error) {
    console.error("Error fetching sheet details:", error);
  }
};

const fetchDriveFiles = async (nextToken: string | null = null) => {
  if (!accessToken.value) return; // Exit if there's no access token available
  let folder_id = "root";
  if (selectedFolders.value.length > 0) {
    folder_id = selectedFolders.value[selectedFolders.value.length - 1].id;
  }
  let query = `(mimeType='application/vnd.google-apps.folder' or mimeType='application/vnd.google-apps.spreadsheet') and trashed=false and '${folder_id}' in parents`;
  const fields = "nextPageToken,files(id,name,mimeType,parents)";
  const pageSize = 20; // Adjust pageSize as necessary
  let url = `https://www.googleapis.com/drive/v3/files?pageSize=${pageSize}&fields=${encodeURIComponent(
    fields,
  )}&q=${encodeURIComponent(query)}`;

  if (nextToken) {
    url += `&pageToken=${nextToken}`;
  }

  // Set up the authorization header with the access token
  const headers = new Headers({
    Authorization: `Bearer ${accessToken.value}`,
  });

  try {
    // Make the request to the Google Drive API
    if (Object.prototype.hasOwnProperty.call(pageResults.value, currentPage.value)) {
      files.value = pageResults.value[currentPage.value];
      return;
    }
    const response = await fetch(url, { headers });
    if (!response.ok) {
      throw new Error("Failed to fetch files");
    }

    const data = await response.json();
    files.value = nextToken ? [...files.value, ...data.files] : data.files;
    pageResults.value[currentPage.value] = data.files;
    nextPageToken.value = data.nextPageToken || null;
  } catch (error) {
    console.error("Error fetching files from Google Drive:", error);
  }
};

const setBackendToken = () => {
  googleSheetSettings.value.access_token = accessToken.value;
};

const signIn = async () => {
  try {
    const authInstance = gapi.auth2.getAuthInstance();
    const googleUser = await authInstance.signIn({
      prompt: "select_account", // This line forces account selection
    });

    // Extracting auth response and setting state
    const authResponse = googleUser.getAuthResponse();
    id_token.value = authResponse.id_token;
    accessToken.value = authResponse.access_token; // Obtain the access token
    setBackendToken();
  } catch (error) {
    console.error("Error during sign in:", error);
  }
};

const displayedFiles = computed(() => {
  return pageResults.value[currentPage.value] || [];
});

const handleDoubleClick = (file: DriveFile) => {
  if (file.mimeType === "application/vnd.google-apps.folder") {
    selectFolder(file);
  } else {
    console.log("File selected:", file.id);
    selectedFile.value = file;
    googleSheetSettings.value.sheet_id = file.id;
    googleSheetSettings.value.sheet_name = file.name;
    fetchSheetDetails(file.id);
    closeDrawer();
  }
};

const createDriveFileFromSheetSettings = (): DriveFile => {
  return {
    id: googleSheetSettings.value.sheet_id,
    name: googleSheetSettings.value.sheet_name,
    mimeType: "application/vnd.google-apps.spreadsheet",
  };
};

const selectFolder = async (folder: DriveFile) => {
  console.log("Folder selected:", folder.id);
  selectedFolders.value.push(folder);
  selectedFolderId.value = folder.id;
  fetchInitialFiles();
};

const fetchInitialFiles = async () => {
  // Reset nextPageToken to null before initial fetch
  pageResults.value = {};
  nextPageToken.value = null;
  currentPage.value = 1;
  await fetchDriveFiles();
};

const pageFetched = () => {
  return Object.prototype.hasOwnProperty.call(pageResults.value, currentPage.value);
};

const nextPageAvailable = computed(() => {
  const all_pages = Object.keys(pageResults.value).map((key) => Number(key));
  return nextPageToken.value !== null || Math.max(...all_pages) > currentPage.value;
});

const changePage = async (pageChange: number) => {
  currentPage.value = currentPage.value + pageChange;

  // Call fetchDriveFiles with the current nextPageToken to load more files
  if (pageFetched()) {
    files.value = pageResults.value[currentPage.value];
  } else {
    await fetchDriveFiles(nextPageToken.value);
  }
};

const goToRoot = (event: Event) => {
  event.preventDefault();
  selectedFolders.value = []; // Clear the breadcrumb to go back to root
  selectedFolderId.value = "";
  fetchInitialFiles();
};

const goToFolder = (index: number) => {
  selectedFolders.value = selectedFolders.value.slice(0, index + 1);
  selectedFolderId.value = selectedFolders.value[index].id;
  fetchInitialFiles();
};

const signOut = async () => {
  try {
    const authInstance = gapi.auth2.getAuthInstance();
    await authInstance.signOut();
    authInstance.disconnect();

    // Reset application state
    isAuthenticated.value = false;
    dataLoaded.value = false;
    id_token.value = "";
    accessToken.value = "";
    data.value = null;
    files.value = [];
  } catch (error) {
    console.error("Error during sign out:", error);
  }
};

const initGoogleAuth = async () => {
  try {
    await new Promise((resolve) => gapi.load("auth2", resolve));
    await gapi.auth2.init({
      client_id: "752151734213-6oetksv4gjcenkrdsr9jjk7omm5qbt7n.apps.googleusercontent.com",
      scope:
        "https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/spreadsheets",
    });

    const authInstance = gapi.auth2.getAuthInstance();
    isAuthenticated.value = authInstance.isSignedIn.get();

    if (isAuthenticated.value) {
      const googleUser = authInstance.currentUser.get();
      const authResponse = googleUser.getAuthResponse();
      id_token.value = authResponse.id_token;
      accessToken.value = authResponse.access_token;
      setBackendToken();
      dataLoaded.value = true;
    }

    authInstance.isSignedIn.listen(async (isSignedIn: boolean) => {
      isAuthenticated.value = isSignedIn;
      if (isSignedIn) {
        const googleUser = authInstance.currentUser.get();
        const authResponse = googleUser.getAuthResponse();
        id_token.value = authResponse.id_token;
        accessToken.value = authResponse.access_token; // Also obtain the access token here
        setBackendToken();
        dataLoaded.value = true;
      } else {
        dataLoaded.value = false;
        id_token.value = "";
        accessToken.value = "";
        data.value = null;
        files.value = [];
      }
    });
  } catch (error) {
    console.error("Error initializing Google Auth:", error);
  }
};

onMounted(async () => {
  await initGoogleAuth();
  if (googleSheetSettings.value.sheet_id) {
    selectedFile.value = createDriveFileFromSheetSettings();
    fetchSheetDetails(googleSheetSettings.value.sheet_id);
    if (googleSheetSettings.value.access_token == "") {
      setBackendToken();
    }
  }
});

const doTokenRefresh = async () => {
  const authInstance = gapi.auth2.getAuthInstance();
  isAuthenticated.value = authInstance.isSignedIn.get();
  if (isAuthenticated.value) {
    const googleUser = authInstance.currentUser.get();
    const authResponse = googleUser.getAuthResponse();
    id_token.value = authResponse.id_token;
    accessToken.value = authResponse.access_token;
    tokenAvailableInBackend.value = false;
    setBackendToken();
  } else {
    signIn();
  }
};
</script>

<style scoped>
.context-menu {
  position: fixed;
  z-index: 1000;
  border: 1px solid #ccc;
  background-color: white;
  padding: 8px;
  box-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
  border-radius: 4px;
  user-select: none;
}

.context-menu button {
  display: block;
  background: none;
  border: none;
  padding: 4px 8px;
  text-align: left;
  width: 100%;
  cursor: pointer;
  z-index: 1;
}

.context-menu button:hover {
  background-color: #f0f0f0;
}

.selected {
  background-color: #e0f7fa;
}

.table-wrapper {
  position: relative;
  max-height: 300px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  border-radius: 8px;
  overflow: auto;
  margin: 5px;
}

.buttons-container {
  position: relative;
  display: flex;
  gap: 10px;
  justify-content: center;
  padding: 10px 0;
  min-width: 250px;
  z-index: 2;
}

button {
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  padding: 10px;
  background-color: #f5f5f5;
  border: 1px solid #ddd;
  border-radius: 4px;
  font-size: 16px;
  transition: background-color 0.3s ease;
  z-index: 2;
}

button:hover {
  background-color: #b3b5ba;
}

button .material-icons {
  margin-right: 8px;
  font-size: 20px;
}

.modal {
  position: fixed;
  z-index: 1000;
  left: 0;
  top: 0;
  width: 100vw;
  height: 100vh;
  overflow: auto;
  background-color: rgba(0, 0, 0, 0.4);
}

.modal-content {
  position: relative;
  background-color: #fefefe;
  margin: 10% auto;
  padding: 20px;
  border: 1px solid #888;
  width: 80%;
  z-index: 1001;
}

.pagination-buttons {
  display: flex;
  gap: 10px;
  justify-content: center;
  margin-top: 20px;
  z-index: 2;
}

.grid-container {
  position: relative;
  display: grid;
  grid-template-columns: repeat(5, 1fr);
  grid-gap: 10px;
  padding: 10px;
  z-index: 1;
}

.file-item {
  display: flex;
  align-items: center;
  justify-content: left;
  text-align: center;
  padding: 10px;
  background-color: #f4f4f4;
  border-radius: 4px;
}

.file-icon {
  width: 24px;
  height: auto;
  margin-right: 8px;
}

@media (max-width: 800px) {
  .grid-container {
    grid-template-columns: repeat(2, 1fr);
  }
}

.breadcrumb {
  margin: 10px 0;
}

.breadcrumb a {
  cursor: pointer;
  color: #0275d8;
  text-decoration: none;
}

.breadcrumb a:hover {
  text-decoration: underline;
}

/* Additional styles for el-select override */
.el-select-dropdown .el-select-dropdown__item.selected {
  background-color: #f5f5f5 !important;
  color: #000000;
}

.close {
  position: relative;
  z-index: 1002;
}
</style>
