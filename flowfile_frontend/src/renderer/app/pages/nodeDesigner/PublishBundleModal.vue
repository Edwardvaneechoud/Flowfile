<template>
  <el-dialog
    :model-value="show"
    title="Publish to community"
    width="640px"
    :close-on-click-modal="false"
    append-to-body
    @update:model-value="(v: boolean) => !v && emit('close')"
  >
    <div class="publish-body">
      <!-- 1. Completeness checklist -->
      <section class="card checklist-card">
        <header class="checklist-head">
          <span class="checklist-title">Readiness</span>
          <span v-if="checking" class="checklist-hint">Checking…</span>
          <span v-else-if="hasErrors" class="status-badge status-badge--danger">Not ready</span>
          <span v-else class="status-badge status-badge--success">Ready</span>
        </header>
        <ul class="checklist">
          <li
            v-for="item in checklist"
            :key="item.key"
            class="check-row"
            :class="`is-${item.status}`"
          >
            <i class="check-icon" :class="iconFor(item.status)" />
            <span class="check-label">{{ item.label }}</span>
            <span v-if="item.message" class="check-message">{{ item.message }}</span>
          </li>
        </ul>
      </section>

      <!-- 1b. Version context: new node vs update, with one-click bumps -->
      <div
        v-if="versionBanner"
        class="version-banner"
        :class="{ 'version-banner--warn': versionNotBumped }"
      >
        <div class="version-banner-text">
          <i :class="versionNotBumped ? 'fa-solid fa-triangle-exclamation' : 'fa-solid fa-tag'"></i>
          <span>{{ versionBanner }}</span>
        </div>
        <div v-if="publishedVersion !== null" class="version-bump-actions">
          <span class="bump-label">Bump:</span>
          <button
            v-for="level in BUMP_LEVELS"
            :key="level"
            class="btn btn-secondary btn-sm"
            :disabled="bumping"
            @click="applyBump(level)"
          >
            {{ level }}
          </button>
        </div>
        <p v-if="changelogNudge" class="changelog-nudge">
          Tip: add a changelog line below — users see it when they update.
        </p>
      </div>

      <!-- 2. Distribution metadata -->
      <div class="form-grid">
        <div class="form-field">
          <label>License</label>
          <el-select v-model="license" data-testid="publish-license" placeholder="Choose a license">
            <el-option v-for="l in LICENSES" :key="l" :label="l" :value="l" />
          </el-select>
        </div>
        <div class="form-field">
          <label>Category</label>
          <el-select
            v-model="category"
            data-testid="publish-category"
            placeholder="Choose a category"
          >
            <el-option v-for="c in CATEGORIES" :key="c" :label="c" :value="c" />
          </el-select>
        </div>
        <div class="form-field form-field--wide">
          <label>Description</label>
          <el-input
            v-model="description"
            type="textarea"
            :rows="2"
            maxlength="300"
            show-word-limit
            placeholder="A short description shown in the community catalog"
          />
        </div>
        <div class="form-field form-field--wide">
          <label>Repository <span class="optional">(optional)</span></label>
          <el-input v-model="repository" placeholder="https://github.com/you/your-node" />
        </div>
        <div class="form-field form-field--wide">
          <label>
            Changelog <span class="optional">(optional — shown to users updating your node)</span>
          </label>
          <el-input
            v-model="changelog"
            type="textarea"
            :rows="2"
            maxlength="1000"
            show-word-limit
            placeholder="What changed in this version, e.g. 1.1.0 - faster matching, new theme option"
          />
        </div>
        <div class="form-field form-field--wide">
          <label class="readme-label">
            <span>README <span class="optional">(optional — stored with the node)</span></span>
            <button class="link-btn" type="button" @click="insertReadmeTemplate">
              Insert template
            </button>
          </label>
          <el-input
            v-model="readme"
            type="textarea"
            :rows="6"
            placeholder="Markdown shown on your node's community page. Leave empty and a TODO stub ships for you to edit on GitHub."
          />
        </div>
      </div>

      <!-- 3. Screenshots -->
      <section class="screenshots-section">
        <div class="screenshots-head">
          <span class="section-label">Screenshots</span>
          <label class="btn btn-secondary btn-sm upload-btn">
            <i class="fa-solid fa-upload"></i>
            Add PNG
            <input type="file" accept=".png" hidden @change="handleUpload" />
          </label>
        </div>
        <div v-if="screenshots.length" class="thumb-grid">
          <div v-for="shot in screenshots" :key="shot.file_name" class="thumb">
            <img :src="screenshotUrls[shot.file_name]" :alt="shot.file_name" class="thumb-img" />
            <button class="thumb-delete" title="Remove" @click="removeScreenshot(shot.file_name)">
              <i class="fa-solid fa-times"></i>
            </button>
          </div>
        </div>
        <p v-else class="screenshots-empty">
          No screenshots yet. Community media must be PNG (max 5).
        </p>
      </section>

      <!-- 4. Publish on GitHub -->
      <section class="card github-card">
        <header class="github-head">
          <span class="section-label"><i class="fa-brands fa-github"></i> Publish on GitHub</span>
          <span v-if="connected" class="status-badge status-badge--success">
            <i class="fa-solid fa-check"></i> @{{ login }}
          </span>
        </header>
        <p class="github-lede">Publishing requires a free GitHub account.</p>

        <el-skeleton v-if="githubLoading" :rows="2" animated />

        <!-- Success: PR opened / refreshed -->
        <div v-else-if="prResult" class="pr-success">
          <div class="pr-success-head">
            <i class="fa-solid fa-circle-check"></i>
            <span>{{ prSuccessMessage(prResult.status) }}</span>
          </div>
          <div class="pr-success-actions">
            <button class="btn btn-primary btn-sm" @click="openPr">
              <i class="fa-solid fa-arrow-up-right-from-square"></i>
              View pull request{{ prResult.pr_number ? ` #${prResult.pr_number}` : "" }}
            </button>
            <button class="btn btn-secondary btn-sm" @click="returnToPublish">
              <i class="fa-solid fa-rotate"></i>
              Update pull request
            </button>
          </div>
          <div class="whats-next">
            <span class="whats-next-title">What happens next</span>
            <ul>
              <li>Automated checks validate your node and dry-run its test setup.</li>
              <li>
                First-time contributor? The checks wait until a maintainer approves the run — that's
                expected, not stuck.
              </li>
              <li>A maintainer reviews your node.</li>
              <li>
                Changed something meanwhile? Publish again with the same version — your open PR is
                updated in place.
              </li>
              <li>Once merged, it appears in the community catalog on the next index refresh.</li>
            </ul>
          </div>
        </div>

        <!-- Connected: confirm + create PR -->
        <div v-else-if="connected" class="github-connected">
          <div v-if="openPrInfo" class="open-pr-note">
            <i class="fa-solid fa-code-pull-request"></i>
            <span>{{ openPrInfo.text }}</span>
            <button class="link-btn" @click="openExistingPr">View PR</button>
          </div>
          <el-checkbox v-model="confirmed" class="confirm-check">
            My node does what its description says — no hidden network calls or data collection.
          </el-checkbox>
          <div class="connected-actions">
            <button class="btn btn-primary" :disabled="!canPublish" @click="createPr">
              <i class="fa-solid fa-code-pull-request"></i>
              {{
                publishing
                  ? "Publishing…"
                  : prEverOpened
                    ? "Update pull request"
                    : "Create pull request"
              }}
            </button>
            <button class="link-btn" :disabled="publishing" @click="disconnect">Disconnect</button>
          </div>
          <p v-if="publishStatusText" class="publish-status">
            <i class="fa-solid fa-circle-notch fa-spin"></i> {{ publishStatusText }}
          </p>
        </div>

        <!-- Not connected: device flow + PAT fallback -->
        <div v-else class="github-connect">
          <div v-if="device" class="device-panel">
            <p class="device-instructions">Enter this code at GitHub to authorize publishing:</p>
            <div class="device-code-row">
              <code class="device-code">{{ device.user_code }}</code>
              <button class="btn btn-secondary btn-sm" @click="copyUserCode">
                <i class="fa-solid fa-copy"></i> Copy
              </button>
            </div>
            <div class="device-actions">
              <button class="btn btn-primary btn-sm" @click="openDeviceUrl">
                <i class="fa-solid fa-arrow-up-right-from-square"></i>
                Open github.com/login/device
              </button>
              <span v-if="polling" class="device-waiting">
                <i class="fa-solid fa-circle-notch fa-spin"></i> Waiting for authorization…
              </span>
            </div>
          </div>

          <div v-else-if="deviceError" class="device-error">
            <p>{{ deviceError }}</p>
            <button class="btn btn-secondary btn-sm" @click="startOver">Start over</button>
          </div>

          <template v-else>
            <el-alert
              v-if="!configured"
              type="info"
              :closable="false"
              show-icon
              :title="notConfiguredMessage"
            />
            <button v-else class="btn btn-primary" :disabled="connecting" @click="startConnect">
              <i class="fa-brands fa-github"></i>
              {{ connecting ? "Starting…" : "Connect GitHub" }}
            </button>
          </template>

          <CollapsibleSection
            title="Use a personal access token instead"
            icon="fa-solid fa-key"
            :default-open="false"
            persist-key="publish-github-pat"
            nested
          >
            <div class="pat-row">
              <el-input
                v-model="pat"
                type="password"
                show-password
                placeholder="ghp_… (classic PAT with public_repo scope)"
              />
              <button
                class="btn btn-primary btn-sm"
                :disabled="patSaving || !pat.trim()"
                @click="savePat"
              >
                {{ patSaving ? "Saving…" : "Save" }}
              </button>
            </div>
            <p v-if="patWarning" class="pat-warning">
              <i class="fa-solid fa-triangle-exclamation"></i> {{ patWarning }}
            </p>
          </CollapsibleSection>
        </div>

        <el-alert
          v-if="githubError"
          type="error"
          :closable="false"
          show-icon
          class="github-error"
          :title="githubError"
        />
      </section>
    </div>

    <template #footer>
      <div class="publish-footer">
        <p class="footer-hint">
          No GitHub account? Download the bundle and share it with a maintainer.
        </p>
        <div class="footer-actions">
          <button class="btn btn-secondary" @click="emit('close')">Close</button>
          <button
            class="btn btn-secondary"
            data-testid="publish-download"
            :disabled="hasErrors || checking || downloading"
            @click="download"
          >
            <i class="fa-solid fa-download"></i>
            {{ downloading ? "Preparing…" : "Download bundle" }}
          </button>
        </div>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { ElMessage } from "element-plus";

import CollapsibleSection from "../../components/common/CollapsibleSection/CollapsibleSection.vue";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import { desktop } from "../../../lib/desktop";
import {
  type DeviceStartResponse,
  type GithubConnectStatus,
  PublishIncompleteError,
  type PublishIssue,
  PublishPrBlockedError,
  type PublishPrResponse,
  type PublishScreenshot,
  type OpenPublishPr,
  checkPublishCompleteness,
  deletePublishScreenshot,
  disconnectGithub,
  fetchOpenPublishPrs,
  fetchPublishReadme,
  fetchPublishScreenshotUrl,
  getGithubStatus,
  listPublishScreenshots,
  pollGithubDevice,
  publishBundle,
  publishPr,
  savePublishReadme,
  saveGithubPat,
  startGithubDevice,
  uploadPublishScreenshot,
} from "../../api/nodeDesigner";
import { saveNode } from "./loadSave";
import {
  type BumpLevel,
  FORK_RETRY_DELAY_MS,
  FORK_RETRY_MAX,
  bumpSemver,
  canCreatePr,
  describeGithubError,
  nextPollDelayMs,
  openPrNotice,
  prSuccessMessage,
  semverGt,
  withReadmeTemplate,
} from "./publishGithub";

const props = defineProps<{
  show: boolean;
  fileName: string;
}>();

const emit = defineEmits<{ (e: "close"): void }>();

const LICENSES = [
  "MIT",
  "Apache-2.0",
  "BSD-3-Clause",
  "BSD-2-Clause",
  "MPL-2.0",
  "Unlicense",
  "CC0-1.0",
];
const CATEGORIES = [
  "Input",
  "Output",
  "Text",
  "Numeric",
  "Date & Time",
  "Cleaning",
  "Machine Learning",
  "Connectors",
  "Geospatial",
  "Utilities",
  "Fun",
];

// label ← which issue codes fail this requirement (first match wins).
const REQUIREMENTS: { key: string; label: string; codes: string[] }[] = [
  { key: "health", label: "Node builds without errors", codes: ["NODE_UNHEALTHY"] },
  { key: "name", label: "Usable node name", codes: ["NODE_NAME_INVALID"] },
  { key: "author", label: "Author set", codes: ["AUTHOR_MISSING"] },
  { key: "version", label: "Semver version", codes: ["VERSION_INVALID"] },
  {
    key: "description",
    label: "Description (10+ characters)",
    codes: ["DESCRIPTION_MISSING", "DESCRIPTION_TOO_SHORT"],
  },
  { key: "examples", label: "Test setup", codes: ["EXAMPLES_MISSING"] },
  { key: "license", label: "License selected", codes: ["LICENSE_INVALID"] },
  { key: "icon", label: "PNG icon", codes: ["ICON_NOT_PNG", "ICON_DEFAULT", "ICON_MISSING"] },
  { key: "screenshots", label: "Screenshot added", codes: ["NO_SCREENSHOTS"] },
  { key: "readme", label: "README", codes: ["README_TOO_LARGE", "README_STUB"] },
  { key: "changelog", label: "Changelog", codes: ["CHANGELOG_TOO_LONG"] },
];

const store = useNodeDesignerStore();

const license = ref("MIT");
const category = ref("Utilities");
const description = ref("");
const repository = ref("");
const readme = ref("");
const changelog = ref("");

const issues = ref<PublishIssue[]>([]);
const checking = ref(false);
const downloading = ref(false);

const BUMP_LEVELS: BumpLevel[] = ["patch", "minor", "major"];
const currentVersion = ref("");
const publishedVersion = ref<string | null>(null);
const bumping = ref(false);

const screenshots = ref<PublishScreenshot[]>([]);
const screenshotUrls = ref<Record<string, string>>({});

const fileStem = computed(() => props.fileName.replace(/\.py$/, ""));
const hasErrors = computed(() => issues.value.some((i) => i.severity === "error"));

// The modal never unmounts, so switching nodes is its only "mount". `hydrating` marks
// that window: every field write inside it comes from the reset or from the server, so
// the autosave and readiness-check watchers must ignore it. `loadSeq` invalidates
// in-flight responses that belong to the node we just switched away from.
const hydrating = ref(true);
let loadSeq = 0;
// Last stem whose load completed, and the stem a load is currently running for.
let hydratedStem: string | null = null;
let pendingStem: string | null = null;
// The README text the backend is known to hold. Autosave keys off this rather than a
// time window: the reset and the sidecar load both write the field, and only a write
// the user made should ever be persisted. A time window would also drop edits typed
// while the modal was still loading.
let lastSyncedReadme = "";

function setReadmeFromServer(text: string) {
  lastSyncedReadme = text;
  readme.value = text;
}

type CheckStatus = "pass" | "warning" | "error";
const checklist = computed(() =>
  REQUIREMENTS.map((req) => {
    const match = issues.value.find((i) => req.codes.includes(i.code));
    const status: CheckStatus = match ? match.severity : "pass";
    return { key: req.key, label: req.label, status, message: match?.message ?? "" };
  }),
);

function iconFor(status: CheckStatus): string {
  if (status === "error") return "fa-solid fa-circle-xmark";
  if (status === "warning") return "fa-solid fa-triangle-exclamation";
  return "fa-solid fa-circle-check";
}

const versionNotBumped = computed(
  () => publishedVersion.value !== null && !semverGt(currentVersion.value, publishedVersion.value),
);

const versionBanner = computed(() => {
  if (!currentVersion.value) return "";
  if (publishedVersion.value === null) return `New node — will publish v${currentVersion.value}`;
  if (versionNotBumped.value) {
    return `v${currentVersion.value} is not newer than the published v${publishedVersion.value} — bump the version to release an update`;
  }
  return `Update — published v${publishedVersion.value} → publishing v${currentVersion.value}`;
});

const changelogNudge = computed(
  () => publishedVersion.value !== null && !versionNotBumped.value && !changelog.value.trim(),
);

// The backend reads the version from the saved file, so a bump must persist first.
async function applyBump(level: BumpLevel) {
  const next = bumpSemver(store.nodeMetadata.version || currentVersion.value, level);
  if (!next) {
    ElMessage.warning("Set a semver version (e.g. 1.0.0) in the Publishing panel first.");
    return;
  }
  bumping.value = true;
  try {
    store.nodeMetadata.version = next;
    if (await saveNode()) await runCheck();
  } finally {
    bumping.value = false;
  }
}

function bundleBody() {
  return {
    file_name: props.fileName,
    license: license.value,
    repository: repository.value,
    description: description.value,
    category: category.value,
    readme: readme.value,
    changelog: changelog.value,
  };
}

let checkTimer: ReturnType<typeof setTimeout> | null = null;
function scheduleCheck() {
  if (checkTimer) clearTimeout(checkTimer);
  checkTimer = setTimeout(runCheck, 250);
}

async function runCheck() {
  if (!props.fileName) return;
  const seq = loadSeq;
  checking.value = true;
  try {
    const report = await checkPublishCompleteness(bundleBody());
    if (seq !== loadSeq) return;
    issues.value = report.issues;
    currentVersion.value = report.version ?? "";
    publishedVersion.value = report.published_version ?? null;
  } catch (e: unknown) {
    if (seq !== loadSeq) return;
    const err = e as { response?: { data?: { detail?: string } }; message?: string };
    ElMessage.error(
      `Could not check readiness: ${err.response?.data?.detail || err.message || "error"}`,
    );
  } finally {
    if (seq === loadSeq) checking.value = false;
  }
}

function revokeUrls() {
  Object.values(screenshotUrls.value).forEach((url) => URL.revokeObjectURL(url));
  screenshotUrls.value = {};
}

async function loadScreenshots() {
  if (!props.fileName) return;
  const seq = loadSeq;
  try {
    const shots = await listPublishScreenshots(fileStem.value);
    if (seq !== loadSeq) return;
    revokeUrls();
    screenshots.value = shots;
    for (const shot of shots) {
      fetchPublishScreenshotUrl(fileStem.value, shot.file_name).then((url) => {
        if (!url) return;
        // A node switch while this was in flight would otherwise show A's image on B.
        if (seq !== loadSeq) {
          URL.revokeObjectURL(url);
          return;
        }
        screenshotUrls.value = { ...screenshotUrls.value, [shot.file_name]: url };
      });
    }
  } catch {
    if (seq === loadSeq) screenshots.value = [];
  }
}

async function handleUpload(event: Event) {
  const target = event.target as HTMLInputElement;
  const file = target.files?.[0];
  if (!file) return;
  if (!file.name.toLowerCase().endsWith(".png")) {
    ElMessage.warning("Community screenshots must be PNG.");
    target.value = "";
    return;
  }
  try {
    await uploadPublishScreenshot(fileStem.value, file);
    await loadScreenshots();
    await runCheck();
  } catch (e: unknown) {
    const err = e as { response?: { data?: { detail?: string } }; message?: string };
    ElMessage.error(`Upload failed: ${err.response?.data?.detail || err.message || "error"}`);
  }
  target.value = "";
}

async function removeScreenshot(fileName: string) {
  try {
    await deletePublishScreenshot(fileStem.value, fileName);
    await loadScreenshots();
    await runCheck();
  } catch (e: unknown) {
    const err = e as { message?: string };
    ElMessage.error(`Could not remove screenshot: ${err.message || "error"}`);
  }
}

async function download() {
  downloading.value = true;
  try {
    const blob = await publishBundle(bundleBody());
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${fileStem.value}-bundle.zip`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
    ElMessage.success("Bundle downloaded.");
  } catch (e: unknown) {
    if (e instanceof PublishIncompleteError) {
      issues.value = e.issues;
      ElMessage.warning("Resolve the readiness items first.");
    } else {
      const err = e as { message?: string };
      ElMessage.error(`Download failed: ${err.message || "error"}`);
    }
  } finally {
    downloading.value = false;
  }
}

// ---- In-app GitHub publishing ----
const githubStatus = ref<GithubConnectStatus | null>(null);
const githubLoading = ref(true);
const githubError = ref("");

const device = ref<DeviceStartResponse | null>(null);
const deviceError = ref("");
const connecting = ref(false);
const polling = ref(false);

const pat = ref("");
const patSaving = ref(false);
const patWarning = ref("");

const confirmed = ref(false);
const publishing = ref(false);
const publishStatusText = ref("");
const prResult = ref<PublishPrResponse | null>(null);
// A PR was opened/refreshed this session: the primary button reads "Update pull request".
const prEverOpened = ref(false);
const openPrs = ref<OpenPublishPr[]>([]);

const openPrInfo = computed(() => openPrNotice(openPrs.value, currentVersion.value));
const relevantOpenPr = computed(
  () => openPrs.value.find((p) => p.version === currentVersion.value) ?? openPrs.value[0] ?? null,
);

function openExistingPr() {
  if (relevantOpenPr.value?.url) desktop.openExternal(relevantOpenPr.value.url);
}

// An open PR for the current version means publishing updates it, not creates one.
watch([openPrs, currentVersion], () => {
  if (openPrs.value.some((p) => p.version === currentVersion.value)) prEverOpened.value = true;
});

let pollTimer: ReturnType<typeof setTimeout> | null = null;
let forkTimer: ReturnType<typeof setTimeout> | null = null;
let pollIntervalSec = 5;

const connected = computed(() => githubStatus.value?.connected ?? false);
const configured = computed(() => githubStatus.value?.configured ?? false);
const login = computed(() => githubStatus.value?.login ?? "");
const notConfiguredMessage = computed(() => describeGithubError("GITHUB_NOT_CONFIGURED"));
const canPublish = computed(() =>
  canCreatePr({
    hasErrors: hasErrors.value,
    checking: checking.value,
    connected: connected.value,
    confirmed: confirmed.value,
    publishing: publishing.value,
  }),
);

function githubErrorText(e: unknown): string {
  if (e instanceof PublishPrBlockedError) return describeGithubError(e.code, e.message);
  const err = e as {
    response?: { data?: { detail?: { message?: string } | string } };
    message?: string;
  };
  const detail = err.response?.data?.detail;
  if (detail && typeof detail === "object" && detail.message) return detail.message;
  if (typeof detail === "string") return detail;
  return err.message || "Something went wrong talking to GitHub.";
}

function clearPollTimer() {
  if (pollTimer) {
    clearTimeout(pollTimer);
    pollTimer = null;
  }
}

function clearForkTimer() {
  if (forkTimer) {
    clearTimeout(forkTimer);
    forkTimer = null;
  }
}

function stopGithubTimers() {
  clearPollTimer();
  clearForkTimer();
  polling.value = false;
}

async function loadGithubStatus() {
  githubLoading.value = true;
  try {
    githubStatus.value = await getGithubStatus();
  } catch {
    githubStatus.value = { configured: false, connected: false, login: "" };
  } finally {
    githubLoading.value = false;
  }
  if (connected.value) refreshOpenPrs();
  else openPrs.value = [];
}

function insertReadmeTemplate() {
  readme.value = withReadmeTemplate(readme.value);
}

// The README sidecar persists per node on the backend; the field only seeds when empty.
async function loadReadme() {
  if (!props.fileName || readme.value.trim()) return;
  const stem = fileStem.value;
  const seq = loadSeq;
  try {
    const stored = await fetchPublishReadme(stem);
    if (stored && seq === loadSeq && stem === fileStem.value && !readme.value.trim()) {
      setReadmeFromServer(stored);
    }
  } catch {
    /* sidecar is best-effort */
  }
}

function cancelPendingTimers() {
  if (readmeSaveTimer) {
    clearTimeout(readmeSaveTimer);
    readmeSaveTimer = null;
  }
  if (checkTimer) {
    clearTimeout(checkTimer);
    checkTimer = null;
  }
}

// resetGithubTransient owns the connect/PR half on close; these are the publish-form
// fields, which nothing else clears. Keep the two lists disjoint.
function resetPerNodeState() {
  setReadmeFromServer("");
  changelog.value = "";
  currentVersion.value = "";
  publishedVersion.value = null;
  openPrs.value = [];
  issues.value = [];
  description.value = "";
  license.value = "MIT";
  category.value = "Utilities";
  repository.value = "";
  screenshots.value = [];
  revokeUrls();
}

// The modal is permanently mounted: switching nodes must not leak per-node state
// (a pending autosave would otherwise write node A's README into node B's sidecar).
// `props.show` is already true when this runs — the parent sets fileName and show in
// one tick — so hydrating, not show, is what keeps these writes out of the autosave.
watch(
  () => props.fileName,
  () => {
    hydrating.value = true;
    hydratedStem = null;
    cancelPendingTimers();
    resetPerNodeState();
    // Deliberately not order-dependent: whichever of this and the `show` watcher runs
    // first starts the load, and the other one is deduped by the pendingStem guard.
    if (props.show) void onOpen();
  },
);

let readmeSaveTimer: ReturnType<typeof setTimeout> | null = null;

function flushReadmeSave() {
  if (!readmeSaveTimer) return;
  clearTimeout(readmeSaveTimer);
  readmeSaveTimer = null;
  void persistReadme();
}

async function persistReadme() {
  const text = readme.value;
  try {
    await savePublishReadme(fileStem.value, text);
    lastSyncedReadme = text;
  } catch (e: unknown) {
    // Silently dropping this hid oversized-README rejections entirely.
    const err = e as { response?: { data?: { detail?: { message?: string } } }; message?: string };
    ElMessage.error(
      `Could not save the README: ${err.response?.data?.detail?.message || err.message || "error"}`,
    );
  }
}

watch(readme, () => {
  // Only a divergence from what the backend holds is a user edit worth saving. The
  // per-node reset used to land here as an empty PUT that tombstoned the new node.
  if (!props.show || readme.value === lastSyncedReadme) return;
  if (readmeSaveTimer) clearTimeout(readmeSaveTimer);
  readmeSaveTimer = setTimeout(() => {
    readmeSaveTimer = null;
    void persistReadme();
  }, 800);
});

// Passive indicator: failures never surface, they just hide the note. Guarded against
// responses landing after the modal closed or switched to another node.
async function refreshOpenPrs() {
  if (!props.fileName) return;
  const stem = fileStem.value;
  try {
    const prs = await fetchOpenPublishPrs(props.fileName);
    if (props.show && stem === fileStem.value) openPrs.value = prs;
  } catch {
    if (stem === fileStem.value) openPrs.value = [];
  }
}

function scheduleNextPoll(intervalSec: number, status: string) {
  clearPollTimer();
  pollTimer = setTimeout(pollOnce, nextPollDelayMs(intervalSec, status));
}

async function pollOnce() {
  pollTimer = null;
  if (!device.value) return;
  try {
    const res = await pollGithubDevice(device.value.device_code);
    if (res.status === "connected") {
      polling.value = false;
      device.value = null;
      await loadGithubStatus();
      ElMessage.success(`Connected as @${res.login ?? login.value}`);
      return;
    }
    pollIntervalSec = res.interval ?? pollIntervalSec;
    scheduleNextPoll(pollIntervalSec, res.status);
  } catch (e) {
    polling.value = false;
    if (
      e instanceof PublishPrBlockedError &&
      (e.code === "DEVICE_EXPIRED" || e.code === "DEVICE_DENIED")
    ) {
      device.value = null;
      deviceError.value = describeGithubError(e.code);
    } else {
      device.value = null;
      githubError.value = githubErrorText(e);
    }
  }
}

async function startConnect() {
  connecting.value = true;
  deviceError.value = "";
  githubError.value = "";
  try {
    device.value = await startGithubDevice();
    pollIntervalSec = device.value.interval || 5;
    polling.value = true;
    scheduleNextPoll(pollIntervalSec, "pending");
  } catch (e) {
    githubError.value = githubErrorText(e);
  } finally {
    connecting.value = false;
  }
}

function startOver() {
  stopGithubTimers();
  device.value = null;
  deviceError.value = "";
  startConnect();
}

async function copyUserCode() {
  if (!device.value) return;
  try {
    await navigator.clipboard.writeText(device.value.user_code);
    ElMessage.success("Code copied to clipboard.");
  } catch {
    ElMessage.warning("Could not copy — enter the code manually.");
  }
}

function openDeviceUrl() {
  desktop.openExternal(device.value?.verification_uri || "https://github.com/login/device");
}

async function savePat() {
  const token = pat.value.trim();
  if (!token) return;
  patSaving.value = true;
  githubError.value = "";
  try {
    const res = await saveGithubPat(token);
    patWarning.value = res.scopes_warning ?? "";
    pat.value = "";
    await loadGithubStatus();
    ElMessage.success(`Connected as @${res.login}`);
  } catch (e) {
    githubError.value = githubErrorText(e);
  } finally {
    patSaving.value = false;
  }
}

async function disconnect() {
  try {
    await disconnectGithub();
    patWarning.value = "";
    confirmed.value = false;
    prResult.value = null;
    await loadGithubStatus();
  } catch (e) {
    ElMessage.error(githubErrorText(e));
  }
}

async function createPr() {
  githubError.value = "";
  prResult.value = null;
  publishing.value = true;
  publishStatusText.value = "";
  await runPublish(0);
}

async function runPublish(forkAttempt: number) {
  try {
    const res = await publishPr(bundleBody());
    if (res.status === "fork_creating") {
      if (forkAttempt >= FORK_RETRY_MAX) {
        publishing.value = false;
        publishStatusText.value = "";
        githubError.value = "GitHub is still creating your fork. Wait a moment, then try again.";
        return;
      }
      publishStatusText.value = "Creating your fork on GitHub…";
      forkTimer = setTimeout(() => {
        forkTimer = null;
        runPublish(forkAttempt + 1);
      }, FORK_RETRY_DELAY_MS);
      return;
    }
    publishing.value = false;
    publishStatusText.value = "";
    prResult.value = res;
    prEverOpened.value = true;
    refreshOpenPrs();
  } catch (e) {
    publishing.value = false;
    publishStatusText.value = "";
    if (e instanceof PublishIncompleteError) {
      issues.value = e.issues;
      githubError.value = "Resolve the readiness items above before publishing.";
    } else {
      githubError.value = githubErrorText(e);
    }
  }
}

function openPr() {
  if (prResult.value?.pr_url) desktop.openExternal(prResult.value.pr_url);
}

// Back to the connected block to publish again; the attestation checkbox stays ticked.
function returnToPublish() {
  prResult.value = null;
  githubError.value = "";
}

function resetGithubTransient() {
  device.value = null;
  deviceError.value = "";
  githubError.value = "";
  publishStatusText.value = "";
  publishing.value = false;
  connecting.value = false;
  prResult.value = null;
  prEverOpened.value = false;
  openPrs.value = [];
  confirmed.value = false;
}

// Readiness runs after the loads, not alongside them: it submits the README, so
// checking before the sidecar has landed reports a node as missing one it has.
async function onOpen() {
  const stem = fileStem.value;
  if (hydrating.value && pendingStem === stem) return; // already loading this node
  pendingStem = stem;
  const seq = ++loadSeq;
  hydrating.value = true;
  cancelPendingTimers();
  if (hydratedStem !== stem) resetPerNodeState();
  if (!description.value.trim()) description.value = store.nodeMetadata.intro ?? "";
  try {
    await Promise.allSettled([loadReadme(), loadScreenshots(), loadGithubStatus()]);
  } finally {
    // allSettled never rejects, so hydration always ends — a stuck flag would
    // silently disable autosave for the rest of the session.
    if (seq === loadSeq) {
      hydrating.value = false;
      hydratedStem = stem;
    }
  }
  if (seq === loadSeq) void runCheck();
}

watch(
  () => props.show,
  (open) => {
    if (!open) {
      flushReadmeSave();
      cancelPendingTimers();
      stopGithubTimers();
      resetGithubTransient();
      return;
    }
    void onOpen();
  },
);

watch([license, category, description, repository, readme, changelog], () => {
  if (props.show && !hydrating.value) scheduleCheck();
});

onBeforeUnmount(() => {
  if (checkTimer) clearTimeout(checkTimer);
  // Flush, not drop: leaving the route inside the debounce window used to lose edits.
  flushReadmeSave();
  stopGithubTimers();
  revokeUrls();
});
</script>

<style scoped>
.publish-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.checklist-card {
  padding: var(--spacing-3);
}

.checklist-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-2);
}

.checklist-title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.checklist-hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.checklist {
  list-style: none;
  margin: 0;
  padding: 0;
  display: grid;
  gap: var(--spacing-1);
}

.check-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
}

.check-icon {
  width: 1rem;
  text-align: center;
}

.is-pass .check-icon {
  color: var(--color-success);
}

.is-warning .check-icon {
  color: var(--color-warning);
}

.is-error .check-icon {
  color: var(--color-danger);
}

.check-label {
  color: var(--color-text-primary);
  flex-shrink: 0;
}

.check-message {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.form-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-3);
}

.version-banner {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border);
  border-radius: var(--radius-md);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.version-banner--warn {
  border-color: var(--color-warning);
  color: var(--color-warning);
}

.version-banner-text {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex: 1;
  min-width: 240px;
}

.version-bump-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
}

.bump-label {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.changelog-nudge {
  flex-basis: 100%;
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.form-field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.form-field--wide {
  grid-column: 1 / -1;
}

.form-field label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.optional {
  font-weight: var(--font-weight-normal);
  color: var(--color-text-muted);
}

.section-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.screenshots-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-2);
}

.thumb-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(96px, 1fr));
  gap: var(--spacing-2);
}

.thumb {
  position: relative;
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  overflow: hidden;
  aspect-ratio: 16 / 10;
  background: var(--color-background-secondary);
}

.thumb-img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.thumb-delete {
  position: absolute;
  top: 2px;
  right: 2px;
  width: 18px;
  height: 18px;
  padding: 0;
  border: none;
  border-radius: var(--border-radius-full);
  background: var(--color-danger);
  color: var(--color-text-inverse);
  font-size: var(--font-size-2xs);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
}

.screenshots-empty {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

/* GitHub publishing card */
.github-card {
  padding: var(--spacing-3);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.github-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-2);
}

.github-lede {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.github-connect,
.github-connected {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.device-panel {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  padding: var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-sm);
  background: var(--color-background-secondary);
}

.device-instructions {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.device-code-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.device-code {
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  letter-spacing: 0.15em;
  color: var(--color-text-primary);
  padding: var(--spacing-1) var(--spacing-2);
  background: var(--color-background-primary);
  border-radius: var(--border-radius-sm);
  border: 1px solid var(--color-border-primary);
}

.device-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  flex-wrap: wrap;
}

.device-waiting,
.publish-status {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.publish-status {
  margin: 0;
}

.device-error {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.device-error p {
  margin: 0;
}

.pat-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.pat-warning {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-warning);
}

.confirm-check :deep(.el-checkbox__label) {
  white-space: normal;
  line-height: 1.4;
  color: var(--color-text-secondary);
}

.connected-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  /* Neither child can shrink below its label, so without this the row overflows
     the card instead of reflowing. */
  flex-wrap: wrap;
}

/* The label swaps between "Publishing…" and "Create/Update pull request"; without a
   floor the whole row resizes on every publish-state change. */
.connected-actions .btn-primary {
  min-width: 11.5rem;
}

.link-btn {
  background: none;
  border: none;
  padding: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
  cursor: pointer;
  text-decoration: underline;
}

.link-btn:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}

.github-error {
  margin-top: var(--spacing-1);
}

/* Success panel */
.pr-success {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.pr-success-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.open-pr-note {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border);
  border-radius: var(--radius-md);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.open-pr-note i {
  color: var(--color-accent);
}

.readme-label {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.pr-success-head {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.pr-success-head i {
  color: var(--color-success);
}

.whats-next-title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.whats-next ul {
  margin: var(--spacing-1) 0 0;
  padding-left: 1.1rem;
  display: grid;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

/* Footer */
.publish-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
  width: 100%;
}

.footer-hint {
  margin: 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  text-align: left;
}

.footer-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-shrink: 0;
}
</style>
