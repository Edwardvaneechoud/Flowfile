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

        <!-- Success: PR opened / already open -->
        <div v-else-if="prResult" class="pr-success">
          <div class="pr-success-head">
            <i class="fa-solid fa-circle-check"></i>
            <span>{{
              prResult.status === "already_open"
                ? "A pull request for this version is already open."
                : "Pull request created!"
            }}</span>
          </div>
          <button class="btn btn-primary btn-sm" @click="openPr">
            <i class="fa-solid fa-arrow-up-right-from-square"></i>
            View pull request{{ prResult.pr_number ? ` #${prResult.pr_number}` : "" }}
          </button>
          <div class="whats-next">
            <span class="whats-next-title">What happens next</span>
            <ul>
              <li>Automated checks validate your node and dry-run its test setup.</li>
              <li>
                First-time contributor? The checks wait until a maintainer approves the run — that's
                expected, not stuck.
              </li>
              <li>A maintainer reviews your node.</li>
              <li>Once merged, it appears in the community catalog within ~10 minutes.</li>
            </ul>
          </div>
        </div>

        <!-- Connected: confirm + create PR -->
        <div v-else-if="connected" class="github-connected">
          <el-checkbox v-model="confirmed" class="confirm-check">
            My node does what its description says — no hidden network calls or data collection.
          </el-checkbox>
          <div class="connected-actions">
            <button class="btn btn-primary" :disabled="!canPublish" @click="createPr">
              <i class="fa-solid fa-code-pull-request"></i>
              {{ publishing ? "Publishing…" : "Create pull request" }}
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
  checkPublishCompleteness,
  deletePublishScreenshot,
  disconnectGithub,
  fetchPublishScreenshotUrl,
  getGithubStatus,
  listPublishScreenshots,
  pollGithubDevice,
  publishBundle,
  publishPr,
  saveGithubPat,
  startGithubDevice,
  uploadPublishScreenshot,
} from "../../api/nodeDesigner";
import {
  FORK_RETRY_DELAY_MS,
  FORK_RETRY_MAX,
  canCreatePr,
  describeGithubError,
  nextPollDelayMs,
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
  { key: "description", label: "Description (10+ characters)", codes: ["DESCRIPTION_MISSING"] },
  { key: "examples", label: "Test setup", codes: ["EXAMPLES_MISSING"] },
  { key: "license", label: "License selected", codes: ["LICENSE_INVALID"] },
  { key: "icon", label: "PNG icon", codes: ["ICON_NOT_PNG", "ICON_DEFAULT", "ICON_MISSING"] },
  { key: "screenshots", label: "Screenshot added", codes: ["NO_SCREENSHOTS"] },
];

const store = useNodeDesignerStore();

const license = ref("MIT");
const category = ref("Utilities");
const description = ref("");
const repository = ref("");

const issues = ref<PublishIssue[]>([]);
const checking = ref(false);
const downloading = ref(false);

const screenshots = ref<PublishScreenshot[]>([]);
const screenshotUrls = ref<Record<string, string>>({});

const fileStem = computed(() => props.fileName.replace(/\.py$/, ""));
const hasErrors = computed(() => issues.value.some((i) => i.severity === "error"));

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

function bundleBody() {
  return {
    file_name: props.fileName,
    license: license.value,
    repository: repository.value,
    description: description.value,
    category: category.value,
  };
}

let checkTimer: ReturnType<typeof setTimeout> | null = null;
function scheduleCheck() {
  if (checkTimer) clearTimeout(checkTimer);
  checkTimer = setTimeout(runCheck, 250);
}

async function runCheck() {
  if (!props.fileName) return;
  checking.value = true;
  try {
    const report = await checkPublishCompleteness(bundleBody());
    issues.value = report.issues;
  } catch (e: unknown) {
    const err = e as { response?: { data?: { detail?: string } }; message?: string };
    ElMessage.error(
      `Could not check readiness: ${err.response?.data?.detail || err.message || "error"}`,
    );
  } finally {
    checking.value = false;
  }
}

function revokeUrls() {
  Object.values(screenshotUrls.value).forEach((url) => URL.revokeObjectURL(url));
  screenshotUrls.value = {};
}

async function loadScreenshots() {
  if (!props.fileName) return;
  try {
    const shots = await listPublishScreenshots(fileStem.value);
    revokeUrls();
    screenshots.value = shots;
    for (const shot of shots) {
      fetchPublishScreenshotUrl(fileStem.value, shot.file_name).then((url) => {
        if (url) screenshotUrls.value = { ...screenshotUrls.value, [shot.file_name]: url };
      });
    }
  } catch {
    screenshots.value = [];
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

function resetGithubTransient() {
  device.value = null;
  deviceError.value = "";
  githubError.value = "";
  publishStatusText.value = "";
  publishing.value = false;
  connecting.value = false;
  prResult.value = null;
  confirmed.value = false;
}

watch(
  () => props.show,
  (open) => {
    if (!open) {
      stopGithubTimers();
      resetGithubTransient();
      return;
    }
    if (!description.value.trim()) description.value = store.nodeMetadata.intro ?? "";
    runCheck();
    loadScreenshots();
    loadGithubStatus();
  },
);

watch([license, category, description, repository], () => {
  if (props.show) scheduleCheck();
});

onBeforeUnmount(() => {
  if (checkTimer) clearTimeout(checkTimer);
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
