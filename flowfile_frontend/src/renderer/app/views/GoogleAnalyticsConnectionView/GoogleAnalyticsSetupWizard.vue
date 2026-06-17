<template>
  <el-dialog
    :model-value="modelValue"
    title="Set up a Google Analytics connection"
    width="820px"
    :close-on-click-modal="false"
    class="ga-wizard-dialog"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <div class="wizard-grid">
      <el-steps
        direction="vertical"
        :active="activeIndex"
        finish-status="success"
        class="wizard-rail"
      >
        <el-step v-for="s in visibleSteps" :key="s.id" :title="s.title" />
      </el-steps>

      <div ref="bodyRef" class="wizard-body" @keyup.enter="onEnter">
        <!-- Step: plain-language method router -->
        <div v-if="currentStep?.id === 'method'" class="step-body">
          <h3 class="step-title">How will you use this connection?</h3>
          <p class="step-text">
            We'll pick the right way to connect and walk you through it — no need to know the
            technical details.
          </p>
          <el-radio-group v-model="method" class="method-choices">
            <el-radio value="service_account" class="method-choice">
              <span class="method-choice-text">
                <span class="method-choice-title">Run reports on a schedule, or unattended</span>
                <span class="method-choice-sub">
                  We'll set up a <strong>service account</strong> — recommended. It runs on its own
                  and never needs you to sign in again.
                </span>
              </span>
            </el-radio>
            <el-radio value="oauth" class="method-choice">
              <span class="method-choice-text">
                <span class="method-choice-title"
                  >Each analyst signs in with their own Google account</span
                >
                <span class="method-choice-sub">
                  We'll set up <strong>Google sign-in</strong> (a one-time Google Cloud OAuth
                  setup).
                </span>
              </span>
            </el-radio>
          </el-radio-group>
        </div>

        <!-- Service account: pick/create project -->
        <div v-else-if="currentStep?.id === 'sa-project'" class="step-body">
          <h3 class="step-title">Pick or create a Google Cloud project</h3>
          <ol class="step-instructions">
            <li>
              Already use Google Cloud? Open the
              <SetupLink href="https://console.cloud.google.com/projectselector2/home/dashboard">
                project selector
              </SetupLink>
              and pick a project.
            </li>
            <li>
              New to Google Cloud?
              <SetupLink href="https://console.cloud.google.com/projectcreate">
                Create a project
              </SetupLink>
              first.
            </li>
          </ol>
          <el-form label-position="top" class="project-field" @submit.prevent>
            <el-form-item label="Google Cloud project ID (optional)">
              <el-input v-model="projectId" placeholder="e.g. my-project-123456" />
              <p class="step-hint">
                Find it in the console's top bar — click the project name — or on the dashboard's
                <em>Project info</em> card. It looks like <code>my-project-123456</code> (the
                lowercase ID, not the display name). We'll then point every console link at it.
              </p>
            </el-form-item>
          </el-form>
          <el-checkbox v-model="confirmed['sa-project']">I have a project selected</el-checkbox>
        </div>

        <!-- Service account: create -->
        <div v-else-if="currentStep?.id === 'sa-create'" class="step-body">
          <h3 class="step-title">Create the service account</h3>
          <p class="step-text">
            A service account is a "robot" Google login Flowfile uses to read your reports — no
            coding, about 5 minutes in Google's console.
          </p>
          <ol class="step-instructions">
            <li>
              Open
              <SetupLink
                :href="consoleUrl('https://console.cloud.google.com/iam-admin/serviceaccounts')"
              >
                Service Accounts
              </SetupLink>
              and click <em>Create service account</em>.
            </li>
            <li>Give it a name and click <em>Done</em>. It gets its own email address.</li>
          </ol>
          <el-checkbox v-model="confirmed['sa-create']">I created the service account</el-checkbox>
        </div>

        <!-- Service account: enable API -->
        <div v-else-if="currentStep?.id === 'sa-enable'" class="step-body">
          <h3 class="step-title">Turn on the Analytics API</h3>
          <ol class="step-instructions">
            <li>
              Open the
              <SetupLink
                :href="
                  consoleUrl(
                    'https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com',
                  )
                "
              >
                Analytics Data API
              </SetupLink>
              page and click <em>Enable</em>.
            </li>
          </ol>
          <p class="step-hint">
            <i class="fa-solid fa-circle-check"></i>
            It's on when the blue <em>Enable</em> button becomes a <em>Manage</em> button.
          </p>
          <el-checkbox v-model="confirmed['sa-enable']">I enabled the API</el-checkbox>
        </div>

        <!-- Service account: download + paste key -->
        <div v-else-if="currentStep?.id === 'sa-key'" class="step-body">
          <h3 class="step-title">Download the key &amp; paste it here</h3>
          <ol class="step-instructions">
            <li>
              Open
              <SetupLink
                :href="consoleUrl('https://console.cloud.google.com/iam-admin/serviceaccounts')"
              >
                your service account
              </SetupLink>
              → <em>Keys</em> tab → <em>Add key → Create new key → JSON</em>.
            </li>
            <li>
              A small <code>.json</code> file downloads — keep it safe, it works like a password.
            </li>
            <li>Open it in any text editor, copy everything, and paste it below.</li>
          </ol>
          <div class="step-callout">
            <i class="fa-solid fa-triangle-exclamation"></i>
            <span>
              Seeing <em>"Service account key creation is disabled"</em>? Your Google organisation
              blocks downloadable keys (<code>iam.disableServiceAccountKeyCreation</code>). Switch
              to
              <button type="button" class="link-btn" @click="switchToOAuth">
                Sign in with Google
              </button>
              — it needs no key — or ask an Org Policy admin to exempt that constraint.
            </span>
          </div>
          <el-input
            v-model="serviceAccountKey"
            type="textarea"
            :rows="6"
            placeholder="Paste the .json file contents here"
          />
          <p v-if="serviceAccountKey.trim() && keyValidation.error" class="step-error">
            <i class="fa-solid fa-circle-exclamation"></i> {{ keyValidation.error }}
          </p>
          <p v-else-if="keyValidation.valid" class="step-ok">
            <i class="fa-solid fa-circle-check"></i> Looks good — key for
            {{ keyValidation.clientEmail }}
          </p>
        </div>

        <!-- Service account: grant viewer -->
        <div v-else-if="currentStep?.id === 'sa-grant'" class="step-body">
          <h3 class="step-title">Give it Viewer access to your data</h3>
          <p class="step-text">
            In
            <SetupLink href="https://analytics.google.com/analytics/web/">
              Google Analytics → Admin → Property access management
            </SetupLink>
            , click <em>+ → Add users</em>, paste this email, and choose <strong>Viewer</strong>:
          </p>
          <div class="copy-row">
            <code class="copy-code">
              {{ keyValidation.clientEmail || "(paste your key in the previous step)" }}
            </code>
            <button
              type="button"
              class="copy-btn"
              :disabled="!keyValidation.clientEmail"
              :aria-label="`Copy ${keyValidation.clientEmail ?? ''}`"
              @click="copyValue(keyValidation.clientEmail)"
            >
              <i class="fa-solid fa-copy"></i> Copy
            </button>
          </div>
          <p class="step-hint">
            This is the robot account's identity — the <code>client_email</code> line inside the key
            you pasted. You can find your <strong>property ID</strong> in GA under
            <em>Admin → Property settings</em>.
          </p>
          <el-checkbox v-model="confirmed['sa-grant']">I granted Viewer access</el-checkbox>
        </div>

        <!-- Service account: name -->
        <div v-else-if="currentStep?.id === 'sa-name'" class="step-body">
          <h3 class="step-title">Name this connection</h3>
          <el-form label-position="top" @submit.prevent>
            <el-form-item label="Connection name" required>
              <el-input v-model="connectionName" placeholder="e.g. marketing-ga4" />
              <p v-if="nameTaken" class="step-error">
                A connection named "{{ connectionName.trim() }}" already exists.
              </p>
              <p v-else class="step-hint">A unique name nodes use to reference this connection.</p>
            </el-form-item>
            <el-form-item label="Default GA4 property ID (optional)">
              <el-input v-model="defaultPropertyId" placeholder="e.g. 123456789" />
              <p class="step-hint">
                Set this so the next step can verify the service account actually has access.
              </p>
            </el-form-item>
            <el-form-item label="Description (optional)">
              <el-input v-model="description" placeholder="Optional note" />
            </el-form-item>
          </el-form>
        </div>

        <!-- Service account: save -->
        <div v-else-if="currentStep?.id === 'sa-save'" class="step-body">
          <h3 class="step-title">Save the connection</h3>
          <p class="step-text">
            We'll encrypt the key with your user key and create
            <strong>{{ connectionName.trim() }}</strong
            >. Click <em>Save connection</em> to continue.
          </p>
          <ul class="summary-list">
            <li><span>Method</span><strong>Service account</strong></li>
            <li>
              <span>Service account</span><strong>{{ keyValidation.clientEmail }}</strong>
            </li>
            <li v-if="defaultPropertyId.trim()">
              <span>Property</span><strong>{{ defaultPropertyId.trim() }}</strong>
            </li>
          </ul>
        </div>

        <!-- OAuth: project -->
        <div v-else-if="currentStep?.id === 'oauth-project'" class="step-body">
          <h3 class="step-title">Pick or create a Google Cloud project</h3>
          <ol class="step-instructions">
            <li>
              Already use Google Cloud? Open the
              <SetupLink href="https://console.cloud.google.com/projectselector2/home/dashboard">
                project selector
              </SetupLink>
              and pick a project.
            </li>
            <li>
              New to Google Cloud?
              <SetupLink href="https://console.cloud.google.com/projectcreate">
                Create a project
              </SetupLink>
              first.
            </li>
          </ol>
          <el-form label-position="top" class="project-field" @submit.prevent>
            <el-form-item label="Google Cloud project ID (optional)">
              <el-input v-model="projectId" placeholder="e.g. my-project-123456" />
              <p class="step-hint">
                Find it in the console's top bar — click the project name — or on the dashboard's
                <em>Project info</em> card. It looks like <code>my-project-123456</code> (the
                lowercase ID, not the display name). We'll then point every console link at it.
              </p>
            </el-form-item>
          </el-form>
          <el-checkbox v-model="confirmed['oauth-project']">I have a project selected</el-checkbox>
        </div>

        <!-- OAuth: enable API -->
        <div v-else-if="currentStep?.id === 'oauth-enable'" class="step-body">
          <h3 class="step-title">Turn on the Analytics API</h3>
          <ol class="step-instructions">
            <li>
              Open the
              <SetupLink
                :href="
                  consoleUrl(
                    'https://console.cloud.google.com/apis/library/analyticsdata.googleapis.com',
                  )
                "
              >
                Analytics Data API
              </SetupLink>
              page and click <em>Enable</em>.
            </li>
          </ol>
          <el-checkbox v-model="confirmed['oauth-enable']">I enabled the API</el-checkbox>
        </div>

        <!-- OAuth: consent screen (Google Auth Platform) -->
        <div v-else-if="currentStep?.id === 'oauth-consent'" class="step-body">
          <h3 class="step-title">Set up the Google Auth Platform</h3>
          <p class="step-text">
            In the
            <SetupLink :href="consoleUrl('https://console.cloud.google.com/auth/overview')">
              Google Auth Platform
            </SetupLink>
            , set up these two pages. If you've used Google sign-in in this project before they may
            already be done — just confirm and continue. On a brand-new project, Google's page shows
            a <em>Get started</em> button — click that there first (it's on Google's side, not
            here).
          </p>
          <ol class="step-instructions">
            <li>
              <SetupLink :href="consoleUrl('https://console.cloud.google.com/auth/branding')">
                Branding
              </SetupLink>
              — enter an app name and your support email.
            </li>
            <li>
              <SetupLink :href="consoleUrl('https://console.cloud.google.com/auth/audience')">
                Audience
              </SetupLink>
              — if a Google Workspace org owns this project and only colleagues need it, choose
              <em>Internal</em> (simplest — no warning, no expiry). Otherwise choose
              <em>External</em> and click <em>Publish app</em> (status <em>In production</em>; a
              Testing app's sign-in expires after 7 days).
            </li>
          </ol>
          <el-checkbox v-model="confirmed['oauth-consent']"
            >I configured the Auth Platform</el-checkbox
          >
        </div>

        <!-- OAuth: create client -->
        <div v-else-if="currentStep?.id === 'oauth-client'" class="step-body">
          <h3 class="step-title">Create the OAuth client</h3>
          <ol class="step-instructions">
            <li>
              Open
              <SetupLink :href="consoleUrl('https://console.cloud.google.com/auth/clients')">
                Clients
              </SetupLink>
              and click <em>Create client</em>.
            </li>
            <li>Pick <strong>Web application</strong> as the application type.</li>
          </ol>
          <el-checkbox v-model="confirmed['oauth-client']"
            >I created a Web application client</el-checkbox
          >
        </div>

        <!-- OAuth: redirect URI -->
        <div v-else-if="currentStep?.id === 'oauth-redirect'" class="step-body">
          <h3 class="step-title">Add the redirect URI</h3>
          <p class="step-text">
            In that OAuth client, under <em>Authorized redirect URIs</em>, add this
            <strong>exactly</strong> — no trailing slash, no extra path:
          </p>
          <div class="copy-row">
            <code class="copy-code">{{ redirectUri }}</code>
            <button
              type="button"
              class="copy-btn"
              :aria-label="`Copy ${redirectUri}`"
              @click="copyValue(redirectUri)"
            >
              <i class="fa-solid fa-copy"></i> Copy
            </button>
          </div>
          <div v-if="redirectWarning" class="step-callout">
            <i class="fa-solid fa-triangle-exclamation"></i>
            <span>{{ redirectWarning }}</span>
          </div>
          <el-checkbox v-model="confirmed['oauth-redirect']">I added the redirect URI</el-checkbox>
        </div>

        <!-- OAuth: save client config -->
        <div v-else-if="currentStep?.id === 'oauth-config'" class="step-body">
          <h3 class="step-title">Save your Client ID &amp; Secret</h3>
          <p class="step-text">
            Paste the Client ID and Secret from the OAuth client you just created.
          </p>
          <el-form label-position="top" @submit.prevent>
            <el-form-item label="Client ID" required>
              <el-input v-model="clientId" placeholder="xxxx.apps.googleusercontent.com" />
            </el-form-item>
            <el-form-item label="Client Secret" :required="!oauthConfiguredLocal">
              <el-input
                v-model="clientSecret"
                type="password"
                show-password
                :placeholder="
                  oauthConfiguredLocal ? 'Leave blank to keep the stored secret' : 'GOCSPX-...'
                "
              />
            </el-form-item>
          </el-form>
          <p class="step-hint">
            Flowfile encrypts the secret at rest with your master key and never returns it to the
            browser.
          </p>
        </div>

        <!-- OAuth: name + connect -->
        <div v-else-if="currentStep?.id === 'oauth-connect'" class="step-body">
          <div v-if="!showOauthConsoleSteps" class="info-banner">
            <i class="fa-solid fa-circle-info"></i>
            <span>
              Google sign-in is already set up on this instance — you just need to connect an
              account.
              <button type="button" class="link-btn" @click="enableReconfigure">
                Reconfigure the OAuth client
              </button>
            </span>
          </div>
          <h3 class="step-title">Name &amp; connect your Google account</h3>
          <el-form label-position="top" @submit.prevent>
            <el-form-item label="Connection name" required>
              <el-input v-model="connectionName" placeholder="e.g. marketing-ga4" />
              <p v-if="nameTaken" class="step-error">
                A connection named "{{ connectionName.trim() }}" already exists.
              </p>
            </el-form-item>
            <el-form-item label="Default GA4 property ID (optional)">
              <el-input v-model="defaultPropertyId" placeholder="e.g. 123456789" />
            </el-form-item>
          </el-form>
          <p class="step-text">
            <em>Connect Google account</em> opens Google sign-in. Use an account with
            <strong>Viewer</strong> access to your GA4 property.
          </p>
        </div>

        <!-- Verify (shared by both branches) -->
        <div v-else-if="currentStep?.kind === 'verify'" class="step-body">
          <h3 class="step-title">Test the connection</h3>
          <p class="step-text">Let's confirm Flowfile can read your Google Analytics data.</p>
          <div v-if="testResult" class="test-result" :class="testResult.success ? 'ok' : 'fail'">
            <i
              :class="
                testResult.success ? 'fa-solid fa-circle-check' : 'fa-solid fa-circle-exclamation'
              "
            ></i>
            <span>{{ testResult.message }}</span>
          </div>
          <p v-if="testResult && !testResult.success" class="step-hint">
            Double-check the
            {{ method === "service_account" ? "service account" : "signed-in account" }} has
            <strong>Viewer</strong> access and the property ID is correct, then test again.
          </p>
        </div>

        <p v-if="currentStep?.kind === 'confirm'" class="step-hint confirm-hint">
          <i class="fa-solid fa-circle-check"></i>
          Tick the box above once you've done this, then click <em>Next</em> to continue.
        </p>
      </div>
    </div>

    <template #footer>
      <div class="wizard-footer">
        <div class="footer-left">
          <button
            v-if="method === 'oauth'"
            type="button"
            class="link-btn"
            @click="emit('open-oauth-guide')"
          >
            <i class="fa-solid fa-book"></i> Read the full Google OAuth guide
          </button>
        </div>
        <div class="footer-right">
          <el-button
            v-if="activeIndex > 0 && currentStep?.kind !== 'verify'"
            :disabled="primaryLoading"
            @click="onBack"
          >
            Back
          </el-button>
          <template v-if="currentStep?.kind === 'verify'">
            <el-button
              v-if="!testResult || !testResult.success"
              type="primary"
              :loading="testing"
              @click="runTest"
            >
              Test connection
            </el-button>
            <el-button
              :type="testResult && testResult.success ? 'primary' : 'default'"
              @click="finish"
            >
              Finish
            </el-button>
          </template>
          <el-button
            v-else
            type="primary"
            :loading="primaryLoading"
            :disabled="!canAdvance"
            @click="onPrimary"
          >
            {{ primaryLabel }}
          </el-button>
        </div>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from "vue";
import {
  ElButton,
  ElCheckbox,
  ElDialog,
  ElForm,
  ElFormItem,
  ElInput,
  ElMessage,
  ElRadio,
  ElRadioGroup,
  ElStep,
  ElSteps,
} from "element-plus";
import SetupLink from "./SetupLink.vue";
import { fetchGoogleOAuthConfig, saveGoogleOAuthConfig } from "./oauthClientApi";
import { testGoogleAnalyticsConnection } from "./api";
import { copyToClipboard } from "../../utils/clipboardUtils";
import {
  canAdvanceStep,
  computeVisibleSteps,
  redirectUriWarning,
  validateServiceAccountKey,
  withProject,
} from "./wizardLogic";
import type { GaSetupMethod, WizardGateState } from "./wizardLogic";
import type {
  GoogleAnalyticsConnectionInterface,
  GoogleAnalyticsConnectionMetadata,
  GoogleAnalyticsConnectionTestResult,
  GoogleAnalyticsServiceAccountInput,
} from "./GoogleAnalyticsConnectionTypes";

const props = defineProps<{
  modelValue: boolean;
  oauthConfigured: boolean;
  isSubmitting: boolean;
  isConnecting: boolean;
  redirectUri: string;
  connections: GoogleAnalyticsConnectionInterface[];
}>();

const emit = defineEmits<{
  (e: "update:modelValue", open: boolean): void;
  (e: "save-service-account", input: GoogleAnalyticsServiceAccountInput): void;
  (e: "connect-oauth", metadata: GoogleAnalyticsConnectionMetadata): void;
  (e: "refresh-oauth-configured"): void;
  (e: "open-oauth-guide"): void;
}>();

const bodyRef = ref<HTMLElement | null>(null);
const activeIndex = ref(0);
const method = ref<GaSetupMethod>("service_account");
const confirmed = reactive<Record<string, boolean>>({});
const serviceAccountKey = ref("");
const connectionName = ref("");
const description = ref("");
const defaultPropertyId = ref("");
// Optional Google Cloud project id used to deep-link the console pages to the
// right project. Shared across both branches.
const projectId = ref("");
const clientId = ref("");
const clientSecret = ref("");
// Decided once on entering the OAuth branch (or via Reconfigure) so saving the
// client config mid-flow doesn't reshuffle the step list under the user.
const showOauthConsoleSteps = ref(true);
const reconfigureOauth = ref(false);
const oauthConfiguredLocal = ref(props.oauthConfigured);
const savingConfig = ref(false);
const testing = ref(false);
const testResult = ref<GoogleAnalyticsConnectionTestResult | null>(null);

const redirectUri = computed(() => props.redirectUri);
const redirectWarning = computed(() => redirectUriWarning(props.redirectUri));
const consoleUrl = (base: string) => withProject(base, projectId.value);
const keyValidation = computed(() => validateServiceAccountKey(serviceAccountKey.value));
const existingNames = computed(() => props.connections.map((c) => c.connectionName));
const nameTaken = computed(() => {
  const trimmed = connectionName.value.trim();
  return trimmed.length > 0 && existingNames.value.includes(trimmed);
});

const visibleSteps = computed(() =>
  computeVisibleSteps({ method: method.value, showOauthConsoleSteps: showOauthConsoleSteps.value }),
);
const currentStep = computed(() => visibleSteps.value[activeIndex.value]);

const wizardState = computed<WizardGateState>(() => ({
  method: method.value,
  confirmed,
  serviceAccountKey: serviceAccountKey.value,
  connectionName: connectionName.value,
  clientId: clientId.value,
  clientSecret: clientSecret.value,
  oauthConfigured: oauthConfiguredLocal.value,
  existingNames: existingNames.value,
}));
const canAdvance = computed(() => canAdvanceStep(currentStep.value, wizardState.value));

const primaryLabel = computed(() => {
  switch (currentStep.value?.id) {
    case "method":
      return "Continue";
    case "sa-save":
      return "Save connection";
    case "oauth-config":
      return "Save & continue";
    case "oauth-connect":
      return "Connect Google account";
    default:
      return "Next";
  }
});
const primaryLoading = computed(() => {
  switch (currentStep.value?.id) {
    case "sa-save":
      return props.isSubmitting;
    case "oauth-config":
      return savingConfig.value;
    case "oauth-connect":
      return props.isConnecting;
    default:
      return false;
  }
});

const resetWizard = () => {
  activeIndex.value = 0;
  method.value = "service_account";
  Object.keys(confirmed).forEach((key) => delete confirmed[key]);
  serviceAccountKey.value = "";
  connectionName.value = "";
  description.value = "";
  defaultPropertyId.value = "";
  projectId.value = "";
  clientId.value = "";
  clientSecret.value = "";
  showOauthConsoleSteps.value = true;
  reconfigureOauth.value = false;
  oauthConfiguredLocal.value = props.oauthConfigured;
  savingConfig.value = false;
  testing.value = false;
  testResult.value = null;
};

const refreshOauthConfig = async () => {
  try {
    oauthConfiguredLocal.value = (await fetchGoogleOAuthConfig()).isConfigured;
  } catch (error) {
    console.error("Failed to load Google OAuth config status:", error);
  }
};

const goToVerify = () => {
  const idx = visibleSteps.value.findIndex((s) => s.kind === "verify");
  if (idx >= 0) activeIndex.value = idx;
};

const enableReconfigure = () => {
  reconfigureOauth.value = true;
  showOauthConsoleSteps.value = true;
  activeIndex.value = 1;
};

// Escape hatch when an org policy blocks service-account keys: jump back to the
// method router with the OAuth path selected (watch(method) clears SA inputs).
const switchToOAuth = () => {
  method.value = "oauth";
  activeIndex.value = 0;
};

const onBack = () => {
  if (activeIndex.value > 0) activeIndex.value -= 1;
};

const saveOauthConfig = async () => {
  savingConfig.value = true;
  try {
    await saveGoogleOAuthConfig({
      clientId: clientId.value.trim(),
      clientSecret: clientSecret.value.trim(),
      redirectUri: redirectUri.value,
    });
    oauthConfiguredLocal.value = true;
    emit("refresh-oauth-configured");
    ElMessage.success("Google OAuth client saved");
    activeIndex.value += 1;
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    ElMessage.error(`Failed to save: ${message}`);
  } finally {
    savingConfig.value = false;
  }
};

const onPrimary = async () => {
  const step = currentStep.value;
  if (!step) return;
  if (step.id === "method") {
    if (method.value === "oauth") {
      await refreshOauthConfig();
      showOauthConsoleSteps.value = reconfigureOauth.value || !oauthConfiguredLocal.value;
    }
    activeIndex.value = 1;
    return;
  }
  if (step.id === "sa-save") {
    emit("save-service-account", {
      connectionName: connectionName.value.trim(),
      serviceAccountKey: serviceAccountKey.value.trim(),
      description: description.value,
      defaultPropertyId: defaultPropertyId.value,
    });
    return;
  }
  if (step.id === "oauth-config") {
    await saveOauthConfig();
    return;
  }
  if (step.id === "oauth-connect") {
    emit("connect-oauth", {
      connectionName: connectionName.value.trim(),
      description: description.value,
      defaultPropertyId: defaultPropertyId.value,
    });
    return;
  }
  activeIndex.value += 1;
};

const runTest = async () => {
  testing.value = true;
  try {
    testResult.value = await testGoogleAnalyticsConnection(connectionName.value.trim());
  } finally {
    testing.value = false;
  }
};

const finish = () => emit("update:modelValue", false);

const copyValue = async (value: string | null) => {
  if (!value) return;
  if (await copyToClipboard(value)) {
    ElMessage.success("Copied");
  } else {
    ElMessage.error("Could not access clipboard — copy it manually");
  }
};

const onEnter = (event: KeyboardEvent) => {
  if ((event.target as HTMLElement)?.tagName === "TEXTAREA") return;
  if (currentStep.value?.kind === "verify") return;
  if (canAdvance.value && !primaryLoading.value) onPrimary();
};

// Open: reset to a clean wizard each time.
watch(
  () => props.modelValue,
  (open) => {
    if (open) resetWizard();
  },
);

// Changing method clears the other branch's collected inputs / confirmations.
watch(method, () => {
  Object.keys(confirmed).forEach((key) => delete confirmed[key]);
  serviceAccountKey.value = "";
  clientId.value = "";
  clientSecret.value = "";
  testResult.value = null;
});

watch(
  () => props.oauthConfigured,
  (value) => {
    oauthConfiguredLocal.value = value;
  },
);

// Advance after async actions by observing the real connection list (the parent
// refreshes it on SA save / OAuth callback), never optimistically.
watch(
  () => props.connections,
  (list) => {
    const id = currentStep.value?.id;
    const name = connectionName.value.trim();
    if (!name) return;
    if (
      id === "sa-save" &&
      list.some((c) => c.connectionName === name && c.authMethod === "service_account")
    ) {
      goToVerify();
    } else if (
      id === "oauth-connect" &&
      list.some((c) => c.connectionName === name && c.authMethod === "oauth" && c.oauthUserEmail)
    ) {
      goToVerify();
    }
  },
);

// On step change, focus the first input so keyboard users land in the right place.
watch(activeIndex, () => {
  nextTick(() => {
    bodyRef.value?.querySelector<HTMLElement>("input, textarea")?.focus();
  });
});
</script>

<style scoped>
.wizard-grid {
  display: grid;
  grid-template-columns: 230px 1fr;
  gap: var(--spacing-5);
  min-height: 340px;
}

.wizard-rail {
  border-right: 1px solid var(--color-border);
  padding-right: var(--spacing-4);
}

.wizard-body {
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
}

.step-title {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-lg);
  font-weight: var(--font-weight-medium);
}

.step-text {
  margin: 0 0 var(--spacing-3);
  color: var(--color-text-secondary);
  line-height: 1.55;
}

.step-instructions {
  margin: 0 0 var(--spacing-3);
  padding-left: var(--spacing-5);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.step-instructions li {
  line-height: 1.5;
}

.step-instructions code,
.step-text code,
.step-hint code {
  background: var(--color-background-muted);
  padding: 1px 6px;
  border-radius: 4px;
  font-size: 0.85em;
  word-break: break-word;
}

.step-hint {
  margin: var(--spacing-2) 0 var(--spacing-3);
  color: var(--color-text-tertiary);
  font-size: var(--font-size-xs);
  line-height: 1.5;
}

.step-hint i {
  color: var(--color-success, #10b981);
  margin-right: 4px;
}

.confirm-hint {
  margin-top: var(--spacing-3);
}

.step-error {
  margin: var(--spacing-2) 0 0;
  color: var(--color-danger, #dc2626);
  font-size: var(--font-size-xs);
}

.step-ok {
  margin: var(--spacing-2) 0 0;
  color: var(--color-success, #10b981);
  font-size: var(--font-size-xs);
}

.project-field {
  margin: var(--spacing-3) 0;
}

.step-callout {
  display: flex;
  gap: var(--spacing-2);
  margin: var(--spacing-3) 0;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-warning-light, #fef3c7);
  border-left: 3px solid var(--color-warning, #f59e0b);
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.step-callout > i {
  color: var(--color-warning, #f59e0b);
  margin-top: 2px;
}

.step-callout code {
  word-break: break-word;
}

.method-choices {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  width: 100%;
}

:deep(.method-choice) {
  display: flex;
  align-items: flex-start;
  width: 100%;
  height: auto;
  margin: 0;
  padding: var(--spacing-3);
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius-md, 8px);
  white-space: normal;
}

:deep(.method-choice.is-checked) {
  border-color: var(--color-accent);
  background: var(--color-accent-subtle);
}

:deep(.method-choice .el-radio__label) {
  white-space: normal;
}

.method-choice-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.method-choice-title {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.method-choice-sub {
  color: var(--color-text-secondary);
  font-size: var(--font-size-xs);
  line-height: 1.45;
}

.copy-row {
  display: flex;
  align-items: stretch;
  gap: var(--spacing-2);
  margin: var(--spacing-2) 0;
}

.copy-code {
  flex: 1;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-background-muted);
  border-radius: var(--border-radius-sm);
  font-size: 0.85em;
  word-break: break-all;
  user-select: all;
}

.copy-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  padding: 0 var(--spacing-3);
  background: var(--color-background-muted);
  border: 1px solid var(--color-border);
  border-radius: var(--border-radius-sm);
  color: var(--color-text-secondary);
  font-size: 0.8em;
  cursor: pointer;
  white-space: nowrap;
}

.copy-btn:hover:not(:disabled) {
  background: var(--color-accent-subtle);
  color: var(--color-accent);
}

.copy-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.summary-list {
  list-style: none;
  margin: var(--spacing-3) 0 0;
  padding: var(--spacing-3) var(--spacing-4);
  background: var(--color-background-muted);
  border-radius: var(--border-radius-sm);
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.summary-list li {
  display: flex;
  justify-content: space-between;
  gap: var(--spacing-3);
  font-size: var(--font-size-sm);
}

.summary-list span {
  color: var(--color-text-tertiary);
}

.info-banner {
  display: flex;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-accent-subtle);
  border-left: 3px solid var(--color-accent);
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  line-height: 1.5;
}

.info-banner i {
  color: var(--color-accent);
  margin-top: 2px;
}

.test-result {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: var(--spacing-3) 0;
  padding: var(--spacing-3);
  border-radius: var(--border-radius-md, 8px);
  font-size: var(--font-size-sm);
}

.test-result.ok {
  background: var(--color-success-light, #d1fae5);
  color: var(--color-success-dark, #065f46);
}

.test-result.fail {
  background: var(--color-warning-light, #fef3c7);
  color: var(--color-warning-dark, #92400e);
}

.wizard-footer {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: var(--spacing-3);
}

.footer-right {
  display: flex;
  gap: var(--spacing-2);
}

.link-btn {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  background: transparent;
  border: none;
  padding: 0;
  color: var(--color-accent);
  font-size: var(--font-size-xs);
  cursor: pointer;
}

.link-btn:hover {
  text-decoration: underline;
  text-underline-offset: 2px;
}
</style>
