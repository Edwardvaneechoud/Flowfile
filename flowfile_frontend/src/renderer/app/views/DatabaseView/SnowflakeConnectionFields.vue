<template>
  <div class="snowflake-fields">
    <div class="form-field">
      <label for="snowflake-account" class="form-label">Account</label>
      <input
        id="snowflake-account"
        v-model="connection.host"
        type="text"
        class="form-input"
        :class="{ 'is-error': accountError }"
        placeholder="myorg-myaccount or a link to your Snowflake account"
        autocomplete="off"
        spellcheck="false"
      />
      <div v-if="accountError" class="form-error" data-testid="snowflake-account-error">
        {{ accountError }}
      </div>
      <div v-else-if="accountHost" class="form-hint" data-testid="snowflake-account-preview">
        → {{ accountHost }}
      </div>
    </div>

    <div class="form-field">
      <span class="form-label">Sign-in method</span>
      <el-radio-group v-model="selectedMethod" class="method-group">
        <el-radio-button
          v-for="method in authMethods"
          :key="method"
          :value="method"
          :data-testid="`snowflake-method-${method}`"
        >
          {{ methodLabel(method) }}
        </el-radio-button>
      </el-radio-group>
      <div class="form-hint">{{ methodDescription(connection.authMethod) }}</div>
    </div>

    <template v-if="connection.authMethod === 'oauth'">
      <div class="form-field">
        <label for="snowflake-role" class="form-label">Role</label>
        <input
          id="snowflake-role"
          v-model="connection.role"
          type="text"
          class="form-input"
          placeholder="Default role"
          autocomplete="off"
        />
        <div class="form-hint">
          Leave empty to use your default role. Changing the role means signing in again.
        </div>
      </div>

      <div class="form-field">
        <div v-if="signedInText" class="identity-row">
          <span class="identity-pill" data-testid="snowflake-signed-in">
            <i class="fa-solid fa-circle-check"></i>
            {{ signedInText }}
          </span>
          <button type="button" class="link-button" @click="startSignIn">Sign in again</button>
        </div>
        <div v-else-if="isWaiting" class="identity-row" data-testid="snowflake-signin-waiting">
          <span class="waiting-text">
            <i class="fa-solid fa-spinner fa-spin"></i>
            {{ signInStatusText(signIn.phase) }}
          </span>
          <button type="button" class="link-button" @click="cancelSignIn">Cancel</button>
        </div>
        <div v-else>
          <button
            type="button"
            class="btn btn-primary"
            data-testid="snowflake-signin"
            @click="startSignIn"
          >
            <i class="fa-solid fa-arrow-up-right-from-square"></i>
            Sign in with Snowflake
          </button>
          <div
            v-if="signIn.phase === 'error' || signIn.phase === 'timeout'"
            class="form-error"
            data-testid="snowflake-signin-error"
          >
            {{ signInStatusText(signIn.phase, { message: signIn.message }) }}
          </div>
        </div>
      </div>
    </template>

    <template
      v-else-if="connection.authMethod === 'access_token' || connection.authMethod === 'key_pair'"
    >
      <div class="form-field">
        <label for="snowflake-username" class="form-label">Username</label>
        <input
          id="snowflake-username"
          v-model="connection.username"
          type="text"
          class="form-input"
          placeholder="Your Snowflake user"
          autocomplete="off"
        />
      </div>

      <div v-if="connection.authMethod === 'access_token'" class="form-field">
        <label for="snowflake-token" class="form-label">Access token</label>
        <div class="password-field">
          <input
            id="snowflake-token"
            v-model="connection.password"
            :type="showToken ? 'text' : 'password'"
            class="form-input"
            :placeholder="keepsStoredSecret ? 'Leave blank to keep existing' : 'Access token'"
            autocomplete="off"
          />
          <button
            type="button"
            class="toggle-visibility"
            aria-label="Toggle token visibility"
            @click="showToken = !showToken"
          >
            <i :class="showToken ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
          </button>
        </div>
        <div class="form-hint">
          Generate one in Snowflake under your user's Programmatic access tokens.
          <button type="button" class="link-button" @click="openTokenDocs">How?</button>
        </div>
      </div>

      <template v-else>
        <div class="form-field">
          <span class="form-label">Private key</span>
          <div class="key-row">
            <button
              type="button"
              class="btn btn-secondary"
              data-testid="snowflake-key-file"
              @click="keyFileInput?.click()"
            >
              <i class="fa-solid fa-key"></i>
              Choose .p8 file
            </button>
            <span v-if="keyFileName" class="key-file-name">{{ keyFileName }}</span>
            <span v-else-if="keepsStoredSecret && !connection.password" class="form-hint">
              Leave empty to keep the stored key.
            </span>
          </div>
          <input
            ref="keyFileInput"
            type="file"
            accept=".p8,.pem,.key,text/plain"
            hidden
            @change="onKeyFileChosen"
          />
          <div v-if="keyFileError" class="form-error">{{ keyFileError }}</div>
          <button type="button" class="link-button paste-toggle" @click="showPaste = !showPaste">
            <i :class="showPaste ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"></i>
            or paste the key
          </button>
          <textarea
            v-if="showPaste"
            id="snowflake-private-key"
            v-model="connection.password"
            class="form-input form-textarea key-textarea"
            placeholder="-----BEGIN PRIVATE KEY-----"
            spellcheck="false"
            @input="keyFileName = null"
          ></textarea>
        </div>

        <div class="form-field">
          <label for="snowflake-passphrase" class="form-label">Passphrase</label>
          <div class="password-field">
            <input
              id="snowflake-passphrase"
              v-model="connection.privateKeyPassphrase"
              :type="showPassphrase ? 'text' : 'password'"
              class="form-input"
              placeholder="Optional"
              autocomplete="off"
            />
            <button
              type="button"
              class="toggle-visibility"
              aria-label="Toggle passphrase visibility"
              @click="showPassphrase = !showPassphrase"
            >
              <i :class="showPassphrase ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
          <div class="form-hint">
            Only if the key file is encrypted. It is used once and not stored.
          </div>
        </div>
      </template>

      <div class="form-field">
        <div class="identity-row">
          <button
            type="button"
            class="btn btn-secondary"
            data-testid="snowflake-connect"
            :disabled="!discoverReady || discover.loading"
            @click="connect"
          >
            <i :class="discover.loading ? 'fa-solid fa-spinner fa-spin' : 'fa-solid fa-plug'"></i>
            Connect
          </button>
          <span v-if="discover.connected" class="identity-pill" data-testid="snowflake-connected">
            <i class="fa-solid fa-circle-check"></i>
            {{ identityText("Connected as", discover.username, discover.role) }}
          </span>
        </div>
        <div v-if="discover.error" class="form-error" data-testid="snowflake-connect-error">
          {{ discover.error }}
        </div>
      </div>
    </template>

    <div class="form-field">
      <label class="form-label">Warehouse</label>
      <el-select
        v-model="connection.warehouse"
        data-testid="snowflake-warehouse"
        class="full-width"
        filterable
        allow-create
        default-first-option
        clearable
        placeholder="Choose or type a warehouse"
        no-data-text="Sign in to list your warehouses, or type a name"
      >
        <el-option v-for="name in warehouseChoices" :key="name" :label="name" :value="name" />
      </el-select>
    </div>

    <div class="form-field">
      <label class="form-label">Database</label>
      <el-select
        v-model="connection.database"
        data-testid="snowflake-database"
        class="full-width"
        filterable
        allow-create
        default-first-option
        clearable
        placeholder="Choose or type a database"
        no-data-text="Sign in to list your databases, or type a name"
      >
        <el-option v-for="name in databaseChoices" :key="name" :label="name" :value="name" />
      </el-select>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { computed, onBeforeUnmount, reactive, ref, watch } from "vue";
import { ElOption, ElRadioButton, ElRadioGroup, ElSelect } from "element-plus";

import { desktop } from "../../../lib/desktop";
import { docsUrl } from "../../lib/docsLinks";
import type { FullDatabaseConnection } from "./databaseConnectionTypes";
import {
  discoverSnowflake,
  getSnowflakeOAuthStatus,
  SnowflakeSignInExpiredError,
  startSnowflakeOAuth,
} from "./snowflakeApi";
import {
  MAX_KEY_FILE_BYTES,
  type SignInPhase,
  canDiscover,
  identityText,
  methodDescription,
  methodLabel,
  nextPollDelayMs,
  previewAccount,
  selectOptions,
  signInDeadline,
  signInStatusText,
  storedSignInStillValid,
} from "./snowflakeConnection";

const MAX_POLL_FAILURES = 5;

const props = defineProps<{
  isEditing?: boolean;
  storedAuthMethod?: string | null;
  storedHost?: string | null;
  storedRole?: string | null;
  authMethods: string[];
}>();

const connection = defineModel<FullDatabaseConnection>({ required: true });

const context = computed(() => ({
  isEditing: !!props.isEditing,
  storedAuthMethod: props.storedAuthMethod,
  storedHost: props.storedHost,
  storedRole: props.storedRole,
}));

const preview = computed(() =>
  (connection.value.host ?? "").trim() ? previewAccount(connection.value.host ?? "") : null,
);
const accountHost = computed(() =>
  preview.value && "host" in preview.value ? preview.value.host : "",
);
const accountError = computed(() =>
  preview.value && "error" in preview.value ? preview.value.error : "",
);

const selectedMethod = computed<string | undefined>({
  get: () => connection.value.authMethod ?? undefined,
  set: (method) => {
    connection.value.authMethod = method ?? null;
  },
});

const keepsStoredSecret = computed(
  () => !!props.isEditing && props.storedAuthMethod === connection.value.authMethod,
);

const warehouseOptions = ref<string[]>([]);
const databaseOptions = ref<string[]>([]);
const warehouseChoices = computed(() =>
  selectOptions(warehouseOptions.value, connection.value.warehouse),
);
const databaseChoices = computed(() =>
  selectOptions(databaseOptions.value, connection.value.database),
);

function applySession(session: {
  warehouse: string | null;
  database: string | null;
  warehouses: string[];
  databases: string[];
}) {
  warehouseOptions.value = session.warehouses;
  databaseOptions.value = session.databases;
  if (!connection.value.warehouse && session.warehouse) {
    connection.value.warehouse = session.warehouse;
  }
  if (!connection.value.database && session.database) {
    connection.value.database = session.database;
  }
}

watch(
  () => props.authMethods,
  (methods) => {
    if (methods.length && !methods.includes(connection.value.authMethod ?? "")) {
      connection.value.authMethod = methods[0];
    }
  },
  { immediate: true },
);

const signIn = reactive<{
  phase: SignInPhase;
  message: string | null;
  username: string | null;
  role: string | null;
}>({ phase: "idle", message: null, username: null, role: null });

let signInAttempt = 0;
let pollTimer: ReturnType<typeof setTimeout> | null = null;

const isWaiting = computed(() => signIn.phase === "starting" || signIn.phase === "waiting");

const signedInText = computed(() => {
  if (signIn.phase === "ok") {
    return signInStatusText("ok", { username: signIn.username, role: signIn.role });
  }
  if (signIn.phase === "idle" && storedSignInStillValid(connection.value, context.value)) {
    return signInStatusText("ok", {
      username: connection.value.username,
      role: connection.value.role,
    });
  }
  return "";
});

function stopPolling() {
  if (pollTimer !== null) {
    clearTimeout(pollTimer);
    pollTimer = null;
  }
}

function resetSignIn() {
  signInAttempt += 1;
  stopPolling();
  signIn.phase = "idle";
  signIn.message = null;
  signIn.username = null;
  signIn.role = null;
  connection.value.oauthFlowId = null;
}

function cancelSignIn() {
  resetSignIn();
}

function failSignIn(phase: "error" | "timeout", message: string | null = null) {
  stopPolling();
  signIn.phase = phase;
  signIn.message = message;
}

function schedulePoll(attempt: number, flowId: string, deadline: number, failures: number) {
  const delay = nextPollDelayMs(Date.now(), deadline);
  if (delay === null) {
    failSignIn("timeout");
    return;
  }
  pollTimer = setTimeout(async () => {
    pollTimer = null;
    if (attempt !== signInAttempt) return;
    try {
      const status = await getSnowflakeOAuthStatus(flowId);
      if (attempt !== signInAttempt) return;
      if (status.status === "ok") {
        signIn.phase = "ok";
        signIn.username = status.username;
        signIn.role = status.role;
        connection.value.oauthFlowId = flowId;
        if (status.username) connection.value.username = status.username;
        applySession(status);
        return;
      }
      if (status.status === "error") {
        failSignIn("error", status.message);
        return;
      }
      schedulePoll(attempt, flowId, deadline, 0);
    } catch (error) {
      if (attempt !== signInAttempt) return;
      const message = error instanceof Error ? error.message : null;
      if (error instanceof SnowflakeSignInExpiredError || failures + 1 >= MAX_POLL_FAILURES) {
        failSignIn("error", message);
        return;
      }
      schedulePoll(attempt, flowId, deadline, failures + 1);
    }
  }, delay);
}

async function startSignIn() {
  resetSignIn();
  const attempt = signInAttempt;
  const account = previewAccount(connection.value.host ?? "");
  if ("error" in account) {
    failSignIn("error", account.error);
    return;
  }
  signIn.phase = "starting";
  try {
    const { authUrl, flowId } = await startSnowflakeOAuth(
      (connection.value.host ?? "").trim(),
      (connection.value.role ?? "").trim() || null,
    );
    if (attempt !== signInAttempt) return;
    // Opens the system browser on desktop and a new tab on the web.
    await desktop.openExternal(authUrl);
    if (attempt !== signInAttempt) return;
    signIn.phase = "waiting";
    schedulePoll(attempt, flowId, signInDeadline(Date.now()), 0);
  } catch (error) {
    if (attempt !== signInAttempt) return;
    failSignIn("error", error instanceof Error ? error.message : null);
  }
}

watch(
  () => [connection.value.host, connection.value.role],
  () => {
    if (signIn.phase !== "idle") resetSignIn();
  },
);

const showToken = ref(false);
const showPassphrase = ref(false);
const showPaste = ref(false);
const keyFileName = ref<string | null>(null);
const keyFileError = ref<string | null>(null);
const keyFileInput = ref<HTMLInputElement | null>(null);

const discover = reactive<{
  loading: boolean;
  connected: boolean;
  error: string | null;
  username: string | null;
  role: string | null;
}>({ loading: false, connected: false, error: null, username: null, role: null });
let discoverAttempt = 0;

const discoverReady = computed(() => canDiscover(connection.value, context.value));

function resetDiscover() {
  discoverAttempt += 1;
  discover.loading = false;
  discover.connected = false;
  discover.error = null;
}

async function connect() {
  const method = connection.value.authMethod;
  if (method !== "access_token" && method !== "key_pair") return;
  resetDiscover();
  const attempt = discoverAttempt;
  discover.loading = true;
  const secret = connection.value.password ?? "";
  try {
    const session = await discoverSnowflake({
      account: (connection.value.host ?? "").trim(),
      authMethod: method,
      username: (connection.value.username ?? "").trim(),
      secret: method === "key_pair" ? secret : secret.trim(),
      privateKeyPassphrase:
        method === "key_pair" ? connection.value.privateKeyPassphrase || null : null,
      role: (connection.value.role ?? "").trim() || null,
      connectionName: props.isEditing ? connection.value.connectionName : null,
    });
    if (attempt !== discoverAttempt) return;
    discover.connected = true;
    discover.username = session.username;
    discover.role = session.role;
    applySession(session);
  } catch (error) {
    if (attempt !== discoverAttempt) return;
    discover.error = error instanceof Error ? error.message : "Could not connect to Snowflake";
  } finally {
    if (attempt === discoverAttempt) discover.loading = false;
  }
}

watch(
  () => [
    connection.value.host,
    connection.value.username,
    connection.value.password,
    connection.value.privateKeyPassphrase,
  ],
  () => {
    if (discover.connected || discover.error || discover.loading) resetDiscover();
  },
);

watch(
  () => connection.value.authMethod,
  (method, previous) => {
    if (method === previous) return;
    resetSignIn();
    resetDiscover();
    connection.value.password = "";
    connection.value.privateKeyPassphrase = null;
    keyFileName.value = null;
    keyFileError.value = null;
  },
);

async function onKeyFileChosen(event: Event) {
  const input = event.target as HTMLInputElement;
  const file = input.files?.[0];
  input.value = "";
  if (!file) return;
  if (file.size > MAX_KEY_FILE_BYTES) {
    keyFileError.value = "That file is too large to be a private key (limit 64 KB).";
    return;
  }
  try {
    connection.value.password = await file.text();
    keyFileName.value = file.name;
    keyFileError.value = null;
  } catch {
    keyFileError.value = "Could not read that file.";
  }
}

function openTokenDocs() {
  desktop.openExternal(docsUrl("users/visual-editor/connections.html#snowflake"));
}

onBeforeUnmount(() => {
  signInAttempt += 1;
  discoverAttempt += 1;
  stopPolling();
});
</script>

<style scoped>
.snowflake-fields {
  grid-column: 1 / -1;
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.method-group {
  flex-wrap: wrap;
}

.identity-row,
.key-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  flex-wrap: wrap;
}

.identity-pill {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1) var(--spacing-3);
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-sm);
}

.waiting-text {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
}

.link-button {
  padding: 0;
  background: transparent;
  border: none;
  color: var(--color-accent);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
}

.link-button:hover {
  text-decoration: underline;
  text-underline-offset: 2px;
}

.paste-toggle {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  align-self: flex-start;
  margin-top: var(--spacing-1);
}

.key-file-name {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  word-break: break-all;
}

.key-textarea {
  font-family: var(--font-family-mono, monospace);
  font-size: var(--font-size-xs);
  min-height: 96px;
}

.full-width {
  width: 100%;
}
</style>
