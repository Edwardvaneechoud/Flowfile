<template>
  <div class="hub-container">
    <div class="mb-3">
      <h2 class="page-title">Hub</h2>
      <p class="page-description">
        <template v-if="state?.joined">The hub this server belongs to.</template>
        <template v-else>
          Connect this Flowfile to your organisation's hub. You sign in once with your hub account,
          in your browser or with a username and password; no password is kept here. Your local
          workspace stays as it is.
        </template>
      </p>
      <p v-if="!state?.connected && !state?.joined" class="hub-note" data-testid="hub-coming-soon">
        Coming soon.
      </p>
    </div>

    <ComputeCard
      card-id="hub-connection"
      title="Hub connection"
      icon="fa-solid fa-tower-broadcast"
      :default-open="false"
    >
      <template #meta="{ open }">
        <span
          v-if="!open && summary"
          :class="['compute-chip', `compute-chip--${summary.tone}`]"
          data-testid="hub-summary"
          >{{ summary.label }}</span
        >
      </template>

      <div v-if="hub.loadError" class="compute-card__banner">
        <i class="fa-solid fa-circle-exclamation"></i>
        <span>{{ hub.loadError }}</span>
      </div>

      <p v-if="hub.loading && !state" class="compute-card__loading">Loading…</p>

      <template v-if="state && (state.connected || state.joined)">
        <div
          class="compute-card__status"
          :class="
            state.reachable ? 'compute-card__status--success' : 'compute-card__status--warning'
          "
          :data-testid="state.joined ? 'hub-joined' : 'hub-status'"
        >
          <i
            :class="
              state.reachable ? 'fa-solid fa-circle-check' : 'fa-solid fa-triangle-exclamation'
            "
            aria-hidden="true"
          ></i>
          <div>
            <template v-if="state.joined">
              <strong
                >This server belongs to
                {{ state.org_name ? `${state.org_name}'s hub` : "the hub" }} at
                <a
                  v-if="state.hub_url"
                  :href="state.hub_url"
                  class="hub-card__link"
                  data-testid="hub-url-link"
                  @click.prevent="openHub(state.hub_url)"
                  >{{ state.hub_url }}</a
                ><template v-else>an unknown address</template>.</strong
              >
              <p v-if="state.account" data-testid="hub-account">
                Signed in as {{ state.account }}.
              </p>
            </template>
            <template v-else>
              <strong>{{ state.org_name ?? state.hub_url }}</strong>
              <p>
                Connected to
                <a
                  v-if="state.hub_url"
                  :href="state.hub_url"
                  class="hub-card__link"
                  data-testid="hub-url-link"
                  @click.prevent="openHub(state.hub_url)"
                  ><code>{{ state.hub_url }}</code></a
                >
                as the device
                <code>{{ state.device_name }}</code
                >.
              </p>
            </template>
            <p v-if="!state.reachable">The hub is not answering: {{ state.error }}</p>
          </div>
        </div>

        <dl v-if="!state.joined" class="hub-card__facts">
          <dt>Account</dt>
          <dd>{{ state.account ?? "—" }}</dd>
          <dt>Hub version</dt>
          <dd>{{ state.hub_version ?? "—" }}</dd>
          <dt>Session expires</dt>
          <dd>{{ expiresLabel }}</dd>
        </dl>

        <p class="compute-field__hint hub-card__hint" data-testid="hub-servers-link">
          The hub's servers, with whether this computer reaches them, are on
          <router-link :to="{ query: { tab: 'backends' } }">Backends</router-link>.
        </p>
      </template>

      <div v-else-if="state" class="hub-card__fields" @keyup.enter="onSubmit">
        <label class="compute-field hub-card__wide">
          <span class="compute-field__label">Hub URL</span>
          <el-input
            v-model="form.hubUrl"
            v-bind="NO_AUTOFILL"
            placeholder="https://hub.example.com"
            data-testid="hub-url"
          />
        </label>
        <template v-if="usePassword">
          <label class="compute-field">
            <span class="compute-field__label">Username</span>
            <el-input v-model="form.username" autocomplete="username" data-testid="hub-username" />
          </label>
          <label class="compute-field">
            <span class="compute-field__label">Password</span>
            <el-input
              v-model="form.password"
              type="password"
              show-password
              autocomplete="current-password"
              data-testid="hub-password"
            />
          </label>
        </template>
        <label class="compute-field hub-card__wide">
          <span class="compute-field__label">Device name</span>
          <el-input v-model="form.deviceName" v-bind="NO_AUTOFILL" data-testid="hub-device-name" />
          <span class="compute-field__hint">
            How this machine shows on the hub. The same name under the same account is the same
            device.
          </span>
        </label>
        <p
          v-if="hub.waitingForBrowser"
          class="hub-card__wide hub-card__waiting"
          data-testid="hub-browser-waiting"
        >
          <i class="fa-solid fa-spinner fa-spin" aria-hidden="true"></i>
          <span>Waiting for you to sign in in your browser…</span>
          <a
            v-if="hub.browserSignInUrl"
            :href="hub.browserSignInUrl"
            class="hub-card__link"
            data-testid="hub-browser-reopen"
            @click.prevent="openHub(hub.browserSignInUrl)"
            >Open the sign-in page again</a
          >
        </p>
        <p
          v-if="passwordNote && usePassword"
          class="compute-field__hint hub-card__wide"
          data-testid="hub-password-only"
        >
          {{ passwordNote }}
        </p>
        <div v-if="!hub.waitingForBrowser" class="hub-card__wide">
          <button
            v-if="!usePassword"
            type="button"
            class="hub-card__switch"
            data-testid="hub-use-password"
            @click="mode = 'password'"
          >
            Use a username and password instead
          </button>
          <button
            v-else-if="browserAvailable"
            type="button"
            class="hub-card__switch"
            data-testid="hub-use-browser"
            @click="mode = 'browser'"
          >
            Sign in with your browser instead
          </button>
        </div>
      </div>

      <template v-if="state && !state.joined" #footer>
        <el-button
          v-if="state.connected"
          size="small"
          plain
          :loading="hub.busy"
          data-testid="hub-disconnect"
          @click="onDisconnect"
        >
          Disconnect
        </el-button>
        <el-button
          v-else-if="usePassword"
          type="primary"
          size="small"
          :loading="hub.busy"
          :disabled="!canConnect"
          data-testid="hub-connect"
          @click="onConnect"
        >
          Connect
        </el-button>
        <el-button
          v-else-if="hub.waitingForBrowser"
          size="small"
          plain
          data-testid="hub-browser-cancel"
          @click="onCancelBrowser"
        >
          Cancel
        </el-button>
        <el-button
          v-else
          type="primary"
          size="small"
          :loading="hub.busy"
          :disabled="!canStartBrowser"
          data-testid="hub-browser-sign-in"
          @click="onBrowserSignIn"
        >
          Sign in with your browser
        </el-button>
      </template>
    </ComputeCard>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { NO_AUTOFILL } from "../../utils/noAutofill";
import ComputeCard from "./ComputeCard.vue";
import { desktop } from "../../../lib/desktop";
import { BrowserSignInUnavailable, useHubConnectionStore } from "../../stores/hub-connection-store";
import type { StatusSummary } from "../../components/settings/handshakeStatus";

const hub = useHubConnectionStore();
const state = computed(() => hub.connection);

const form = reactive({ hubUrl: "", username: "", password: "", deviceName: "" });

watch(state, (next) => {
  if (next && !next.connected && !form.deviceName) form.deviceName = next.device_name ?? "";
});

const mode = ref<"browser" | "password">("browser");
/** Why the password form is shown: the hub's or this machine's refusal of a sign-in through the browser. */
const passwordNote = ref("");
const hubRefusedBrowser = ref(false);
const browserAvailable = computed(
  () => state.value?.browser_sign_in !== false && !hubRefusedBrowser.value,
);
const usePassword = computed(() => mode.value === "password" || !browserAvailable.value);

watch(
  () => form.hubUrl,
  () => {
    hubRefusedBrowser.value = false;
    passwordNote.value = "";
  },
);

const summary = computed<StatusSummary | null>(() => {
  if (!state.value) return null;
  if (state.value.joined) return { tone: "success", label: "Joined" };
  if (!state.value.connected) return { tone: "neutral", label: "Not connected" };
  if (!state.value.reachable) return { tone: "warning", label: "Hub not answering" };
  return { tone: "success", label: "Connected" };
});

function openHub(url: string) {
  void desktop.openExternal(url);
}

const expiresLabel = computed(() =>
  state.value?.expires_at ? new Date(state.value.expires_at).toLocaleString() : "—",
);

const canConnect = computed(
  () => form.hubUrl.trim() !== "" && form.username.trim() !== "" && form.password !== "",
);

const canStartBrowser = computed(() => form.hubUrl.trim() !== "");

const onBrowserSignIn = async () => {
  if (!canStartBrowser.value || hub.busy || hub.waitingForBrowser) return;
  try {
    const next = await hub.signInWithBrowser(
      { hub_url: form.hubUrl.trim(), device_name: form.deviceName.trim() || undefined },
      (url) => desktop.openExternal(url),
    );
    if (next) ElMessage.success(`Connected to ${next.org_name ?? next.hub_url}.`);
  } catch (error) {
    if (error instanceof BrowserSignInUnavailable) {
      hubRefusedBrowser.value = true;
      mode.value = "password";
      passwordNote.value = error.message;
      return;
    }
    ElMessage.error((error as Error).message);
  }
};

const onCancelBrowser = () => {
  void hub.cancelBrowserSignIn();
};

const onSubmit = () => {
  if (usePassword.value) void onConnect();
  else void onBrowserSignIn();
};

const onConnect = async () => {
  if (!canConnect.value || hub.busy) return;
  try {
    const next = await hub.connect({
      hub_url: form.hubUrl.trim(),
      username: form.username.trim(),
      password: form.password,
      device_name: form.deviceName.trim() || undefined,
    });
    ElMessage.success(`Connected to ${next.org_name ?? next.hub_url}.`);
  } catch (error) {
    ElMessage.error((error as Error).message);
  } finally {
    form.password = "";
  }
};

const onDisconnect = async () => {
  try {
    await hub.disconnect();
    ElMessage.success("Disconnected from the hub.");
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

onMounted(() => {
  void hub.load();
});
</script>

<style scoped src="./computeSettings.css"></style>
<style scoped>
.hub-card__link {
  color: var(--color-accent);
  text-decoration: underline;
}
.compute-card__banner {
  margin-bottom: var(--spacing-3);
}
.hub-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}
.hub-note {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}
.hub-card__facts {
  display: grid;
  grid-template-columns: max-content minmax(0, 1fr);
  gap: var(--spacing-1) var(--spacing-4);
  margin: var(--spacing-4) 0 0;
  font-size: var(--font-size-sm);
}
.hub-card__facts dt {
  color: var(--color-text-secondary);
}
.hub-card__facts dd {
  margin: 0;
  overflow-wrap: anywhere;
}
.hub-card__hint {
  margin: var(--spacing-4) 0 0;
}
.hub-card__fields {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
  align-items: start;
  gap: var(--spacing-4);
}
.hub-card__wide {
  grid-column: 1 / -1;
}
.hub-card__waiting {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
.hub-card__switch {
  padding: 0;
  border: none;
  background: none;
  font: inherit;
  font-size: var(--font-size-sm);
  color: var(--color-accent);
  text-decoration: underline;
  cursor: pointer;
}
</style>
