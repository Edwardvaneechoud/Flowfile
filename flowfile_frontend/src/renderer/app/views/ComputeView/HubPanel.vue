<template>
  <div class="hub-container">
    <div class="mb-3">
      <h2 class="page-title">Hub</h2>
      <p class="page-description">
        <template v-if="state?.joined">The hub this server belongs to.</template>
        <template v-else>
          Connect this Flowfile to your organisation's hub. You sign in once with your hub account;
          the password is sent to the hub and never kept here. Your local workspace stays as it is.
        </template>
      </p>
    </div>

    <ComputeCard card-id="hub-connection" title="Hub connection" icon="fa-solid fa-tower-broadcast">
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
                {{ state.orgName ? `${state.orgName}'s hub` : "the hub" }} at
                {{ state.hubUrl ?? "an unknown address" }}.</strong
              >
              <p v-if="state.account" data-testid="hub-account">
                Signed in as {{ state.account }}.
              </p>
            </template>
            <template v-else>
              <strong>{{ state.orgName ?? state.hubUrl }}</strong>
              <p>
                Connected to <code>{{ state.hubUrl }}</code> as the device
                <code>{{ state.deviceName }}</code
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
          <dd>{{ state.hubVersion ?? "—" }}</dd>
          <dt>Session expires</dt>
          <dd>{{ expiresLabel }}</dd>
        </dl>

        <p class="compute-field__hint hub-card__hint" data-testid="hub-servers-link">
          The hub's servers, with whether this computer reaches them, are on
          <router-link :to="{ query: { tab: 'backends' } }">Backends</router-link>.
        </p>
      </template>

      <div v-else-if="state" class="hub-card__fields">
        <label class="compute-field hub-card__wide">
          <span class="compute-field__label">Hub URL</span>
          <el-input
            v-model="form.hubUrl"
            v-bind="NO_AUTOFILL"
            placeholder="https://hub.example.com"
            data-testid="hub-url"
          />
        </label>
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
        <label class="compute-field hub-card__wide">
          <span class="compute-field__label">Device name</span>
          <el-input v-model="form.deviceName" v-bind="NO_AUTOFILL" data-testid="hub-device-name" />
          <span class="compute-field__hint">
            How this machine shows on the hub. The same name under the same account is the same
            device.
          </span>
        </label>
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
          v-else
          type="primary"
          size="small"
          :loading="hub.busy"
          :disabled="!canConnect"
          data-testid="hub-connect"
          @click="onConnect"
        >
          Connect
        </el-button>
      </template>
    </ComputeCard>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, watch } from "vue";
import { ElMessage } from "element-plus";
import { NO_AUTOFILL } from "../../utils/noAutofill";
import ComputeCard from "./ComputeCard.vue";
import { useHubConnectionStore } from "../../stores/hub-connection-store";
import type { StatusSummary } from "../../components/settings/handshakeStatus";

const hub = useHubConnectionStore();
const state = computed(() => hub.connection);

const form = reactive({ hubUrl: "", username: "", password: "", deviceName: "" });

watch(state, (next) => {
  if (next && !next.connected && !form.deviceName) form.deviceName = next.deviceName ?? "";
});

const summary = computed<StatusSummary | null>(() => {
  if (!state.value) return null;
  if (state.value.joined) return { tone: "success", label: "Joined" };
  if (!state.value.connected) return { tone: "neutral", label: "Not connected" };
  if (!state.value.reachable) return { tone: "warning", label: "Hub not answering" };
  return { tone: "success", label: "Connected" };
});

const expiresLabel = computed(() =>
  state.value?.expiresAt ? new Date(state.value.expiresAt).toLocaleString() : "—",
);

const canConnect = computed(
  () => form.hubUrl.trim() !== "" && form.username.trim() !== "" && form.password !== "",
);

const onConnect = async () => {
  if (!canConnect.value) return;
  try {
    const next = await hub.connect({
      hubUrl: form.hubUrl.trim(),
      username: form.username.trim(),
      password: form.password,
      deviceName: form.deviceName.trim() || undefined,
    });
    ElMessage.success(`Connected to ${next.orgName ?? next.hubUrl}.`);
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
.compute-card__banner {
  margin-bottom: var(--spacing-3);
}
.hub-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
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
</style>
