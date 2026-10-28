<template>
  <div class="backends-container">
    <header class="backends-heading">
      <div class="backends-heading__icon">
        <i class="fa-solid fa-network-wired" aria-hidden="true"></i>
      </div>
      <div>
        <h2 class="page-title">Compute backends</h2>
        <p class="page-description">
          Data nodes run on the default worker unless a flow or node picks another. Python Script
          and kernel nodes run in Python kernels instead.
        </p>
      </div>
    </header>

    <template v-if="isAdmin">
      <section
        class="compute-card default-worker"
        data-testid="default-worker-card"
        aria-labelledby="default-worker-title"
      >
        <header class="compute-card__header">
          <span class="compute-card__icon"
            ><i class="fa-solid fa-house-laptop" aria-hidden="true"></i
          ></span>
          <div class="compute-card__intro">
            <h3 id="default-worker-title" class="compute-card__title">Default worker</h3>
            <p class="compute-card__description">
              The built-in worker on this server. Flows set to Auto, and nodes that follow them, run
              here.
            </p>
          </div>
          <span class="backends-count">Built in</span>
        </header>
        <div class="compute-card__body">
          <WorkerPoolCard />
        </div>
      </section>

      <section class="compute-card" aria-labelledby="configured-workers-title">
        <header class="compute-card__header">
          <span class="compute-card__icon"
            ><i class="fa-solid fa-server" aria-hidden="true"></i
          ></span>
          <div class="compute-card__intro">
            <h3 id="configured-workers-title" class="compute-card__title">Other workers</h3>
            <p class="compute-card__description">
              Workers on other hosts that a flow or node can pick by id. Each keeps its own warm
              pool, set on that worker.
            </p>
          </div>
          <span v-if="!loading" class="backends-count"
            >{{ backends.length }} {{ backends.length === 1 ? "worker" : "workers" }}</span
          >
        </header>
        <div class="compute-card__body">
          <div v-if="loadError" class="backends-banner" role="alert">
            <i class="fa-solid fa-circle-exclamation" aria-hidden="true"></i
            ><span>{{ loadError }}</span>
          </div>
          <p v-if="loading && !backends.length" class="backends-loading">Loading workers…</p>
          <ul v-if="backends.length" class="backends-list" aria-label="Configured workers">
            <li v-for="backend in backends" :key="backend.id" class="backend-worker">
              <div class="backend-worker__header">
                <div class="backend-worker__identity">
                  <h4 class="backend-worker__name">
                    <code>{{ backend.id }}</code>
                  </h4>
                  <div class="backends-url">{{ backend.url }}</div>
                </div>
                <div
                  class="backend-worker__files"
                  :data-testid="`backend-server-files-${backend.id}`"
                >
                  <span>Sees this server's files</span>
                  <strong>{{ serverFilesLabel(backend, verdicts[backend.id]) }}</strong>
                </div>
                <div class="backends-actions">
                  <el-button
                    size="small"
                    :loading="busy[backend.id]"
                    :aria-label="`Handshake ${backend.id}`"
                    @click="onHandshake(backend.id)"
                  >
                    <i class="fa-solid fa-rotate" aria-hidden="true"></i> Handshake
                  </el-button>
                  <el-button
                    size="small"
                    type="danger"
                    plain
                    :loading="busy[backend.id]"
                    :aria-label="`Remove ${backend.id}`"
                    @click="onRemove(backend.id)"
                    >Remove</el-button
                  >
                </div>
              </div>
              <BackendHandshakeStatus :verdict="verdicts[backend.id]" />
            </li>
          </ul>
          <div v-else-if="!loading && !loadError" class="backends-empty">
            <i class="fa-solid fa-server" aria-hidden="true"></i>
            <div class="backends-empty__title">No workers configured</div>
            <p>Add your first worker below to make it available to your flows.</p>
          </div>
        </div>
      </section>

      <section class="compute-card" aria-labelledby="add-worker-title">
        <header class="compute-card__header">
          <span class="compute-card__icon"
            ><i class="fa-solid fa-plus" aria-hidden="true"></i
          ></span>
          <div class="compute-card__intro">
            <h3 id="add-worker-title" class="compute-card__title">Add a worker</h3>
            <p class="compute-card__description">
              Connect a worker using its URL and bearer token.
            </p>
          </div>
        </header>
        <form @submit.prevent="onAdd">
          <div class="compute-card__body">
            <div class="backends-add__fields">
              <label class="compute-field">
                <span class="compute-field__label">Name</span>
                <el-input v-model="form.name" v-bind="NO_AUTOFILL" placeholder="team"
                  ><template #prepend>worker:</template></el-input
                >
              </label>
              <label class="compute-field">
                <span class="compute-field__label">Worker URL</span>
                <el-input
                  v-model="form.url"
                  v-bind="NO_AUTOFILL"
                  placeholder="https://worker.example.com"
                />
                <span v-if="plainHttp" class="compute-field__hint backends-field__warning">
                  Plain HTTP sends the token and plans unencrypted. Anyone with the token can run
                  code on the worker. Outside a trusted network, use HTTPS.
                </span>
              </label>
              <label class="compute-field">
                <span class="compute-field__label">Bearer token</span>
                <el-input
                  v-model="form.token"
                  v-bind="NO_AUTOFILL"
                  type="password"
                  show-password
                  placeholder="Worker access token"
                />
              </label>
            </div>

            <div class="backends-options">
              <div class="backends-option">
                <el-checkbox
                  v-model="form.shares_core_disk"
                  data-testid="backend-server-files-input"
                  aria-describedby="backend-server-files-hint"
                  >Sees this server's files</el-checkbox
                >
                <p id="backend-server-files-hint" class="compute-field__hint">
                  Enable only when the worker can access this server's files at the same paths, on
                  this host or through shared folders.
                </p>
                <details class="compute-disclosure">
                  <summary>About shared file access</summary>
                  <p class="compute-field__hint">
                    Mount the server's data, flows and kernel shared folders at the same paths.
                    Flows can then read and write local paths, and Python Script and kernel nodes
                    may run there. Leave this off for a worker that sets
                    <code>FLOWFILE_WORKER_LOCAL_FILES=1</code> itself.
                  </p>
                </details>
              </div>
              <details
                class="compute-disclosure backends-certificate"
                :open="form.ca_cert ? true : undefined"
              >
                <summary>
                  <i class="fa-solid fa-shield-halved" aria-hidden="true"></i> Private TLS
                  certificate <span class="backends-optional">Optional</span>
                </summary>
                <label class="compute-field">
                  <span class="compute-field__label">CA certificate (PEM)</span>
                  <span class="compute-field__hint"
                    >For a certificate that isn't publicly trusted, paste the CA certificate this
                    server should trust.</span
                  >
                  <el-input
                    v-model="form.ca_cert"
                    v-bind="NO_AUTOFILL"
                    type="textarea"
                    :rows="4"
                    placeholder="-----BEGIN CERTIFICATE-----"
                  />
                </label>
              </details>
            </div>
          </div>
          <footer class="compute-card__footer">
            <p class="compute-field__hint">
              To change an existing worker, remove it and add it again.
            </p>
            <el-button type="primary" native-type="submit" :loading="adding" :disabled="!canAdd"
              ><i class="fa-solid fa-plus" aria-hidden="true"></i> Add worker</el-button
            >
          </footer>
        </form>
      </section>

      <ExchangeStoreCard />
    </template>

    <div v-else class="backends-locked">
      <i class="fa-solid fa-lock" aria-hidden="true"></i>
      <h3>Administrator setting</h3>
      <p>
        The workers Flowfile can reach are server-wide and managed by administrators. Your flows use
        the worker service automatically.
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useAuthStore } from "../../stores/auth-store";
import { NO_AUTOFILL } from "../../utils/noAutofill";
import BackendHandshakeStatus from "../../components/settings/BackendHandshakeStatus.vue";
import WorkerPoolCard from "../../components/settings/WorkerPoolCard.vue";
import ExchangeStoreCard from "./ExchangeStoreCard.vue";
import {
  backendInputFrom,
  emptyBackendForm,
  plainHttpOffLoopback,
  serverFilesLabel,
  useComputeBackends,
} from "./useComputeBackends";

const isAdmin = computed(() => useAuthStore().isAdmin);

const { backends, loading, loadError, adding, busy, verdicts, load, add, remove, handshake } =
  useComputeBackends();

const form = reactive(emptyBackendForm());
const canAdd = computed(() => form.name !== "" && form.url !== "" && form.token !== "");
const plainHttp = computed(() => plainHttpOffLoopback(form.url));

const onAdd = async () => {
  if (!canAdd.value || adding.value) return;
  try {
    const row = await add(backendInputFrom(form));
    Object.assign(form, emptyBackendForm());
    ElMessage.success(`Added ${row.id}`);
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

const onRemove = async (id: string) => {
  try {
    await ElMessageBox.confirm(
      `Remove ${id}? Its token, CA certificate and "Sees this server's files" setting are not ` +
        "kept; adding it again needs them, so tick the box again if it was ticked.",
      "Remove worker",
      { confirmButtonText: "Remove", cancelButtonText: "Keep", type: "warning" },
    );
  } catch {
    return;
  }
  try {
    await remove(id);
    ElMessage.success(`Removed ${id}`);
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

const onHandshake = async (id: string) => {
  try {
    await handshake(id);
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

watch(
  isAdmin,
  (allowed) => {
    if (allowed) void load();
  },
  { immediate: true },
);
</script>

<style scoped src="./computeSettings.css"></style>
<style scoped>
.backends-container {
  container: compute-backends / inline-size;
  width: 100%;
  max-width: 1180px;
  margin: 0 auto;
  padding: var(--spacing-6);
  display: grid;
  gap: var(--spacing-5);
}
.backends-heading {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}
.backends-heading__icon {
  display: grid;
  place-items: center;
  flex-shrink: 0;
  width: 44px;
  height: 44px;
  border-radius: var(--border-radius-lg);
  background: var(--color-accent-subtle);
  color: var(--color-accent-dark);
  font-size: 20px;
}
.backends-heading .page-title {
  margin: 0 0 var(--spacing-1);
}
.backends-heading .page-description {
  margin: 0;
}
/* The shared card CSS stretches number inputs for forms; the pool's size stepper stays compact. */
.default-worker :deep(.pool-size-row .el-input-number) {
  width: 120px;
}
.default-worker :deep(.pool-size-row .el-input-number .el-input__inner) {
  text-align: center;
}
.backends-count {
  flex-shrink: 0;
  padding: var(--spacing-1) var(--spacing-2);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  background: var(--color-background-primary);
}
.backends-banner {
  display: flex;
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  border-radius: var(--border-radius-md);
  background: var(--color-danger-light);
  color: var(--color-danger-dark);
  margin-bottom: var(--spacing-3);
}
.backends-loading {
  color: var(--color-text-secondary);
  margin: 0;
}
.backends-list {
  display: grid;
  gap: var(--spacing-4);
  list-style: none;
  margin: 0;
  padding: 0;
}
.backend-worker {
  min-width: 0;
}
.backend-worker + .backend-worker {
  padding-top: var(--spacing-4);
  border-top: 1px solid var(--color-border-primary);
}
.backend-worker__header {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: var(--spacing-4);
  margin-bottom: var(--spacing-4);
}
.backend-worker__identity {
  flex: 1;
  min-width: min(100%, 220px);
}
.backend-worker__name {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  overflow-wrap: anywhere;
}
.backends-url {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  overflow-wrap: anywhere;
}
.backend-worker__files {
  display: grid;
  gap: var(--spacing-1);
  font-size: var(--font-size-sm);
}
.backend-worker__files span {
  color: var(--color-text-tertiary);
}
.backend-worker__files strong {
  color: var(--color-text-primary);
  font-weight: var(--font-weight-medium);
}
.backends-actions {
  display: flex;
  gap: var(--spacing-2);
}
.backends-actions .el-button + .el-button {
  margin-left: 0;
}
.backends-empty {
  display: grid;
  justify-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-5);
  text-align: center;
  color: var(--color-text-secondary);
}
.backends-empty > i {
  font-size: 24px;
  color: var(--color-accent-dark);
}
.backends-empty__title {
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}
.backends-empty p {
  margin: 0;
  font-size: var(--font-size-sm);
}
.backends-add__fields {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1.3fr) minmax(0, 1fr);
  gap: var(--spacing-4);
  align-items: start;
}
.backends-options {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-5);
  margin-top: var(--spacing-5);
  padding-top: var(--spacing-4);
  border-top: 1px solid var(--color-border-light);
}
.backends-option,
.backends-certificate {
  min-width: 0;
}
.backends-option > p {
  margin: var(--spacing-1) 0;
}
.backends-option .el-checkbox {
  height: auto;
  white-space: normal;
}
.backends-certificate summary {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  list-style: none;
}
.backends-certificate summary::-webkit-details-marker {
  display: none;
}
.backends-certificate summary::before {
  content: "›";
  font-size: 18px;
  line-height: 1;
  transition: transform 0.15s;
}
.backends-certificate[open] summary::before {
  transform: rotate(90deg);
}
.backends-certificate .compute-field {
  margin-top: var(--spacing-3);
}
.backends-optional {
  margin-left: auto;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  font-weight: normal;
}
.backends-field__warning {
  color: var(--color-warning-dark);
}
.backends-locked {
  text-align: center;
  padding: var(--spacing-8);
  color: var(--color-text-secondary);
}
.backends-locked i {
  font-size: 2em;
  opacity: 0.4;
}
@media (max-width: 760px) {
  .backends-container {
    padding: var(--spacing-4);
  }
}
@container compute-backends (max-width: 700px) {
  .backends-add__fields,
  .backends-options {
    grid-template-columns: 1fr;
  }
  .backends-heading {
    align-items: flex-start;
  }
}
</style>
