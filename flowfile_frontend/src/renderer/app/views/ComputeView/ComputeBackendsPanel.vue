<template>
  <div class="backends-container">
    <div class="mb-3">
      <h2 class="page-title">Compute backends</h2>
      <p class="page-description">
        Workers and Polars clusters Flowfile can reach besides its built-in worker service, on this
        host or another. A handshake checks a backend's versions against this server's, and which
        plugins and Python functions it runs.
      </p>
    </div>

    <div v-if="isAdmin" class="backends-card">
      <div v-if="loadError" class="backends-banner">
        <i class="fa-solid fa-circle-exclamation"></i>
        <span>{{ loadError }}</span>
      </div>

      <h3 class="backends-card-title">Configured backends</h3>
      <p class="backends-card-description">
        Each worker has its own URL and bearer token, and, when its TLS certificate is not public,
        the CA certificate to trust; its id is <code>worker:&lt;name&gt;</code>. A Polars cluster
        you run yourself is reached at its scheduler's address; its id is
        <code>polars-cloud:&lt;name&gt;</code>. The built-in backends, <code>local</code> and
        <code>worker</code>, are not listed. To change a backend, remove it and add it again.
      </p>

      <p v-if="loading && !backends.length" class="backends-loading">Loading backends…</p>

      <table v-if="backends.length" class="backends-table">
        <thead>
          <tr>
            <th>Id</th>
            <th>Kind</th>
            <th>URL</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <template v-for="backend in backends" :key="backend.id">
            <tr>
              <td>
                <code>{{ backend.id }}</code>
              </td>
              <td>{{ KIND_LABELS[backend.kind] ?? backend.kind }}</td>
              <td class="backends-url">{{ backend.url }}</td>
              <td class="backends-actions">
                <el-button
                  size="small"
                  :loading="busy[backend.id]"
                  @click="onHandshake(backend.id)"
                >
                  Handshake
                </el-button>
                <el-button
                  size="small"
                  type="danger"
                  plain
                  :loading="busy[backend.id]"
                  @click="onRemove(backend.id)"
                >
                  Remove
                </el-button>
              </td>
            </tr>
            <tr v-if="verdicts[backend.id]" class="backends-verdict-row">
              <td colspan="4">
                <div
                  v-if="verdicts[backend.id].ok && verdicts[backend.id].backend"
                  class="backends-verdict backends-verdict--ok"
                >
                  <i class="fa-solid fa-circle-check"></i>
                  <div>
                    <div>{{ matchLine(verdicts[backend.id], backend.kind) }}</div>
                    <div
                      v-for="difference in verdicts[backend.id].differences"
                      :key="difference"
                      class="backends-difference"
                    >
                      {{ difference }}
                    </div>
                    <div
                      v-for="(reason, name) in verdicts[backend.id].absent"
                      :key="name"
                      class="backends-difference"
                    >
                      offered without: {{ absentLabel(String(name)) }} — {{ reason }}
                    </div>
                  </div>
                </div>
                <div v-else class="backends-verdict backends-verdict--error">
                  <i class="fa-solid fa-circle-xmark"></i>
                  <span v-if="verdicts[backend.id].error">{{ verdicts[backend.id].error }}</span>
                  <div v-else>
                    <div>Does not match this server:</div>
                    <div
                      v-for="difference in verdicts[backend.id].differences"
                      :key="difference"
                      class="backends-difference"
                    >
                      {{ difference }}
                    </div>
                  </div>
                </div>
              </td>
            </tr>
          </template>
        </tbody>
      </table>

      <div v-else-if="!loading && !loadError" class="backends-empty">
        <i class="fa-solid fa-network-wired"></i>
        <div class="backends-empty__title">No backends configured</div>
        <div class="backends-empty__hint">Add a worker or a Polars cluster below.</div>
      </div>

      <div class="backends-add">
        <h4 class="backends-add__title">Add a backend</h4>
        <el-radio-group v-model="form.kind" size="small" class="backends-add__kind">
          <el-radio-button value="worker">{{ KIND_LABELS.worker }}</el-radio-button>
          <el-radio-button value="polars-cloud">{{ KIND_LABELS["polars-cloud"] }}</el-radio-button>
        </el-radio-group>
        <p v-if="form.kind === 'polars-cloud'" class="backends-add__note">
          <i class="fa-solid fa-triangle-exclamation"></i>
          A self-run Polars scheduler has no login, so keep it on a private network.
        </p>
        <div class="backends-add__fields">
          <label class="backends-field">
            <span class="backends-field__label">Name</span>
            <el-input v-model="form.name" v-bind="NO_AUTOFILL" placeholder="team">
              <template #prepend>{{ form.kind }}:</template>
            </el-input>
          </label>
          <template v-if="form.kind === 'polars-cloud'">
            <label class="backends-field">
              <span class="backends-field__label">Scheduler URL</span>
              <el-input
                v-model="form.scheduler_url"
                v-bind="NO_AUTOFILL"
                placeholder="http://10.0.0.5:5051"
              />
            </label>
            <label class="backends-field">
              <span class="backends-field__label">Dashboard URL (optional)</span>
              <el-input
                v-model="form.dashboard_url"
                v-bind="NO_AUTOFILL"
                placeholder="http://10.0.0.5:3001"
              />
            </label>
          </template>
          <template v-else>
            <label class="backends-field">
              <span class="backends-field__label">URL</span>
              <el-input v-model="form.url" v-bind="NO_AUTOFILL" placeholder="https://…" />
              <span v-if="plainHttp" class="backends-field__warning">
                <i class="fa-solid fa-triangle-exclamation"></i>
                Plain HTTP to another host: the token and every plan cross the network in clear, and
                whoever holds the token can run code on the worker. Beyond a trusted network, use
                https:// and, for a certificate that is not public, its CA certificate.
              </span>
            </label>
            <label class="backends-field">
              <span class="backends-field__label">Token</span>
              <el-input v-model="form.token" v-bind="NO_AUTOFILL" type="password" show-password />
            </label>
            <label class="backends-field">
              <span class="backends-field__label">CA certificate (PEM, optional)</span>
              <el-input
                v-model="form.ca_cert"
                v-bind="NO_AUTOFILL"
                type="textarea"
                :rows="3"
                placeholder="-----BEGIN CERTIFICATE-----"
              />
            </label>
          </template>
        </div>
        <el-button type="primary" size="small" :loading="adding" :disabled="!canAdd" @click="onAdd">
          Add
        </el-button>
      </div>
    </div>

    <div v-else class="backends-locked">
      <i class="fa-solid fa-lock"></i>
      <h3>Administrator setting</h3>
      <p>
        The backends Flowfile can reach are server-wide and managed by administrators. Your flows
        use the worker service automatically.
      </p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import type {
  ComputeBackendInput,
  ComputeBackendKind,
  HandshakeVerdict,
} from "../../api/computeBackends.api";
import { useAuthStore } from "../../stores/auth-store";
import { absentLabel } from "../../utils/executionLocation";
import { NO_AUTOFILL } from "../../utils/noAutofill";
import { plainHttpOffLoopback, useComputeBackends } from "./useComputeBackends";

const isAdmin = computed(() => useAuthStore().isAdmin);

const { backends, loading, loadError, adding, busy, verdicts, load, add, remove, handshake } =
  useComputeBackends();

const KIND_LABELS: Record<ComputeBackendKind, string> = {
  worker: "Worker",
  "polars-cloud": "Polars cluster",
};

const form = reactive({
  kind: "worker" as ComputeBackendKind,
  name: "",
  url: "",
  token: "",
  ca_cert: "",
  scheduler_url: "",
  dashboard_url: "",
});
const canAdd = computed(() =>
  form.kind === "polars-cloud"
    ? form.name !== "" && form.scheduler_url !== ""
    : form.name !== "" && form.url !== "" && form.token !== "",
);
const plainHttp = computed(() => plainHttpOffLoopback(form.url));

const matchLine = (verdict: HandshakeVerdict, kind: ComputeBackendKind): string => {
  const backend = verdict.backend!;
  const exact = !verdict.differences.length && !Object.keys(verdict.absent).length;
  const flowfile = kind === "worker" ? `Flowfile ${backend.app_version}, ` : "";
  return (
    `${exact ? "Matches this server" : "Accepted by this server"}: ${flowfile}` +
    `Polars ${backend.polars_version}, Python ${backend.python_version}, ${backend.platform}`
  );
};

const formInput = (): ComputeBackendInput =>
  form.kind === "polars-cloud"
    ? {
        kind: "polars-cloud",
        name: form.name,
        scheduler_url: form.scheduler_url,
        dashboard_url: form.dashboard_url,
      }
    : { name: form.name, url: form.url, token: form.token, ca_cert: form.ca_cert };

const onAdd = async () => {
  if (!canAdd.value) return;
  try {
    const row = await add(formInput());
    form.name = "";
    form.url = "";
    form.token = "";
    form.ca_cert = "";
    form.scheduler_url = "";
    form.dashboard_url = "";
    ElMessage.success(`Added ${row.id}`);
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

const onRemove = async (id: string) => {
  try {
    const kept = id.startsWith("worker:")
      ? " Its token and CA certificate are not kept; adding it again needs them."
      : "";
    await ElMessageBox.confirm(`Remove ${id}?${kept}`, "Remove backend", {
      confirmButtonText: "Remove",
      cancelButtonText: "Keep",
      type: "warning",
    });
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

onMounted(() => {
  if (isAdmin.value) void load();
});
</script>

<style scoped>
.backends-container {
  max-width: 1320px;
  margin: 0 auto;
  padding: var(--spacing-5);
}

.backends-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-xs);
  padding: var(--spacing-4) var(--spacing-5);
}

.backends-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  border: 1px solid var(--color-danger);
  margin-bottom: var(--spacing-3);
}

.backends-banner i {
  margin-top: 2px;
  flex-shrink: 0;
}

.backends-card-title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.backends-card-description {
  margin: 0;
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
  text-wrap: pretty;
}

.backends-loading {
  margin: var(--spacing-4) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.backends-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: var(--spacing-4);
}

.backends-table th {
  text-align: left;
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  padding: var(--spacing-1) var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.backends-table td {
  font-size: var(--font-size-md);
  color: var(--color-text-primary);
  padding: var(--spacing-1-5) var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.backends-url {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-sm);
  word-break: break-all;
}

.backends-actions {
  text-align: right;
  white-space: nowrap;
}

.backends-verdict-row td {
  padding-top: 0;
}

.backends-verdict {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.backends-verdict i {
  margin-top: 2px;
  flex-shrink: 0;
}

.backends-verdict--ok {
  background: var(--color-success-light);
  color: var(--color-success-dark);
}

.backends-verdict--error {
  background: var(--color-danger-light);
  color: var(--color-danger);
}

.backends-difference {
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  word-break: break-all;
}

.backends-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: var(--spacing-6) var(--spacing-4);
  gap: var(--spacing-1);
}

.backends-empty i {
  font-size: 20px;
  color: var(--color-text-muted);
  margin-bottom: var(--spacing-1);
}

.backends-empty__title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.backends-empty__hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  max-width: 46ch;
}

.backends-add {
  margin-top: var(--spacing-4);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
}

.backends-add__title {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.backends-add__kind {
  margin-bottom: var(--spacing-3);
}

.backends-add__note {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-xs);
  line-height: var(--line-height-normal);
  color: var(--color-warning);
}

.backends-add__note i {
  margin-top: 2px;
  flex-shrink: 0;
}

.backends-add__fields {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-3);
}

.backends-field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.backends-field__label {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.backends-field__warning {
  font-size: var(--font-size-xs);
  line-height: var(--line-height-normal);
  color: var(--color-warning);
}

.backends-locked {
  text-align: center;
  padding: var(--spacing-8) var(--spacing-4);
  color: var(--color-text-secondary);
}

.backends-locked i {
  font-size: 2em;
  opacity: 0.4;
}
</style>
