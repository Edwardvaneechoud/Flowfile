<template>
  <ComputeCard
    card-id="exchange-store"
    title="Exchange store"
    icon="fa-solid fa-arrow-right-arrow-left"
    class="exchange-card"
    data-testid="exchange-store-card"
  >
    <template #description>
      Shared S3 storage for passing results between backends. Without it, each flow runs on a single
      backend.
    </template>
    <template #meta="{ open }">
      <span
        v-if="!open && statusSummary"
        :class="['compute-chip', `compute-chip--${statusSummary.tone}`]"
        data-testid="exchange-store-summary"
        >{{ statusSummary.label }}</span
      >
    </template>
    <div v-if="loadError" class="exchange-card__banner">
      <i class="fa-solid fa-circle-exclamation"></i>
      <span>{{ loadError }}</span>
    </div>

    <p v-if="loading && !state" class="exchange-card__loading">Loading…</p>

    <div
      v-if="state"
      class="exchange-card__status"
      :class="`exchange-card__status--${statusTone}`"
      data-testid="exchange-store-status"
    >
      <i
        :class="
          state.source === 'none'
            ? 'fa-regular fa-circle'
            : statusTone === 'warning'
              ? 'fa-solid fa-triangle-exclamation'
              : 'fa-solid fa-circle-check'
        "
        aria-hidden="true"
      ></i>
      <div>
        <template v-if="state.source === 'none'">
          <strong>Not configured</strong>
          <p>A node's own Compute backend is ignored until an exchange store is configured.</p>
        </template>
        <template v-else-if="!state.connection">
          <strong>Connection required</strong>
          <p>
            <code>{{ state.uri }}</code> has no connection, so every crossing between backends is
            refused.
            <template v-if="state.source === 'env'"
              >Set <code>FLOWFILE_EXCHANGE_CONNECTION</code> to a stored S3 connection's
              name.</template
            >
          </p>
        </template>
        <template v-else>
          <strong>{{
            state.maxRowsError ? "Row limit needs attention" : "Exchange store configured"
          }}</strong>
          <p>
            Results cross through <code>{{ state.uri }}</code> with the connection
            <code>{{ state.connection }}</code
            ><template v-if="state.maxRows !== null"
              >, at most {{ state.maxRows }} rows per input</template
            ><template v-if="state.source === 'env'"> (from the environment)</template>.
          </p>
        </template>
      </div>
    </div>

    <p v-if="state?.envOverride" class="exchange-card__warning" data-testid="exchange-store-env">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <span>
        <code>FLOWFILE_EXCHANGE_URI</code> is set in the environment and overrides what is saved
        here. Unset it to manage the store from this page.
      </span>
    </p>

    <div v-if="state" class="exchange-card__fields">
      <label class="compute-field">
        <span class="compute-field__label">S3 prefix</span>
        <el-input
          v-model="form.uri"
          v-bind="NO_AUTOFILL"
          placeholder="s3://bucket/flowfile"
          data-testid="exchange-store-uri"
        />
      </label>
      <label class="compute-field">
        <span class="compute-field__label">Connection</span>
        <el-select
          v-model="form.connection"
          filterable
          clearable
          value-on-clear=""
          placeholder="A stored S3 connection"
          data-testid="exchange-store-connection"
        >
          <el-option v-for="name in state.connections" :key="name" :label="name" :value="name" />
        </el-select>
        <span v-if="!state.connections.length" class="compute-field__hint">
          No S3 connection yet. Add one under Settings → Connections; every worker and cluster opens
          the store with it, whoever runs the flow.
        </span>
      </label>
      <label class="compute-field">
        <span class="compute-field__label">Row limit (optional)</span>
        <el-input-number
          v-model="form.maxRows"
          :min="0"
          :controls="false"
          placeholder="no limit"
          data-testid="exchange-store-max-rows"
        />
        <span class="compute-field__hint">
          Maximum rows per input, checked before the run. Leave empty for no limit.
        </span>
        <span
          v-if="state.maxRowsError"
          class="exchange-field__error"
          data-testid="exchange-store-max-rows-error"
        >
          <i class="fa-solid fa-triangle-exclamation"></i>
          {{ state.maxRowsError }}; every crossing between backends is refused until it is fixed.
        </span>
      </label>
    </div>
    <template v-if="state" #footer>
      <el-button
        type="primary"
        size="small"
        :loading="saving"
        :disabled="!canSave"
        data-testid="exchange-store-save"
        @click="onSave"
      >
        Save exchange store
      </el-button>
      <el-button
        v-if="state.saved.uri"
        size="small"
        plain
        :loading="saving"
        data-testid="exchange-store-clear"
        @click="onClear"
      >
        Clear store
      </el-button>
    </template>
  </ComputeCard>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, watch } from "vue";
import { ElMessage } from "element-plus";
import { NO_AUTOFILL } from "../../utils/noAutofill";
import ComputeCard from "./ComputeCard.vue";
import { useExchangeStore } from "./useExchangeStore";
import type { StatusSummary } from "../../components/settings/handshakeStatus";

const { state, loading, loadError, saving, load, save, clear } = useExchangeStore();

const statusTone = computed(() => {
  if (!state.value || state.value.source === "none") return "neutral";
  return !state.value.connection || state.value.maxRowsError ? "warning" : "success";
});

const statusSummary = computed<StatusSummary | null>(() => {
  if (!state.value) return null;
  if (state.value.source === "none") return { tone: "neutral", label: "Not configured" };
  if (!state.value.connection) return { tone: "warning", label: "Connection required" };
  if (state.value.maxRowsError) return { tone: "warning", label: "Row limit needs attention" };
  return { tone: "success", label: "Configured" };
});

const form = reactive<{ uri: string; connection: string; maxRows: number | undefined }>({
  uri: "",
  connection: "",
  maxRows: undefined,
});

watch(state, (next) => {
  if (!next) return;
  form.uri = next.saved.uri ?? "";
  form.connection = next.saved.connection ?? "";
  form.maxRows = next.saved.maxRows ?? undefined;
});

const canSave = computed(() => form.uri.trim() !== "" && !!form.connection);

const onSave = async () => {
  if (!canSave.value) return;
  try {
    const next = await save({
      uri: form.uri.trim(),
      connection: form.connection,
      maxRows: form.maxRows ?? null,
    });
    ElMessage.success(
      next.envOverride
        ? "Saved; the environment still pins the store until FLOWFILE_EXCHANGE_URI is unset."
        : "Exchange store saved; the next run uses it.",
    );
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

const onClear = async () => {
  try {
    await clear();
    ElMessage.success("Exchange store cleared.");
  } catch (error) {
    ElMessage.error((error as Error).message);
  }
};

onMounted(() => {
  void load();
});
</script>

<style scoped src="./computeSettings.css"></style>
<style scoped>
.exchange-card__banner,
.exchange-card__warning {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
}
.exchange-card__banner {
  background: var(--color-danger-light);
  color: var(--color-danger-dark);
}
.exchange-card__warning {
  margin: var(--spacing-3) 0 0;
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
}
.exchange-card__banner i,
.exchange-card__warning i,
.exchange-card__status > i {
  margin-top: 3px;
  flex-shrink: 0;
}
.exchange-card__loading {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}
.exchange-card__status {
  display: flex;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  font-size: var(--font-size-sm);
}
.exchange-card__status > div {
  min-width: 0;
}
.exchange-card__status strong {
  font-weight: var(--font-weight-medium);
}
.exchange-card__status p {
  color: var(--color-text-secondary);
  margin: var(--spacing-1) 0 0;
  line-height: var(--line-height-normal);
  overflow-wrap: anywhere;
}
.exchange-card__status--neutral {
  color: var(--color-text-secondary);
}
.exchange-card__status--success {
  color: var(--color-success-dark);
  border-left: 3px solid var(--color-success);
}
.exchange-card__status--warning {
  color: var(--color-warning-dark);
  border-left: 3px solid var(--color-warning);
}
.exchange-card__fields {
  display: grid;
  grid-template-columns: minmax(0, 1.4fr) minmax(0, 1fr) minmax(140px, 0.6fr);
  align-items: start;
  gap: var(--spacing-4);
  margin-top: var(--spacing-4);
}
.exchange-field__error {
  font-size: var(--font-size-xs);
  line-height: var(--line-height-normal);
  color: var(--color-warning-dark);
}
.compute-card__footer .el-button + .el-button {
  margin-left: 0;
}
@container compute-backends (max-width: 700px) {
  .exchange-card__fields {
    grid-template-columns: 1fr;
  }
}
</style>
