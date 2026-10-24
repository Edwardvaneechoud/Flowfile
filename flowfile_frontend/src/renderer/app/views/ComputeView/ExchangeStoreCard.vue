<template>
  <div class="exchange-card" data-testid="exchange-store-card">
    <h3 class="exchange-card__title">Exchange store</h3>
    <p class="exchange-card__description">
      Where results cross between backends: an S3 prefix every worker and cluster can reach, opened
      with a stored S3 connection. With a store, each node runs on the backend its own Compute
      setting names and its data crosses here. Without one, every node of a flow runs on the flow's
      backend.
    </p>

    <div v-if="loadError" class="exchange-card__banner">
      <i class="fa-solid fa-circle-exclamation"></i>
      <span>{{ loadError }}</span>
    </div>

    <p v-if="loading && !state" class="exchange-card__loading">Loading…</p>

    <p v-if="state" class="exchange-card__status" data-testid="exchange-store-status">
      <template v-if="state.source === 'none'">
        <i class="fa-regular fa-circle"></i>
        Not configured: a node's own Compute backend is ignored.
      </template>
      <template v-else>
        <i class="fa-solid fa-circle-check"></i>
        Results cross through <code>{{ state.uri }}</code> with the connection
        <code>{{ state.connection }}</code>
        <span v-if="state.maxRows !== null">, at most {{ state.maxRows }} rows per input</span>
        <span v-if="state.source === 'env'"> (from the environment)</span>.
      </template>
    </p>

    <p v-if="state?.envOverride" class="exchange-card__warning" data-testid="exchange-store-env">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <span>
        <code>FLOWFILE_EXCHANGE_URI</code> is set in the environment and overrides what is saved
        here. Unset it to manage the store from this page.
      </span>
    </p>

    <div v-if="state" class="exchange-card__fields">
      <label class="exchange-field">
        <span class="exchange-field__label">S3 prefix</span>
        <el-input
          v-model="form.uri"
          v-bind="NO_AUTOFILL"
          placeholder="s3://bucket/flowfile"
          data-testid="exchange-store-uri"
        />
      </label>
      <label class="exchange-field">
        <span class="exchange-field__label">Connection</span>
        <el-select
          v-model="form.connection"
          filterable
          clearable
          placeholder="A stored S3 connection"
          data-testid="exchange-store-connection"
        >
          <el-option v-for="name in state.connections" :key="name" :label="name" :value="name" />
        </el-select>
        <span v-if="!state.connections.length" class="exchange-field__hint">
          No S3 connection yet. Add one under Settings → Connections; every worker and cluster opens
          the store with it, whoever runs the flow.
        </span>
      </label>
      <label class="exchange-field">
        <span class="exchange-field__label">Row limit (optional)</span>
        <el-input-number
          v-model="form.maxRows"
          :min="0"
          :controls="false"
          placeholder="no limit"
          data-testid="exchange-store-max-rows"
        />
        <span class="exchange-field__hint">
          The most rows an input may cross with; checked before the run from the counts core already
          has.
        </span>
      </label>
    </div>

    <div v-if="state" class="exchange-card__actions">
      <el-button
        type="primary"
        size="small"
        :loading="saving"
        :disabled="!canSave"
        data-testid="exchange-store-save"
        @click="onSave"
      >
        Save
      </el-button>
      <el-button
        v-if="state.saved.uri"
        size="small"
        plain
        :loading="saving"
        data-testid="exchange-store-clear"
        @click="onClear"
      >
        Clear
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, watch } from "vue";
import { ElMessage } from "element-plus";
import { NO_AUTOFILL } from "../../utils/noAutofill";
import { useExchangeStore } from "./useExchangeStore";

const { state, loading, loadError, saving, load, save, clear } = useExchangeStore();

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

const canSave = computed(() => form.uri.trim() !== "" && form.connection !== "");

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

<style scoped>
.exchange-card__title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.exchange-card__description {
  margin: 0;
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
  text-wrap: pretty;
}

.exchange-card__banner,
.exchange-card__warning {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: var(--spacing-3) 0 0;
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
}

.exchange-card__banner {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  border: 1px solid var(--color-danger);
}

.exchange-card__warning {
  color: var(--color-warning);
}

.exchange-card__banner i,
.exchange-card__warning i,
.exchange-card__status i {
  margin-top: 2px;
  flex-shrink: 0;
}

.exchange-card__loading {
  margin: var(--spacing-4) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.exchange-card__status {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: var(--spacing-3) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.exchange-card__status code {
  word-break: break-all;
}

.exchange-card__fields {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: var(--spacing-3);
  margin-top: var(--spacing-4);
}

.exchange-field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1);
}

.exchange-field__label {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.exchange-field__hint {
  font-size: var(--font-size-xs);
  line-height: var(--line-height-normal);
  color: var(--color-text-tertiary);
}

.exchange-card__actions {
  margin-top: var(--spacing-3);
  display: flex;
  gap: var(--spacing-2);
}
</style>
