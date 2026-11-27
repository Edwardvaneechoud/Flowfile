<template>
  <div v-if="visible" class="modal-overlay" @click.self="$emit('close')">
    <div class="modal-container external-modal" data-testid="external-catalog-modal">
      <div class="modal-header">
        <h3 class="modal-title">
          {{ editing ? `Edit ${catalog?.name}` : "Add a catalog from elsewhere" }}
        </h3>
      </div>
      <div class="modal-content" @input="onEdited" @change="onEdited">
        <p class="intro">
          An Iceberg REST catalog your company already runs, such as Apache Polaris, Snowflake Open
          Catalog or Lakekeeper. Its schemas and tables show up here beside your own.
        </p>

        <label v-if="!editing" class="field">
          <span class="field-label">Name</span>
          <input
            v-model="form.name"
            class="input-field"
            data-testid="external-name"
            placeholder="How Flowfile names it, e.g. polaris"
          />
        </label>
        <label class="field">
          <span class="field-label">Description</span>
          <input v-model="form.description" class="input-field" placeholder="Optional" />
        </label>
        <label class="field">
          <span class="field-label">Address</span>
          <input
            v-model="form.uri"
            class="input-field mono"
            data-testid="external-uri"
            placeholder="https://…/api/catalog"
          />
        </label>
        <label class="field">
          <span class="field-label">Warehouse</span>
          <input
            v-model="form.warehouse"
            class="input-field"
            data-testid="external-warehouse"
            placeholder="The catalog's warehouse or catalog name"
          />
        </label>

        <fieldset class="field sign-in">
          <legend class="field-label">Sign in with</legend>
          <label v-for="option in signInOptions" :key="option.value" class="radio">
            <input
              v-model="form.sign_in"
              type="radio"
              name="external-sign-in"
              :value="option.value"
              :data-testid="option.testId"
            />
            {{ option.label }}
          </label>
        </fieldset>

        <label v-if="form.sign_in === 'token'" class="field">
          <span class="field-label">Token</span>
          <input
            v-model="form.token"
            type="password"
            autocomplete="off"
            class="input-field"
            data-testid="external-token"
            :placeholder="editing ? 'Leave empty to keep' : ''"
          />
          <span class="hint">A token is not renewed; a run that outlasts it fails.</span>
        </label>

        <template v-if="form.sign_in === 'client_credentials'">
          <label class="field">
            <span class="field-label">Client id</span>
            <input
              v-model="form.client_id"
              class="input-field"
              autocomplete="off"
              data-testid="external-client-id"
            />
          </label>
          <label class="field">
            <span class="field-label">Client secret</span>
            <input
              v-model="form.client_secret"
              type="password"
              autocomplete="off"
              class="input-field"
              data-testid="external-client-secret"
              :placeholder="editing ? 'Leave empty to keep' : ''"
            />
          </label>
          <button type="button" class="more" @click="showMore = !showMore">
            <i :class="showMore ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'"></i>
            More
          </button>
          <template v-if="showMore || form.scope || form.token_endpoint">
            <label class="field">
              <span class="field-label">Scope</span>
              <input v-model="form.scope" class="input-field" data-testid="external-scope" />
              <span class="hint">Polaris and Snowflake Open Catalog: {{ POLARIS_SCOPE }}</span>
            </label>
            <label class="field">
              <span class="field-label">Token endpoint</span>
              <input
                v-model="form.token_endpoint"
                class="input-field mono"
                data-testid="external-token-endpoint"
              />
              <span class="hint">Empty: the catalog's own</span>
            </label>
          </template>
        </template>

        <p v-if="editing && signInAgain" class="hint warn">{{ SIGN_IN_AGAIN }}</p>
        <p class="hint">{{ NO_KEY_TYPED }}</p>
      </div>
      <div v-if="result || (formError && touched)" class="result-row">
        <p
          v-if="result"
          class="test-result"
          :class="result.ok ? 'ok' : 'failed'"
          data-testid="external-test-result"
        >
          <i :class="result.ok ? 'fa-solid fa-circle-check' : 'fa-solid fa-circle-xmark'"></i>
          {{ result.message }}
        </p>
        <p v-else-if="formError && touched" class="test-result pending">{{ formError }}</p>
      </div>
      <div class="modal-actions">
        <button
          class="btn btn-secondary"
          data-testid="external-test"
          :disabled="busy || !!formError"
          @click="runTest"
        >
          <i v-if="testing" class="fa-solid fa-spinner fa-spin"></i>
          Test
        </button>
        <span class="spacer"></span>
        <button class="btn btn-secondary" @click="$emit('close')">Cancel</button>
        <button
          class="btn btn-primary"
          data-testid="external-submit"
          :disabled="busy || !!formError"
          @click="submit"
        >
          <i v-if="saving" class="fa-solid fa-spinner fa-spin"></i>
          {{ editing ? "Save" : "Add catalog" }}
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { ElMessage } from "element-plus";
import { CatalogApi } from "../../api/catalog.api";
import type { ExternalCatalogTestResult, NamespaceTree } from "../../types";
import { useCatalogStore } from "../../stores/catalog-store";
import {
  NO_KEY_TYPED,
  POLARIS_SCOPE,
  SIGN_IN_AGAIN,
  emptyExternalForm,
  externalFormError,
  externalPayload,
  formFromCatalog,
  needsSignInAgain,
} from "./externalCatalog";

const props = defineProps<{ visible: boolean; catalog?: NamespaceTree | null }>();
const emit = defineEmits<{ (e: "close"): void; (e: "saved", namespace: NamespaceTree): void }>();

const catalogStore = useCatalogStore();
const form = reactive(emptyExternalForm());
const result = ref<ExternalCatalogTestResult | null>(null);
const testing = ref(false);
const saving = ref(false);
const touched = ref(false);
const showMore = ref(false);

const signInOptions = [
  { value: "none", label: "No sign-in", testId: "external-signin-none" },
  { value: "token", label: "A token", testId: "external-signin-token" },
  {
    value: "client_credentials",
    label: "A client id and secret",
    testId: "external-signin-client",
  },
] as const;

const editing = computed(() => !!props.catalog?.external);
const original = computed(() => props.catalog?.external ?? null);
const formError = computed(() => externalFormError(form, original.value));
const signInAgain = computed(() => needsSignInAgain(form, original.value));
const busy = computed(() => testing.value || saving.value);

watch(
  () => props.visible,
  (open) => {
    if (!open) return;
    Object.assign(
      form,
      props.catalog?.external ? formFromCatalog(props.catalog) : emptyExternalForm(),
    );
    result.value = null;
    touched.value = false;
    showMore.value = false;
  },
);

function onEdited() {
  touched.value = true;
  result.value = null;
}

/** Core's sentence: a string detail, or the messages of a refused body without pydantic's prefix. */
function said(e: any, fallback: string): string {
  const detail = e?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail) && detail.length)
    return detail.map((item) => String(item.msg ?? "").replace(/^Value error, /, "")).join(" ");
  return fallback;
}

async function runTest() {
  if (formError.value) return;
  testing.value = true;
  try {
    const body = externalPayload(form, editing.value);
    delete body.name;
    delete body.description;
    const namespaceId = editing.value ? props.catalog?.id : undefined;
    result.value = await CatalogApi.testExternalCatalog(
      namespaceId !== undefined ? { ...body, namespace_id: namespaceId } : body,
    );
  } catch (e: any) {
    result.value = { ok: false, message: said(e, "The test could not be run."), schemas: null };
  } finally {
    testing.value = false;
  }
}

async function submit() {
  if (formError.value) return;
  saving.value = true;
  try {
    const body = externalPayload(form, editing.value);
    const saved =
      editing.value && props.catalog
        ? await CatalogApi.updateExternalCatalog(props.catalog.id, body)
        : await CatalogApi.createExternalCatalog(body);
    await Promise.all([catalogStore.loadTree(), catalogStore.loadStats()]);
    ElMessage.success(editing.value ? `Saved ${saved.name}` : `Added ${saved.name}`);
    emit("saved", saved);
    emit("close");
  } catch (e: any) {
    result.value = {
      ok: false,
      message: said(e, "The catalog could not be saved."),
      schemas: null,
    };
  } finally {
    saving.value = false;
  }
}
</script>

<style scoped>
.external-modal {
  width: 520px;
  max-width: calc(100vw - 32px);
}

.intro {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.field {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin: 0 0 var(--spacing-3);
  border: none;
  padding: 0;
}

.field-label {
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.input-field {
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  background: var(--color-background-primary);
  color: var(--color-text-primary);
  font-size: var(--font-size-sm);
  box-sizing: border-box;
}

.input-field:focus {
  outline: none;
  border-color: var(--color-primary);
}

.mono {
  font-family: var(--font-family-mono);
}

.sign-in {
  flex-direction: row;
  flex-wrap: wrap;
  gap: var(--spacing-3);
}

.sign-in legend {
  width: 100%;
  margin-bottom: 4px;
}

.radio {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  cursor: pointer;
}

.more {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin: 0 0 var(--spacing-3);
  padding: 0;
  border: none;
  background: none;
  font-size: var(--font-size-xs);
  color: var(--color-text-secondary);
  cursor: pointer;
}

.hint {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.hint.warn {
  color: var(--el-color-warning, #e6a23c);
}

.test-result {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: var(--spacing-2) 0 0;
  padding: var(--spacing-2) var(--spacing-3);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.test-result.ok {
  color: var(--el-color-success, #67c23a);
  background: color-mix(in srgb, var(--el-color-success, #67c23a) 10%, transparent);
}

.test-result.pending {
  color: var(--color-text-secondary);
  background: var(--color-background-tertiary);
}

.test-result.failed {
  color: var(--el-color-danger, #f56c6c);
  background: color-mix(in srgb, var(--el-color-danger, #f56c6c) 10%, transparent);
}

.result-row {
  padding: 0 var(--spacing-4);
}

.modal-actions .spacer {
  flex: 1;
}
</style>
