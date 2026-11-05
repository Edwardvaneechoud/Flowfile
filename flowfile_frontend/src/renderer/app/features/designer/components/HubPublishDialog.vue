<template>
  <el-dialog
    v-model="isVisible"
    title="Publish to hub"
    width="min(560px, calc(100vw - 32px))"
    :close-on-click-modal="false"
    custom-class="high-z-index-dialog"
  >
    <div class="hub-dialog-body">
      <p v-if="loading" class="hub-dialog-status">Loading…</p>

      <template v-else-if="result">
        <el-alert
          type="success"
          :closable="false"
          show-icon
          :title="publishedLabel(result.version)"
          data-testid="hub-published"
        />
        <p class="hub-dialog-hint">
          {{ result.name }} is on the hub as version {{ result.version }}. The flow was saved with
          that version, so your next publish builds on it.
        </p>
      </template>

      <template v-else-if="conflict">
        <el-alert
          type="warning"
          :closable="false"
          show-icon
          :title="conflictLabel(conflict.base, conflict.current)"
          data-testid="hub-conflict"
        />
        <div class="hub-version-compare">
          <div class="hub-version-card">
            <h4>Your base</h4>
            <dl>
              <template v-for="fact in versionFacts(conflict.base)" :key="fact.label">
                <dt>{{ fact.label }}</dt>
                <dd>{{ fact.value }}</dd>
              </template>
            </dl>
          </div>
          <div class="hub-version-card">
            <h4>On the hub now</h4>
            <dl>
              <template v-for="fact in versionFacts(conflict.current)" :key="fact.label">
                <dt>{{ fact.label }}</dt>
                <dd>{{ fact.value }}</dd>
              </template>
            </dl>
          </div>
        </div>
        <p class="hub-dialog-hint">
          Publishing over keeps the other version in the hub's history. Opening it instead replaces
          your local copy of this flow.
        </p>
      </template>

      <template v-else>
        <div v-if="!hubOrigin" class="form-group">
          <label>Namespace</label>
          <catalog-namespace-picker
            ref="picker"
            v-model="namespaceId"
            hub
            catalogs-selectable
            data-testid="hub-publish-namespace"
          />
          <p v-if="namespaceId === null" class="hub-dialog-hint">
            With none picked, the flow goes to the hub's default namespace.
          </p>
          <el-button
            v-else
            link
            type="primary"
            size="small"
            class="hub-dialog-clear"
            data-testid="hub-publish-default-namespace"
            @click="namespaceId = null"
          >
            Use the hub's default namespace instead
          </el-button>
        </div>
        <p v-else class="hub-dialog-hint">
          This flow is on the hub as version {{ hubOrigin.version }}; this publish becomes the next
          one.
        </p>
        <div class="form-group">
          <label>Message</label>
          <el-input
            v-model="message"
            type="textarea"
            :rows="2"
            size="small"
            placeholder="What changed (optional)"
            data-testid="hub-publish-message"
          />
        </div>
        <el-alert
          v-if="warnings.length"
          type="warning"
          :closable="false"
          show-icon
          data-testid="hub-publish-warnings"
        >
          <p v-for="line in warnings" :key="line" class="hub-dialog-warning">{{ line }}</p>
        </el-alert>
      </template>

      <el-alert
        v-if="error"
        type="error"
        :closable="false"
        show-icon
        :title="error"
        data-testid="hub-publish-error"
      />
    </div>

    <template #footer>
      <div class="dialog-footer">
        <template v-if="result">
          <el-button type="primary" size="small" @click="isVisible = false">Done</el-button>
        </template>
        <template v-else-if="conflict">
          <el-button size="small" :disabled="busy" @click="isVisible = false">Cancel</el-button>
          <el-button
            v-if="conflict.current"
            size="small"
            :loading="busy"
            data-testid="hub-open-instead"
            @click="openInstead"
          >
            {{ openInsteadLabel(conflict.current) }}
          </el-button>
          <el-button
            v-if="conflict.current"
            type="primary"
            size="small"
            :loading="busy"
            data-testid="hub-publish-over"
            @click="publish(conflict.current.version)"
          >
            {{ publishOverLabel(conflict.current) }}
          </el-button>
        </template>
        <template v-else>
          <el-button size="small" :disabled="busy" @click="isVisible = false">Cancel</el-button>
          <el-button
            type="primary"
            size="small"
            :loading="busy"
            :disabled="loading"
            data-testid="hub-publish"
            @click="publish()"
          >
            Publish
          </el-button>
        </template>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { ElMessageBox } from "element-plus";
import {
  openFromHub,
  publishCheck,
  publishToHub,
  refusalOf,
  type HubFlowVersion,
  type HubPublishResult,
} from "../../../api/hubFlows.api";
import type { HubOrigin } from "../../../types";
import { detailOf } from "../../../utils/apiDetail";
import {
  conflictLabel,
  openInsteadLabel,
  publishOverLabel,
  publishedLabel,
  refusalMessage,
  versionFacts,
  warningLines,
} from "../hubFlows";
import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";

const props = defineProps<{
  visible: boolean;
  flowId: number;
  hubOrigin: HubOrigin | null;
}>();

const emit = defineEmits<{
  (e: "update:visible", value: boolean): void;
  (e: "published", result: HubPublishResult): void;
  (e: "open-flow", payload: { message: string; flowPath: string }): void;
}>();

interface Conflict {
  flowUuid: string;
  base: HubFlowVersion | null;
  current: HubFlowVersion | null;
}

const isVisible = ref(props.visible);
const picker = ref<InstanceType<typeof CatalogNamespacePicker> | null>(null);
const loading = ref(false);
const busy = ref(false);
const error = ref("");
const namespaceId = ref<number | null>(null);
const message = ref("");
const warnings = ref<string[]>([]);
const result = ref<HubPublishResult | null>(null);
const conflict = ref<Conflict | null>(null);

watch(
  () => props.visible,
  (v) => {
    isVisible.value = v;
    if (v) void prepare();
  },
);

watch(isVisible, (v) => {
  if (v !== props.visible) emit("update:visible", v);
});

async function prepare() {
  loading.value = true;
  error.value = "";
  result.value = null;
  conflict.value = null;
  message.value = "";
  namespaceId.value = null;
  warnings.value = [];
  try {
    warnings.value = warningLines(await publishCheck(props.flowId));
  } catch (err) {
    error.value = detailOf(err, "Couldn't reach the hub.");
  } finally {
    loading.value = false;
  }
}

async function publish(overwriteVersion?: number) {
  busy.value = true;
  error.value = "";
  try {
    const published = await publishToHub({
      flowId: props.flowId,
      namespaceId: props.hubOrigin ? null : picker.value?.hubIdOf(namespaceId.value),
      message: message.value.trim() || undefined,
      overwriteVersion,
    });
    conflict.value = null;
    result.value = published;
    emit("published", published);
  } catch (err) {
    const refusal = refusalOf(err);
    if (refusal?.kind === "stale_base") {
      conflict.value = { flowUuid: refusal.flowUuid, base: refusal.base, current: refusal.current };
    } else {
      error.value = refusal ? refusalMessage(refusal) : detailOf(err, "Couldn't publish the flow.");
    }
  } finally {
    busy.value = false;
  }
}

async function openInstead() {
  if (!conflict.value?.current) return;
  const current = conflict.value.current;
  try {
    await ElMessageBox.confirm(
      `Opening v${current.version} replaces your local copy of this flow. Edits you have not published are lost.`,
      openInsteadLabel(current),
      { type: "warning", confirmButtonText: openInsteadLabel(current), cancelButtonText: "Cancel" },
    );
  } catch {
    return;
  }
  busy.value = true;
  error.value = "";
  try {
    const opened = await openFromHub({
      flowUuid: conflict.value.flowUuid,
      version: current.version,
      replace: true,
    });
    emit("open-flow", { message: "Flow opened from hub", flowPath: opened.flowPath });
    isVisible.value = false;
  } catch (err) {
    const refusal = refusalOf(err);
    error.value = refusal ? refusalMessage(refusal) : detailOf(err, "Couldn't open the flow.");
  } finally {
    busy.value = false;
  }
}
</script>

<style scoped>
.hub-dialog-body {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-1);
  font-family: var(--font-family-base);
}

.form-group label {
  display: block;
  margin-bottom: var(--spacing-2);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.hub-dialog-status,
.hub-dialog-hint {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  line-height: var(--line-height-normal);
}

.form-group .hub-dialog-hint,
.form-group .hub-dialog-clear {
  margin-top: var(--spacing-2);
}

.hub-dialog-warning {
  margin: 0;
  overflow-wrap: anywhere;
}

.hub-version-compare {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-3);
}

.hub-version-card {
  padding: var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
}

.hub-version-card h4 {
  margin: 0 0 var(--spacing-2);
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-semibold, 600);
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary);
}

.hub-version-card dl {
  display: grid;
  grid-template-columns: auto 1fr;
  gap: var(--spacing-1) var(--spacing-3);
  margin: 0;
  font-size: var(--font-size-sm);
}

.hub-version-card dt {
  color: var(--color-text-secondary);
}

.hub-version-card dd {
  margin: 0;
  overflow-wrap: anywhere;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: var(--spacing-2);
}
</style>
