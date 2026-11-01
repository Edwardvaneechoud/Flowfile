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
        <p class="hub-dialog-success" data-testid="hub-published">
          <span class="material-icons">check_circle</span>
          {{ publishedLabel(result.version) }}
        </p>
        <p class="hub-dialog-hint">
          {{ result.name }} is on the hub as version {{ result.version }}. The flow was saved with
          that version, so your next publish builds on it.
        </p>
      </template>

      <template v-else-if="conflict">
        <p class="hub-dialog-conflict" data-testid="hub-conflict">
          <span class="material-icons">warning</span>
          {{ conflictLabel(conflict.base, conflict.current) }}
        </p>
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
          <el-select
            v-model="namespaceId"
            size="small"
            clearable
            placeholder="The hub's default namespace"
            style="width: 100%"
            data-testid="hub-publish-namespace"
          >
            <el-option
              v-for="ns in namespaces"
              :key="ns.id"
              :label="namespaceLabel(ns)"
              :value="ns.id"
            />
          </el-select>
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
        <ul v-if="warnings.length" class="hub-dialog-warnings" data-testid="hub-publish-warnings">
          <li v-for="line in warnings" :key="line">
            <span class="material-icons">warning</span>
            <span>{{ line }}</span>
          </li>
        </ul>
      </template>

      <p v-if="error" class="hub-dialog-error" data-testid="hub-publish-error">{{ error }}</p>
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
  listHubNamespaces,
  openFromHub,
  publishCheck,
  publishToHub,
  refusalOf,
  type HubFlowVersion,
  type HubNamespace,
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
const loading = ref(false);
const busy = ref(false);
const error = ref("");
const namespaces = ref<HubNamespace[]>([]);
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

const namespaceLabel = (ns: HubNamespace) => (ns.level > 0 ? `  ${ns.name}` : ns.name);

async function prepare() {
  loading.value = true;
  error.value = "";
  result.value = null;
  conflict.value = null;
  message.value = "";
  namespaceId.value = null;
  warnings.value = [];
  try {
    const [uses, hubNamespaces] = await Promise.all([
      publishCheck(props.flowId),
      props.hubOrigin ? Promise.resolve([] as HubNamespace[]) : listHubNamespaces(),
    ]);
    warnings.value = warningLines(uses);
    namespaces.value = hubNamespaces;
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
      namespaceId: props.hubOrigin ? null : namespaceId.value,
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

<style scoped src="./hubDialogs.css"></style>
