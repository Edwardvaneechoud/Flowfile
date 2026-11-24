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
        <p v-if="publishedAsNew" class="hub-dialog-hint" data-testid="hub-published-as-new">
          {{ publishedAsNewHint(result.name, result.version) }}
        </p>
        <p v-else class="hub-dialog-hint">
          {{ result.name }} is on the hub as version {{ result.version }}. The flow was saved with
          that version, so your next publish builds on it.
        </p>
      </template>

      <template v-else-if="merged">
        <el-alert
          :type="merged.conflicts.length ? 'warning' : 'success'"
          :closable="false"
          show-icon
          :title="settledSome && !merged.conflicts.length ? SETTLED : mergedLabel(merged)"
          data-testid="hub-merged"
        />
        <ul v-if="merged.changes.length" class="hub-merge-list" data-testid="hub-merged-changes">
          <li v-for="(change, i) in merged.changes" :key="i">
            {{ change.kind }} {{ change.label }}
          </li>
        </ul>
        <div v-if="openConflicts.length" class="hub-settle" data-testid="hub-merged-conflicts">
          <merge-conflict-item
            v-for="item in openConflicts"
            :key="item.key"
            :item="item"
            :item-key="item.key"
            testid="hub-settle"
            :busy="busy"
            theirs-first
            @take="settle(item.key, $event)"
          />
        </div>
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
          Merging brings their changes into your copy, step by step, as one step you can undo.
          Publishing over keeps the other version in the hub's history. Opening it instead replaces
          your local copy of this flow.
        </p>
      </template>

      <template v-else>
        <template v-if="hubOrigin && !asNew">
          <p class="hub-dialog-hint">
            This flow is on the hub as version {{ hubOrigin.version }}; this publish becomes the
            next one.
          </p>
          <el-button
            link
            type="primary"
            size="small"
            class="hub-dialog-switch"
            data-testid="hub-publish-as-new"
            @click="startAsNew"
          >
            Publish as a new flow instead
          </el-button>
        </template>
        <div v-if="asNew" class="form-group">
          <label>Name</label>
          <el-input
            v-model="newName"
            size="small"
            placeholder="Name of the new flow"
            data-testid="hub-publish-new-name"
          />
        </div>
        <div v-if="!hubOrigin || asNew" class="form-group">
          <label>Schema</label>
          <catalog-namespace-picker
            ref="picker"
            v-model="namespaceId"
            hub
            data-testid="hub-publish-namespace"
          />
          <p v-if="namespaceId === null" class="hub-dialog-hint">
            With none picked, the flow goes to the hub's default schema.
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
            Use the hub's default schema instead
          </el-button>
        </div>
        <template v-if="asNew">
          <p class="hub-dialog-hint" data-testid="hub-publish-new-note">
            {{ PUBLISH_AS_NEW_NOTE }}
          </p>
          <el-button
            link
            type="primary"
            size="small"
            class="hub-dialog-switch"
            data-testid="hub-publish-next-version"
            @click="asNew = false"
          >
            Publish the next version instead
          </el-button>
        </template>
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
        <template v-else-if="merged">
          <el-button
            v-if="openConflicts.length || !merged.applied"
            size="small"
            data-testid="hub-merged-done"
            @click="isVisible = false"
          >
            Done
          </el-button>
          <el-button
            v-else
            type="primary"
            size="small"
            :loading="busy"
            data-testid="hub-publish"
            @click="publish()"
          >
            Publish
          </el-button>
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
            size="small"
            :loading="busy"
            data-testid="hub-publish-over"
            @click="publish(conflict.current.version)"
          >
            {{ publishOverLabel(conflict.current) }}
          </el-button>
          <el-button
            v-if="conflict.current"
            type="primary"
            size="small"
            :loading="busy"
            data-testid="hub-merge"
            @click="mergeNewest"
          >
            {{ mergeLabel(conflict.current) }}
          </el-button>
        </template>
        <template v-else>
          <el-button size="small" :disabled="busy" @click="isVisible = false">Cancel</el-button>
          <el-button
            type="primary"
            size="small"
            :loading="busy"
            :disabled="loading || (asNew && !newName.trim())"
            data-testid="hub-publish"
            @click="publish()"
          >
            {{ asNew ? "Publish as new flow" : "Publish" }}
          </el-button>
        </template>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import {
  listMergeConflicts,
  mergeFromHub,
  openFromHub,
  publishCheck,
  publishToHub,
  refusalOf,
  resolveMergeConflict,
  type HubMergeResult,
  type MergeConflictRow,
  type HubPublishResult,
  type HubRefusal,
} from "../../../api/hubFlows.api";
import type { HubOrigin } from "../../../types";
import { detailOf } from "../../../utils/apiDetail";
import {
  OPENED_FROM_HUB,
  PUBLISH_AS_NEW_NOTE,
  SETTLED,
  conflictLabel,
  mergeLabel,
  mergedLabel,
  newFlowName,
  openInsteadLabel,
  publishOverLabel,
  publishedAsNewHint,
  publishedLabel,
  refusalMessage,
  versionFacts,
  warningLines,
} from "../hubFlows";
import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";
import MergeConflictItem from "../../../components/merge/MergeConflictItem.vue";

const props = defineProps<{
  visible: boolean;
  flowId: number;
  hubOrigin: HubOrigin | null;
  /** The open flow's name, which a new flow's name starts from. */
  flowName?: string | null;
}>();

const emit = defineEmits<{
  (e: "update:visible", value: boolean): void;
  (e: "published", result: HubPublishResult): void;
  (e: "open-flow", payload: { message: string; flowPath: string }): void;
  (e: "merged", result: HubMergeResult): void;
}>();

const isVisible = ref(props.visible);
const picker = ref<InstanceType<typeof CatalogNamespacePicker> | null>(null);
const loading = ref(false);
const busy = ref(false);
const error = ref("");
const namespaceId = ref<number | null>(null);
const message = ref("");
const warnings = ref<string[]>([]);
const result = ref<HubPublishResult | null>(null);
const conflict = ref<Extract<HubRefusal, { kind: "stale_base" }> | null>(null);
const merged = ref<HubMergeResult | null>(null);
const openConflicts = ref<MergeConflictRow[]>([]);
const settledSome = ref(false);
const asNew = ref(false);
const newName = ref("");
const publishedAsNew = ref(false);

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
  merged.value = null;
  openConflicts.value = [];
  settledSome.value = false;
  message.value = "";
  namespaceId.value = null;
  warnings.value = [];
  asNew.value = false;
  newName.value = "";
  publishedAsNew.value = false;
  try {
    warnings.value = warningLines(await publishCheck(props.flowId));
  } catch (err) {
    error.value = detailOf(err, "Couldn't reach the hub.");
  } finally {
    loading.value = false;
  }
}

function startAsNew() {
  asNew.value = true;
  error.value = "";
  namespaceId.value = null;
  if (!newName.value.trim()) newName.value = newFlowName(props.flowName);
}

async function publish(overwriteVersion?: number) {
  busy.value = true;
  error.value = "";
  const asNewFlow = Boolean(props.hubOrigin) && asNew.value;
  try {
    const published = await publishToHub({
      flow_id: props.flowId,
      namespace_id: props.hubOrigin && !asNewFlow ? null : picker.value?.hubIdOf(namespaceId.value),
      message: message.value.trim() || undefined,
      overwrite_version: overwriteVersion,
      ...(asNewFlow ? { as_new: true, name: newName.value.trim() } : {}),
    });
    conflict.value = null;
    merged.value = null;
    publishedAsNew.value = asNewFlow;
    result.value = published;
    emit("published", published);
  } catch (err) {
    const refusal = refusalOf(err);
    if (refusal?.kind === "stale_base") {
      conflict.value = refusal;
    } else if (refusal?.kind === "unresolved_conflicts") {
      await showOpenConflicts();
    } else {
      error.value = refusal ? refusalMessage(refusal) : detailOf(err, "Couldn't publish the flow.");
    }
  } finally {
    busy.value = false;
  }
}

async function mergeNewest() {
  busy.value = true;
  error.value = "";
  try {
    const outcome = await mergeFromHub(props.flowId);
    conflict.value = null;
    merged.value = outcome;
    openConflicts.value = outcome.conflicts.length ? await listMergeConflicts(props.flowId) : [];
    emit("merged", outcome);
  } catch (err) {
    const refusal = refusalOf(err);
    error.value = refusal
      ? refusalMessage(refusal)
      : detailOf(err, "Couldn't merge the hub's version.");
  } finally {
    busy.value = false;
  }
}

async function showOpenConflicts() {
  const rows = await listMergeConflicts(props.flowId);
  openConflicts.value = rows;
  merged.value = {
    version: props.hubOrigin?.version ?? 0,
    applied: true,
    changes: [],
    conflicts: rows.map(({ key, kind, label, message }) => ({ key, kind, label, message })),
    warnings: [],
  };
}

async function settle(key: string, take: "ours" | "theirs") {
  const current = merged.value;
  if (!current) return;
  busy.value = true;
  error.value = "";
  const keep = (rows: MergeConflictRow[]): HubMergeResult => {
    openConflicts.value = rows;
    const left = new Set(rows.map((row) => row.key));
    merged.value = { ...current, conflicts: current.conflicts.filter((c) => left.has(c.key)) };
    return merged.value;
  };
  try {
    const next = keep(await resolveMergeConflict(props.flowId, key, take));
    settledSome.value = true;
    emit("merged", next);
  } catch (err) {
    error.value = detailOf(err, "Couldn't settle the conflict.");
    // Show what the flow holds now, so no row names a conflict that is gone.
    await listMergeConflicts(props.flowId).then(keep, () => undefined);
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
      flow_uuid: conflict.value.flow_uuid,
      version: current.version,
      replace: true,
    });
    emit("open-flow", { message: OPENED_FROM_HUB, flowPath: opened.flow_path });
    ElMessage.success(OPENED_FROM_HUB);
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
.hub-settle {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-top: 12px;
}

.hub-merge-list {
  margin: 8px 0 0;
  padding-left: 18px;
  font-size: 13px;
  color: var(--color-text-secondary, #475569);
}

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

.hub-dialog-switch {
  align-self: flex-start;
  margin-top: calc(-1 * var(--spacing-2));
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
