<template>
  <div class="change-feed-section">
    <div class="catalog-field">
      <label class="catalog-label">Read</label>
      <el-tooltip
        :disabled="disabledReason === null"
        :content="disabledReason ?? ''"
        placement="top"
      >
        <el-select v-model="cdcMode" size="small" :disabled="disabledReason !== null">
          <el-option label="Full table" value="off" />
          <el-option v-if="allowLastRun" label="Changes since last run" value="since_last_run" />
          <el-option label="Changes since version" value="since_version" />
          <el-option label="Changes since time" value="since_timestamp" />
        </el-select>
      </el-tooltip>
    </div>

    <div v-if="showNotEnabled" class="cdc-warning">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <div class="cdc-warning-body">
        <span>
          Change tracking is not enabled on this table. Only commits made after it is turned on are
          tracked.
        </span>
        <el-button
          v-if="canEnable"
          size="small"
          type="primary"
          :loading="enabling"
          @click="emit('enable')"
        >
          Enable change tracking
        </el-button>
      </div>
    </div>

    <div v-if="cdcMode === 'since_version'" class="catalog-field">
      <label class="catalog-label">Since version</label>
      <template v-if="!versionParam">
        <el-select
          v-if="versionOptions.length > 0"
          v-model="fromVersion"
          size="small"
          filterable
          placeholder="Pick a version"
        >
          <el-option
            v-for="v in versionOptions"
            :key="v.version"
            :label="v.label"
            :value="v.version"
          />
        </el-select>
        <el-input-number
          v-else
          v-model="fromVersionNumber"
          size="small"
          :min="0"
          controls-position="right"
          placeholder="Version"
        />
      </template>
      <el-select
        v-if="versionParamOptions.length > 0"
        v-model="versionParam"
        size="small"
        clearable
        placeholder="Or use a flow parameter"
      >
        <el-option
          v-for="p in versionParamOptions"
          :key="p.name"
          :label="'${' + p.name + '}'"
          :value="p.name"
        />
      </el-select>
      <p class="section-hint">Reads every change committed after this version.</p>
      <p v-if="errors.version" class="field-error">{{ errors.version }}</p>
    </div>

    <div v-if="cdcMode === 'since_timestamp'" class="catalog-field">
      <label class="catalog-label">Since</label>
      <DateTimePicker
        v-if="!timestampParam"
        v-model="fromTimestamp"
        placeholder="Pick a date and time"
        show-seconds
      />
      <el-select
        v-if="timestampParamOptions.length > 0"
        v-model="timestampParam"
        size="small"
        clearable
        placeholder="Or use a flow parameter"
      >
        <el-option
          v-for="p in timestampParamOptions"
          :key="p.name"
          :label="'${' + p.name + '}'"
          :value="p.name"
        />
      </el-select>
      <p v-if="errors.timestamp" class="field-error">{{ errors.timestamp }}</p>
    </div>

    <template v-if="allowLastRun && cdcMode === 'since_last_run'">
      <div class="cdc-status">
        <i class="fa-solid fa-bookmark"></i>
        <span class="cdc-status-text">{{ cursorStatus }}</span>
        <el-button v-if="cursor" text size="small" :loading="resetting" @click="resetCursor">
          Reset cursor
        </el-button>
      </div>
      <div class="catalog-field">
        <label class="catalog-label">Start from</label>
        <el-select v-model="cdcStart" size="small">
          <el-option label="Now — skip existing history" value="now" />
          <el-option label="Beginning — replay everything tracked" value="beginning" />
        </el-select>
      </div>
      <CollapsibleSection
        title="Cursor name (optional)"
        :default-open="false"
        nested
        :persist-key="`${persistKey}Consumer`"
      >
        <div class="catalog-field">
          <el-input
            v-model="consumerName"
            size="small"
            placeholder="Defaults to this node in this flow"
          />
          <p v-if="errors.consumerName" class="field-error">{{ errors.consumerName }}</p>
          <p class="section-hint">
            A named cursor is shared by every flow and notebook reading this table under the same
            name.
          </p>
        </div>
      </CollapsibleSection>
      <p class="section-hint cdc-hint">
        <i class="fa-solid fa-circle-info"></i>
        Delivery is at-least-once — a failed run replays its window, so downstream writers should
        upsert, not append.
      </p>
    </template>

    <div v-if="cdcMode !== 'off'" class="catalog-field">
      <el-checkbox v-model="includePreimage" size="small">
        Include row values from before each update
      </el-checkbox>
    </div>
  </div>
</template>

<script lang="ts" setup>
/**
 * The "Read" selector of a Delta change-feed reader and the fields each mode needs.
 *
 * Shared by the catalog reader (which also offers "since last run" with its cursor) and the
 * cloud storage reader (a bare path has no cursor store). The parent owns loading the table's
 * tracking status and acting on ``enable`` / ``reset-cursor``; every field edit is emitted as a
 * whole new settings object through ``update:modelValue``.
 */
import { computed, onMounted } from "vue";
import { useFlowStore } from "../../../stores/flow-store";
import type { FlowParameter } from "../../../types/flow.types";
import type { CdcCursor, TableCdcStatus } from "../../../types/catalog.types";
import type { CdcMode, CdcReaderSettings, CdcStart } from "../../../types/node.types";
import { cdcFieldErrors, cursorStatusLine, findCursor } from "../../../utils/catalogCdc";
import CollapsibleSection from "../CollapsibleSection/CollapsibleSection.vue";
import DateTimePicker from "../DateTimePicker/DateTimePicker.vue";

const props = withDefaults(
  defineProps<{
    modelValue: CdcReaderSettings;
    nodeId: number;
    /** Null while unknown; the "not enabled" warning only shows for a known-untracked table. */
    status: TableCdcStatus | null;
    versionOptions: { version: number; label: string }[];
    disabledReason: string | null;
    canEnable: boolean;
    enabling: boolean;
    resetting?: boolean;
    allowLastRun: boolean;
    persistKey: string;
  }>(),
  { resetting: false },
);

const emit = defineEmits<{
  (e: "update:modelValue", value: CdcReaderSettings): void;
  (e: "enable"): void;
  (e: "reset-cursor", cursor: CdcCursor): void;
}>();

const flowStore = useFlowStore();

function update(patch: Partial<CdcReaderSettings>) {
  emit("update:modelValue", { ...props.modelValue, ...patch });
}

const cdcMode = computed<CdcMode>({
  get: () => props.modelValue.cdc_mode,
  set: (mode: CdcMode) => {
    const next: CdcReaderSettings = { ...props.modelValue, cdc_mode: mode };
    if (mode !== "since_version") next.cdc_from_version = null;
    if (mode !== "since_timestamp") next.cdc_from_timestamp = null;
    if (mode === "off") {
      next.cdc_consumer_name = null;
      next.cdc_include_preimage = false;
    }
    emit("update:modelValue", next);
  },
});

const fromVersion = computed({
  get: () => props.modelValue.cdc_from_version,
  set: (version: number | string | null) => update({ cdc_from_version: version ?? null }),
});

const fromVersionNumber = computed({
  get: () =>
    typeof props.modelValue.cdc_from_version === "number"
      ? props.modelValue.cdc_from_version
      : undefined,
  set: (version: number | undefined) => update({ cdc_from_version: version ?? null }),
});

const fromTimestamp = computed({
  get: () => props.modelValue.cdc_from_timestamp,
  set: (value: string | null) => update({ cdc_from_timestamp: value }),
});

const cdcStart = computed({
  get: () => props.modelValue.cdc_start ?? "now",
  set: (start: CdcStart) => update({ cdc_start: start }),
});

const consumerName = computed({
  get: () => props.modelValue.cdc_consumer_name ?? "",
  set: (value: string) => update({ cdc_consumer_name: value.trim() ? value : null }),
});

const includePreimage = computed({
  get: () => props.modelValue.cdc_include_preimage,
  set: (value: boolean) => update({ cdc_include_preimage: value }),
});

const PARAM_REF = /^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$/;
const paramRefName = (value: unknown): string =>
  typeof value === "string" ? (PARAM_REF.exec(value)?.[1] ?? "") : "";
const versionParamOptions = computed<FlowParameter[]>(() =>
  flowStore.parameters.filter((p) => p.type === "integer"),
);
const timestampParamOptions = computed<FlowParameter[]>(() =>
  flowStore.parameters.filter((p) => !p.type || p.type === "string"),
);
const versionParam = computed({
  get: () => paramRefName(props.modelValue.cdc_from_version),
  set: (name: string) => update({ cdc_from_version: name ? "${" + name + "}" : null }),
});
const timestampParam = computed({
  get: () => paramRefName(props.modelValue.cdc_from_timestamp),
  set: (name: string) => update({ cdc_from_timestamp: name ? "${" + name + "}" : null }),
});
onMounted(() => {
  if (flowStore.flowId > 0) void flowStore.loadParameters(flowStore.flowId);
});

const errors = computed(() => cdcFieldErrors(props.modelValue));

const showNotEnabled = computed(
  () => cdcMode.value !== "off" && props.status !== null && !props.status.cdc_enabled,
);

const cursor = computed(() =>
  props.status
    ? findCursor(props.status.cursors, props.modelValue.cdc_consumer_name ?? null, props.nodeId)
    : null,
);

const cursorStatus = computed(() => cursorStatusLine(cursor.value, cdcStart.value));

function resetCursor() {
  if (cursor.value) emit("reset-cursor", cursor.value);
}
</script>

<style scoped>
.change-feed-section {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.catalog-field {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.catalog-label {
  font-size: 12px;
  font-weight: 500;
  color: var(--color-text-secondary);
}

.section-hint {
  margin: 0;
  font-size: 12px;
  color: var(--color-text-muted);
}

.field-error {
  margin: 0;
  font-size: 11px;
  color: var(--el-color-danger);
}

.cdc-status {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 8px;
  font-size: 12px;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary, #f5f7fa);
  border: 1px solid var(--color-border-primary);
  border-radius: 4px;
}

.cdc-status-text {
  flex: 1;
}

.cdc-warning {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  background: rgba(245, 158, 11, 0.08);
  border: 1px solid rgba(245, 158, 11, 0.3);
  border-radius: 4px;
  font-size: 12px;
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.cdc-warning i {
  color: var(--color-warning, #f59e0b);
  margin-top: 2px;
}

.cdc-warning-body {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 6px;
}

.cdc-hint {
  display: flex;
  align-items: flex-start;
  gap: 6px;
  line-height: 1.4;
}
</style>
