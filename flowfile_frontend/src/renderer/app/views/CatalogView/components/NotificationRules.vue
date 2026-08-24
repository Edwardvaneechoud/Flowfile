<template>
  <div class="notification-rules">
    <!-- Rules table -->
    <div v-if="rules.length > 0" class="rules-table">
      <div class="table-header">
        <span class="col-channel">Channel</span>
        <span class="col-toggle">On failure</span>
        <span class="col-toggle">On success</span>
        <span class="col-toggle">On recovery</span>
        <span class="col-enabled">Active</span>
        <span class="col-actions" />
      </div>
      <div
        v-for="rule in rules"
        :key="rule.id"
        class="table-row"
        :class="{ 'row-disabled': !rule.enabled }"
      >
        <div class="col-channel">
          <span class="channel-tag" :class="channelTypeClass(rule.channel_type)">
            <i :class="channelTypeIcon(rule.channel_type)" />
            {{ channelTypeLabel(rule.channel_type) }}
          </span>
          <span class="channel-name">{{ rule.channel_name ?? `Channel #${rule.channel_id}` }}</span>
        </div>
        <div class="col-toggle">
          <el-switch
            :model-value="rule.on_failure"
            size="small"
            :disabled="busyId === rule.id"
            @change="(val: boolean) => patch(rule, { on_failure: val })"
          />
        </div>
        <div class="col-toggle">
          <el-switch
            :model-value="rule.on_success"
            size="small"
            :disabled="busyId === rule.id"
            @change="(val: boolean) => patch(rule, { on_success: val })"
          />
        </div>
        <div class="col-toggle">
          <el-switch
            :model-value="rule.on_recovery"
            size="small"
            :disabled="busyId === rule.id"
            @change="(val: boolean) => patch(rule, { on_recovery: val })"
          />
        </div>
        <div class="col-enabled">
          <el-switch
            :model-value="rule.enabled"
            size="small"
            :disabled="busyId === rule.id"
            @change="(val: boolean) => patch(rule, { enabled: val })"
          />
        </div>
        <div class="col-actions">
          <el-tooltip content="Remove alert" placement="top" :show-after="400">
            <el-button
              size="small"
              type="danger"
              text
              :disabled="busyId === rule.id"
              @click="removeRule(rule)"
            >
              <i class="fa-solid fa-trash" />
            </el-button>
          </el-tooltip>
        </div>
      </div>
    </div>

    <p v-else class="rules-empty">{{ emptyText }}</p>

    <!-- No channels yet: point the user at the Alerts panel instead of a dead select -->
    <div v-if="channels.length === 0" class="rules-prompt">
      <i class="fa-solid fa-circle-info" />
      <span>{{ noChannelsHint }}</span>
      <el-button size="small" type="primary" text @click="emit('needChannel')">
        {{ noChannelsAction }}
      </el-button>
    </div>

    <!-- Add rule -->
    <div v-else class="rules-add">
      <el-select
        v-model="newChannelId"
        placeholder="Select a channel"
        size="small"
        class="add-channel-select"
      >
        <el-option
          v-for="channel in channels"
          :key="channel.id"
          :label="channel.name"
          :value="channel.id"
          :disabled="usedChannelIds.has(channel.id)"
        >
          <span>{{ channel.name }}</span>
          <span class="option-type">{{ channelTypeLabel(channel.channel_type) }}</span>
        </el-option>
      </el-select>
      <el-checkbox v-model="newRule.on_failure" size="small">Failure</el-checkbox>
      <el-checkbox v-model="newRule.on_success" size="small">Success</el-checkbox>
      <el-checkbox v-model="newRule.on_recovery" size="small">Recovery</el-checkbox>
      <el-button
        size="small"
        type="primary"
        :loading="creating"
        :disabled="newChannelId === null || creating"
        @click="addRule"
      >
        <i class="fa-solid fa-plus" /> Add alert
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useNotificationsStore } from "../../../stores/notifications-store";
import type { NotificationChannel, NotificationRule, NotificationRuleUpdate } from "../../../types";
import { channelTypeClass, channelTypeIcon, channelTypeLabel } from "../catalog-formatters";

const props = withDefaults(
  defineProps<{
    rules: NotificationRule[];
    channels: NotificationChannel[];
    /** Rule scope. Leave both unset for "every flow I own". */
    scope?: { registrationId?: number | null; scheduleId?: number | null };
    emptyText?: string;
    noChannelsHint?: string;
    noChannelsAction?: string;
  }>(),
  {
    scope: () => ({}),
    emptyText: "No alerts yet.",
    noChannelsHint: "You need a notification channel before you can be alerted.",
    noChannelsAction: "Add a channel",
  },
);

// "changed": any successful create/update/delete, so the parent can refresh derived
// counts. "needChannel": the user asked to create a channel from the empty prompt.
const emit = defineEmits(["changed", "needChannel"]);

const notificationsStore = useNotificationsStore();

const busyId = ref<number | null>(null);
const creating = ref(false);
const newChannelId = ref<number | null>(null);
// Sensible defaults: shout on failure and when it comes back, stay quiet on success.
const newRule = reactive({ on_failure: true, on_success: false, on_recovery: true });

// One rule per channel per scope keeps the list readable and avoids duplicate pings.
const usedChannelIds = computed(() => new Set(props.rules.map((r) => r.channel_id)));

function detail(e: unknown, fallback: string): string {
  return (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? fallback;
}

async function patch(rule: NotificationRule, body: NotificationRuleUpdate) {
  busyId.value = rule.id;
  try {
    await notificationsStore.updateRule(rule.id, body);
    emit("changed");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to update alert"));
  } finally {
    busyId.value = null;
  }
}

async function addRule() {
  if (newChannelId.value === null) return;
  creating.value = true;
  try {
    await notificationsStore.createRule({
      channel_id: newChannelId.value,
      registration_id: props.scope.registrationId ?? null,
      schedule_id: props.scope.scheduleId ?? null,
      on_failure: newRule.on_failure,
      on_success: newRule.on_success,
      on_recovery: newRule.on_recovery,
    });
    newChannelId.value = null;
    newRule.on_failure = true;
    newRule.on_success = false;
    newRule.on_recovery = true;
    ElMessage.success("Alert added");
    emit("changed");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to add alert"));
  } finally {
    creating.value = false;
  }
}

async function removeRule(rule: NotificationRule) {
  try {
    await ElMessageBox.confirm(
      `Stop sending these alerts to "${rule.channel_name ?? "this channel"}"?`,
      "Remove alert",
      { confirmButtonText: "Remove", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return; // User cancelled
  }
  busyId.value = rule.id;
  try {
    await notificationsStore.deleteRule(rule.id);
    ElMessage.success("Alert removed");
    emit("changed");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to remove alert"));
  } finally {
    busyId.value = null;
  }
}
</script>

<style scoped>
.rules-table {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.rules-table .table-header,
.rules-table .table-row {
  grid-template-columns: 1fr 110px 110px 110px 80px 60px;
}

.col-channel {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-width: 0;
}

.channel-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.col-toggle,
.col-enabled {
  display: flex;
  align-items: center;
}

.rules-empty {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.rules-prompt {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  background: var(--color-background-secondary);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.rules-add {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: var(--spacing-3);
  margin-top: var(--spacing-3);
}

.add-channel-select {
  width: 220px;
}

.option-type {
  float: right;
  margin-left: var(--spacing-3);
  color: var(--el-text-color-secondary);
  font-size: var(--font-size-xs);
}
</style>
