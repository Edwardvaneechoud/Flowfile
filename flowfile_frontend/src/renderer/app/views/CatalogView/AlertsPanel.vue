<template>
  <div class="alerts-overview">
    <div class="alerts-header">
      <h2>Alerts</h2>
      <div class="header-actions">
        <el-button size="small" :loading="refreshing" @click="refresh">
          <i v-if="!refreshing" class="fa-solid fa-arrows-rotate btn-icon" /> Refresh
        </el-button>
        <el-button type="primary" size="small" @click="openCreateChannel">
          <i class="fa-solid fa-plus" /> Add channel
        </el-button>
      </div>
    </div>

    <div class="alerts-intro">
      <i class="fa-solid fa-circle-info"></i>
      <span>
        Flowfile posts to a channel when a run of one of your flows finishes. Add a channel, then
        pick which outcomes it should hear about — here for all flows, or on a single schedule.
      </span>
    </div>

    <!-- Channels -->
    <div class="section">
      <div class="section-header">
        <h3><i class="fa-solid fa-satellite-dish section-icon"></i> Channels</h3>
        <el-button
          v-if="notificationsStore.channels.length > 0"
          size="small"
          type="primary"
          text
          @click="openCreateChannel"
        >
          <i class="fa-solid fa-plus" /> Add channel
        </el-button>
      </div>

      <EmptyState
        v-if="!notificationsStore.loadingChannels && notificationsStore.channels.length === 0"
        icon="fa-solid fa-bell-slash"
        title="No notification channels yet"
        description="Get notified in Slack, Teams or Discord when a scheduled flow fails."
      >
        <template #actions>
          <el-button type="primary" @click="openCreateChannel">
            <i class="fa-solid fa-plus" /> Add channel
          </el-button>
        </template>
      </EmptyState>

      <div v-else-if="notificationsStore.channels.length > 0" class="channels-table">
        <div class="table-header">
          <span class="col-name">Name</span>
          <span class="col-type">Type</span>
          <span class="col-url">Webhook</span>
          <span class="col-enabled">Active</span>
          <span class="col-actions">Actions</span>
        </div>
        <div
          v-for="channel in notificationsStore.channels"
          :key="channel.id"
          class="table-row"
          :class="{ 'row-disabled': !channel.enabled }"
        >
          <div class="col-name">
            <span class="channel-name">{{ channel.name }}</span>
            <span
              v-if="testResultFor(channel.id)"
              class="test-inline"
              :class="testResultFor(channel.id)?.ok ? 'ok' : 'failed'"
            >
              <i
                :class="
                  testResultFor(channel.id)?.ok
                    ? 'fa-solid fa-circle-check'
                    : 'fa-solid fa-circle-exclamation'
                "
              />
              {{ testResultFor(channel.id)?.ok ? "Test message sent" : testErrorFor(channel.id) }}
            </span>
          </div>
          <div class="col-type">
            <span class="channel-tag" :class="channelTypeClass(channel.channel_type)">
              <i :class="channelTypeIcon(channel.channel_type)" />
              {{ channelTypeLabel(channel.channel_type) }}
            </span>
          </div>
          <div class="col-url mono">{{ channel.webhook_url_preview }}</div>
          <div class="col-enabled">
            <el-switch
              :model-value="channel.enabled"
              size="small"
              @change="(val: boolean) => toggleChannel(channel, val)"
            />
          </div>
          <div class="col-actions">
            <el-tooltip content="Send a test message" placement="top" :show-after="400">
              <el-button
                size="small"
                text
                :loading="notificationsStore.testingChannelId === channel.id"
                :disabled="notificationsStore.testingChannelId !== null"
                @click="testChannel(channel)"
              >
                <i
                  v-if="notificationsStore.testingChannelId !== channel.id"
                  class="fa-solid fa-paper-plane"
                />
              </el-button>
            </el-tooltip>
            <el-tooltip content="Edit" placement="top" :show-after="400">
              <el-button size="small" text @click="openEditChannel(channel)">
                <i class="fa-solid fa-pen" />
              </el-button>
            </el-tooltip>
            <el-tooltip content="Delete" placement="top" :show-after="400">
              <el-button size="small" type="danger" text @click="deleteChannel(channel)">
                <i class="fa-solid fa-trash" />
              </el-button>
            </el-tooltip>
          </div>
        </div>
      </div>
    </div>

    <!-- Global rules -->
    <div class="section">
      <div class="section-header">
        <h3><i class="fa-solid fa-globe section-icon"></i> Alert me for all flows</h3>
      </div>
      <p class="section-hint">
        These alerts cover every run of every flow you own. For a single schedule, open that
        schedule and use its Notifications section instead.
      </p>
      <NotificationRules
        :rules="notificationsStore.globalRules"
        :channels="notificationsStore.channels"
        empty-text="No account-wide alerts yet — add one below to hear about every flow."
        no-channels-hint="Add a channel first, then choose which outcomes it should hear about."
        no-channels-action="Add a channel"
        @need-channel="openCreateChannel"
      />
    </div>

    <!-- Recent notifications -->
    <CollapsibleSection
      title="Recent notifications"
      icon="fa-solid fa-clock-rotate-left"
      persist-key="alerts.history"
      :default-open="false"
      :count="notificationsStore.history.length"
    >
      <template #actions>
        <el-button
          size="small"
          text
          :loading="notificationsStore.loadingHistory"
          @click="loadHistory"
        >
          <i v-if="!notificationsStore.loadingHistory" class="fa-solid fa-arrows-rotate" />
        </el-button>
      </template>

      <p v-if="notificationsStore.history.length === 0" class="rules-empty">
        Nothing sent yet. Delivered alerts show up here.
      </p>
      <div v-else class="history-table">
        <div class="table-header">
          <span class="col-status">Status</span>
          <span class="col-event">Event</span>
          <span class="col-flow">Flow</span>
          <span class="col-channel">Channel</span>
          <span class="col-time">When</span>
        </div>
        <div v-for="item in notificationsStore.history" :key="item.id" class="table-row">
          <div class="col-status">
            <el-tooltip
              v-if="item.status === 'dead' && item.last_error"
              :content="item.last_error"
              placement="top"
            >
              <span class="status-badge" :class="deliveryStatusClass(item.status)">
                {{ deliveryStatusLabel(item.status) }}
              </span>
            </el-tooltip>
            <span v-else class="status-badge" :class="deliveryStatusClass(item.status)">
              {{ deliveryStatusLabel(item.status) }}
            </span>
            <span v-if="item.attempts > 1" class="attempts">×{{ item.attempts }}</span>
          </div>
          <div class="col-event">
            <i :class="notificationEventIcon(item.event_type)" class="type-icon" />
            {{ notificationEventLabel(item.event_type) }}
          </div>
          <div class="col-flow">{{ item.flow_name ?? "--" }}</div>
          <div class="col-channel">{{ item.channel_name ?? "--" }}</div>
          <div class="col-time">{{ formatDate(item.sent_at ?? item.created_at) }}</div>
        </div>
      </div>
    </CollapsibleSection>

    <CreateChannelModal
      :visible="showChannelModal"
      :mode="editChannel ? 'edit' : 'create'"
      :edit-channel="editChannel"
      :saving="savingChannel"
      @create="handleCreateChannel"
      @update="handleUpdateChannel"
      @close="closeChannelModal"
    />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useNotificationsStore } from "../../stores/notifications-store";
import type {
  NotificationChannel,
  NotificationChannelCreate,
  NotificationChannelUpdate,
} from "../../types";
import {
  channelTypeClass,
  channelTypeIcon,
  channelTypeLabel,
  deliveryStatusClass,
  deliveryStatusLabel,
  formatDate,
  notificationEventIcon,
  notificationEventLabel,
} from "./catalog-formatters";
import { CollapsibleSection, EmptyState } from "../../components/common";
import CreateChannelModal from "./CreateChannelModal.vue";
import NotificationRules from "./components/NotificationRules.vue";

const notificationsStore = useNotificationsStore();

const refreshing = ref(false);
const showChannelModal = ref(false);
const savingChannel = ref(false);
const editChannel = ref<NotificationChannel | null>(null);

function detail(e: unknown, fallback: string): string {
  return (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ?? fallback;
}

function testResultFor(channelId: number) {
  return notificationsStore.testResults[channelId] ?? null;
}

function testErrorFor(channelId: number): string {
  return notificationsStore.testResults[channelId]?.error ?? "Test failed";
}

async function refresh() {
  refreshing.value = true;
  try {
    await notificationsStore.initialize();
  } finally {
    refreshing.value = false;
  }
}

function loadHistory() {
  return notificationsStore.loadHistory();
}

function openCreateChannel() {
  editChannel.value = null;
  showChannelModal.value = true;
}

function openEditChannel(channel: NotificationChannel) {
  editChannel.value = channel;
  showChannelModal.value = true;
}

function closeChannelModal() {
  showChannelModal.value = false;
  editChannel.value = null;
}

async function handleCreateChannel(body: NotificationChannelCreate) {
  savingChannel.value = true;
  try {
    await notificationsStore.createChannel(body);
    closeChannelModal();
    ElMessage.success("Channel added");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to add channel"));
  } finally {
    savingChannel.value = false;
  }
}

async function handleUpdateChannel(body: NotificationChannelUpdate) {
  const channel = editChannel.value;
  if (!channel) return;
  savingChannel.value = true;
  try {
    await notificationsStore.updateChannel(channel.id, body);
    notificationsStore.clearTestResult(channel.id);
    closeChannelModal();
    ElMessage.success("Channel updated");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to update channel"));
  } finally {
    savingChannel.value = false;
  }
}

async function toggleChannel(channel: NotificationChannel, enabled: boolean) {
  try {
    await notificationsStore.updateChannel(channel.id, { enabled });
  } catch (e) {
    ElMessage.error(detail(e, "Failed to update channel"));
    await notificationsStore.loadChannels();
  }
}

async function deleteChannel(channel: NotificationChannel) {
  try {
    await ElMessageBox.confirm(
      `Delete "${channel.name}"? Any alerts routed to it are removed too.`,
      "Delete channel",
      { confirmButtonText: "Delete", cancelButtonText: "Cancel", type: "warning" },
    );
  } catch {
    return; // User cancelled
  }
  try {
    await notificationsStore.deleteChannel(channel.id);
    ElMessage.success("Channel deleted");
  } catch (e) {
    ElMessage.error(detail(e, "Failed to delete channel"));
  }
}

/** Sends a real message, so the verdict is surfaced both inline and as a toast. */
async function testChannel(channel: NotificationChannel) {
  const result = await notificationsStore.testChannel(channel.id);
  if (result.ok) ElMessage.success("Test message sent");
  else ElMessage.error(result.error || "Test failed");
}

onMounted(() => {
  notificationsStore.initialize();
});
</script>

<style scoped>
.alerts-overview {
  max-width: 1000px;
  margin: 0 auto;
}

.alerts-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: var(--spacing-4);
}

.alerts-overview h2 {
  margin: 0;
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.header-actions {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.alerts-intro {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-3);
  margin-bottom: var(--spacing-5);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  line-height: 1.4;
}

.alerts-intro i {
  margin-top: 1px;
  flex-shrink: 0;
}

.section-hint {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  line-height: 1.4;
}

.channels-table,
.history-table {
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  overflow: hidden;
}

.channels-table .table-header,
.channels-table .table-row {
  grid-template-columns: 1fr 130px minmax(160px, 1.2fr) 80px 130px;
}

.history-table .table-header,
.history-table .table-row {
  grid-template-columns: 120px 160px 1fr 1fr 150px;
}

.channels-table .table-row,
.history-table .table-row {
  cursor: default;
}

.col-name {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.channel-name {
  font-weight: var(--font-weight-medium);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.col-url {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--color-text-muted);
  font-size: var(--font-size-xs);
}

.col-enabled {
  display: flex;
  align-items: center;
}

.col-status {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
}

.col-event,
.col-channel,
.col-time {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.attempts {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}

.test-inline {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: var(--font-size-xs);
  overflow-wrap: anywhere;
}

.test-inline.ok {
  color: var(--color-success);
}

.test-inline.failed {
  color: var(--color-danger);
}

.rules-empty {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-muted);
}

.btn-icon {
  margin-right: 6px;
}
</style>
