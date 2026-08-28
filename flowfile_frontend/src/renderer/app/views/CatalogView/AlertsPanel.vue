<template>
  <div class="alerts-overview">
    <div class="alerts-header">
      <h2>Alerts</h2>
      <div class="header-actions">
        <el-button size="small" :loading="refreshing" @click="refresh">
          <i v-if="!refreshing" class="fa-solid fa-arrows-rotate" /> Refresh
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
          <span class="col-url">
            Webhook
            <el-tooltip
              content="Only a masked preview is shown — the full URL is stored encrypted and never sent back to the browser."
              placement="top"
              :show-after="200"
            >
              <i class="fa-regular fa-circle-question header-help" tabindex="0" />
            </el-tooltip>
          </span>
          <span class="col-enabled">
            Active
            <el-tooltip
              content="Switched off, this channel receives nothing — from any alert."
              placement="top"
              :show-after="200"
            >
              <i class="fa-regular fa-circle-question header-help" tabindex="0" />
            </el-tooltip>
          </span>
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
              :loading="togglingChannelIds.has(channel.id)"
              :disabled="togglingChannelIds.has(channel.id)"
              :aria-label="`Channel ${channel.name} active`"
              @change="(val: boolean) => toggleChannel(channel, val)"
            />
          </div>
          <div class="col-actions">
            <el-tooltip
              content="Send a test message to this channel now"
              placement="top"
              :show-after="400"
            >
              <el-button
                size="small"
                text
                aria-label="Send a test message"
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
            <el-tooltip content="Edit name, type or URL" placement="top" :show-after="400">
              <el-button
                size="small"
                text
                aria-label="Edit channel"
                @click="openEditChannel(channel)"
              >
                <i class="fa-solid fa-pen" />
              </el-button>
            </el-tooltip>
            <el-tooltip content="Delete channel and its alerts" placement="top" :show-after="400">
              <el-button
                size="small"
                type="danger"
                text
                aria-label="Delete channel"
                @click="deleteChannel(channel)"
              >
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
        <h3><i class="fa-solid fa-globe section-icon"></i> Alerts for all flows</h3>
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
      :summary="historySummary"
    >
      <template #actions>
        <el-tooltip content="Reload the delivery history" placement="top" :show-after="400">
          <el-button
            size="small"
            text
            aria-label="Reload delivery history"
            :loading="notificationsStore.loadingHistory"
            @click="loadHistory"
          >
            <i v-if="!notificationsStore.loadingHistory" class="fa-solid fa-arrows-rotate" />
          </el-button>
        </el-tooltip>
      </template>

      <p v-if="notificationsStore.history.length === 0" class="rules-empty">
        Nothing sent yet. Delivered alerts show up here.
      </p>
      <template v-else>
        <div class="history-table">
          <div class="table-header">
            <span class="col-status">Status</span>
            <span class="col-event">Event</span>
            <span class="col-flow">Flow</span>
            <span class="col-channel">Channel</span>
            <span class="col-time">When</span>
          </div>
          <div v-for="item in pagedHistory" :key="item.id" class="table-row">
            <div class="col-status">
              <el-tooltip :content="statusTooltip(item)" placement="top" :show-after="200">
                <span class="status-badge" :class="deliveryStatusClass(item.status)" tabindex="0">
                  {{ deliveryStatusLabel(item.status) }}
                </span>
              </el-tooltip>
              <el-tooltip
                v-if="item.attempts > 1"
                :content="`${item.attempts} delivery attempts`"
                placement="top"
                :show-after="200"
              >
                <span class="attempts">×{{ item.attempts }}</span>
              </el-tooltip>
            </div>
            <div class="col-event">
              <el-tooltip
                :content="notificationEventDescription(item.event_type)"
                placement="top"
                :show-after="200"
              >
                <span class="event-cell">
                  <i :class="notificationEventIcon(item.event_type)" class="type-icon" />
                  {{ notificationEventLabel(item.event_type) }}
                </span>
              </el-tooltip>
            </div>
            <div class="col-flow">{{ item.flow_name ?? "--" }}</div>
            <div class="col-channel">{{ item.channel_name ?? "--" }}</div>
            <div class="col-time">{{ formatDate(item.sent_at ?? item.created_at) }}</div>
          </div>
        </div>

        <div v-if="historyTotalPages > 1" class="pagination-bar">
          <button
            class="page-btn"
            :disabled="historyPage <= 1"
            aria-label="First page"
            @click="historyPage = 1"
          >
            <i class="fa-solid fa-angles-left" />
          </button>
          <button
            class="page-btn"
            :disabled="historyPage <= 1"
            aria-label="Previous page"
            @click="historyPage -= 1"
          >
            <i class="fa-solid fa-angle-left" />
          </button>
          <span class="page-info">Page {{ historyPage }} of {{ historyTotalPages }}</span>
          <button
            class="page-btn"
            :disabled="historyPage >= historyTotalPages"
            aria-label="Next page"
            @click="historyPage += 1"
          >
            <i class="fa-solid fa-angle-right" />
          </button>
          <button
            class="page-btn"
            :disabled="historyPage >= historyTotalPages"
            aria-label="Last page"
            @click="historyPage = historyTotalPages"
          >
            <i class="fa-solid fa-angles-right" />
          </button>
        </div>
      </template>
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
import { computed, onMounted, ref, watch } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { useNotificationsStore } from "../../stores/notifications-store";
import type {
  NotificationChannel,
  NotificationChannelCreate,
  NotificationChannelUpdate,
  NotificationHistoryItem,
} from "../../types";
import {
  channelTypeClass,
  channelTypeIcon,
  channelTypeLabel,
  deliveryStatusClass,
  deliveryStatusLabel,
  formatDate,
  notificationEventDescription,
  notificationEventIcon,
  notificationEventLabel,
} from "./catalog-formatters";
import { detailMessage } from "../../composables/saveError";
import { CollapsibleSection, EmptyState } from "../../components/common";
import CreateChannelModal from "./CreateChannelModal.vue";
import NotificationRules from "./components/NotificationRules.vue";

const notificationsStore = useNotificationsStore();

const refreshing = ref(false);
const showChannelModal = ref(false);
const savingChannel = ref(false);
const editChannel = ref<NotificationChannel | null>(null);

function testResultFor(channelId: number) {
  return notificationsStore.testResults[channelId] ?? null;
}

function testErrorFor(channelId: number): string {
  return notificationsStore.testResults[channelId]?.error ?? "Test failed";
}

const HISTORY_PAGE_SIZE = 10;
const historyPage = ref(1);
const historyTotalPages = computed(() =>
  Math.max(1, Math.ceil(notificationsStore.history.length / HISTORY_PAGE_SIZE)),
);
const pagedHistory = computed(() =>
  notificationsStore.history.slice(
    (historyPage.value - 1) * HISTORY_PAGE_SIZE,
    historyPage.value * HISTORY_PAGE_SIZE,
  ),
);
// Clamp rather than reset so a background refresh can't leave us on a page past the end.
watch(historyTotalPages, (pages) => {
  if (historyPage.value > pages) historyPage.value = pages;
});

// Surfaces failures on the collapsed header, where a bare count would hide them.
const historySummary = computed(() => {
  const failed = notificationsStore.history.filter((h) => h.status === "dead").length;
  if (failed === 0) return undefined;
  return `${failed} failed · ${notificationsStore.history.length} total`;
});

function statusTooltip(item: NotificationHistoryItem): string {
  if (item.status === "sent") return "Delivered to the webhook.";
  if (item.status === "pending") return "Queued — sent on the next delivery pass.";
  if (item.status === "sending") return "Delivery attempt in progress.";
  return item.last_error
    ? `Gave up after ${item.attempts} attempts — ${item.last_error}`
    : "Delivery gave up after repeated failures.";
}

async function refresh() {
  refreshing.value = true;
  try {
    await notificationsStore.initialize();
    historyPage.value = 1;
  } finally {
    refreshing.value = false;
  }
}

async function loadHistory() {
  await notificationsStore.loadHistory();
  historyPage.value = 1;
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
    ElMessage.error(detailMessage(e, "Failed to add channel"));
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
    ElMessage.error(detailMessage(e, "Failed to update channel"));
  } finally {
    savingChannel.value = false;
  }
}

const togglingChannelIds = ref<Set<number>>(new Set());

async function toggleChannel(channel: NotificationChannel, enabled: boolean) {
  const start = new Set(togglingChannelIds.value);
  start.add(channel.id);
  togglingChannelIds.value = start;
  try {
    await notificationsStore.updateChannel(channel.id, { enabled });
  } catch (e) {
    ElMessage.error(detailMessage(e, "Failed to update channel"));
    await notificationsStore.loadChannels();
  } finally {
    const done = new Set(togglingChannelIds.value);
    done.delete(channel.id);
    togglingChannelIds.value = done;
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
    ElMessage.error(detailMessage(e, "Failed to delete channel"));
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

/* Rows here hold switches and buttons, not links — no clickable-row highlight. */
.channels-table .table-row:hover,
.history-table .table-row:hover {
  background: transparent;
}

/* Element Plus adds margin between adjacent buttons; the column gap already spaces them. */
.col-actions .el-button + .el-button {
  margin-left: 0;
}

.history-table .col-flow {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
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

/* Element Plus renders the button label in a flex span, which drops the
   whitespace between icon and text — restore the gap for icon+label buttons. */
.alerts-overview :deep(.el-button > span) {
  gap: 6px;
}

.event-cell {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  max-width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
