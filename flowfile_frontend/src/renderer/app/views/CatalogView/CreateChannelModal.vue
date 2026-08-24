<template>
  <el-dialog
    :model-value="visible"
    width="540px"
    align-center
    class="channel-dialog"
    @close="$emit('close')"
  >
    <template #header>
      <div class="dialog-header">
        <span class="dialog-header-icon">
          <i :class="isEdit ? 'fa-solid fa-pen-to-square' : 'fa-solid fa-bell'"></i>
        </span>
        <div class="dialog-header-text">
          <span class="dialog-title">{{ isEdit ? "Edit channel" : "Add channel" }}</span>
          <span class="dialog-subtitle">{{
            isEdit ? "Change where these alerts go" : "Where should Flowfile send alerts?"
          }}</span>
        </div>
      </div>
    </template>

    <el-form label-position="top" class="channel-form" @submit.prevent="handleSave">
      <el-form-item label="Name">
        <el-input
          v-model="form.name"
          :maxlength="100"
          placeholder="e.g. #data-alerts"
          @input="resetTest"
        />
      </el-form-item>

      <el-form-item label="Where should it post?">
        <div class="type-cards">
          <button
            v-for="t in CHANNEL_TYPES"
            :key="t.value"
            type="button"
            class="type-card"
            :class="{ active: form.channel_type === t.value }"
            @click="selectType(t.value)"
          >
            <span class="type-card-icon"><i :class="t.icon"></i></span>
            <span class="type-card-text">
              <span class="type-card-title">{{ t.title }}</span>
              <span class="type-card-sub">{{ t.help }}</span>
            </span>
            <i
              v-if="form.channel_type === t.value"
              class="fa-solid fa-circle-check type-card-check"
            ></i>
          </button>
        </div>
      </el-form-item>

      <el-form-item :label="isEdit ? 'Webhook URL (leave blank to keep current)' : 'Webhook URL'">
        <el-input
          v-model="form.webhook_url"
          :placeholder="urlPlaceholder"
          spellcheck="false"
          @input="resetTest"
        />
        <div class="hint-text">
          {{ activeType.help }}
        </div>
      </el-form-item>

      <!-- Test before saving: the URL never comes back from the server, so this is
           the only moment a typo is cheap to catch. -->
      <div class="test-row">
        <el-button size="small" :loading="testing" :disabled="!canTest || testing" @click="runTest">
          <i v-if="!testing" class="fa-solid fa-paper-plane btn-icon" /> Send test
        </el-button>
        <span v-if="testResult && testResult.ok" class="test-result ok">
          <i class="fa-solid fa-circle-check" /> Test message sent
        </span>
        <span v-else-if="testResult" class="test-result failed">
          <i class="fa-solid fa-circle-exclamation" />
          {{ testResult.error || "Test failed" }}
        </span>
        <span v-else class="test-result hint">Optional — confirms the URL works.</span>
      </div>
    </el-form>

    <template #footer>
      <el-button @click="$emit('close')">Cancel</el-button>
      <el-button type="primary" :disabled="!isValid" :loading="saving" @click="handleSave">
        {{ isEdit ? "Save changes" : "Add channel" }}
      </el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { useNotificationsStore } from "../../stores/notifications-store";
import type {
  NotificationChannel,
  NotificationChannelCreate,
  NotificationChannelType,
  NotificationChannelUpdate,
  NotificationTestResult,
} from "../../types";

const props = defineProps<{
  visible: boolean;
  saving?: boolean;
  // "edit" reuses the same form; the stored webhook URL is never returned, so an
  // empty URL field means "keep the one already saved".
  mode?: "create" | "edit";
  editChannel?: NotificationChannel | null;
}>();

const emit = defineEmits(["close", "create", "update"]);

const notificationsStore = useNotificationsStore();

const isEdit = computed(() => props.mode === "edit");

const CHANNEL_TYPES: {
  value: NotificationChannelType;
  icon: string;
  title: string;
  help: string;
  placeholder: string;
}[] = [
  {
    value: "slack",
    icon: "fa-brands fa-slack",
    title: "Slack",
    help: "Paste a Slack Incoming Webhook URL — create one at api.slack.com/apps",
    placeholder: "https://hooks.slack.com/services/...",
  },
  {
    value: "discord",
    icon: "fa-brands fa-discord",
    title: "Discord",
    help: "Channel settings → Integrations → Webhooks",
    placeholder: "https://discord.com/api/webhooks/...",
  },
  {
    value: "teams",
    icon: "fa-brands fa-microsoft",
    title: "Microsoft Teams",
    help: "Channel → Connectors → Incoming Webhook",
    placeholder: "https://outlook.office.com/webhook/...",
  },
  {
    value: "generic",
    icon: "fa-solid fa-link",
    title: "Generic webhook",
    help: "Any HTTPS endpoint; receives a JSON POST",
    placeholder: "https://example.com/hooks/flowfile",
  },
];

const form = ref<{
  name: string;
  channel_type: NotificationChannelType;
  webhook_url: string;
}>({
  name: "",
  channel_type: "slack",
  webhook_url: "",
});

const testing = ref(false);
const testResult = ref<NotificationTestResult | null>(null);

const activeType = computed(
  () => CHANNEL_TYPES.find((t) => t.value === form.value.channel_type) ?? CHANNEL_TYPES[0],
);

const urlPlaceholder = computed(() =>
  isEdit.value ? "Leave blank to keep the saved URL" : activeType.value.placeholder,
);

const canTest = computed(() => form.value.webhook_url.trim().length > 0);

const isValid = computed(() => {
  if (!form.value.name.trim()) return false;
  // Editing may keep the stored URL; creating always needs one.
  return isEdit.value || form.value.webhook_url.trim().length > 0;
});

function resetTest() {
  testResult.value = null;
}

function selectType(type: NotificationChannelType) {
  form.value.channel_type = type;
  resetTest();
}

watch(
  () => props.visible,
  (open) => {
    if (!open) return;
    testResult.value = null;
    testing.value = false;
    if (isEdit.value && props.editChannel) {
      form.value.name = props.editChannel.name;
      form.value.channel_type = props.editChannel.channel_type;
      form.value.webhook_url = "";
    } else {
      form.value.name = "";
      form.value.channel_type = "slack";
      form.value.webhook_url = "";
    }
  },
);

async function runTest() {
  if (!canTest.value) return;
  testing.value = true;
  testResult.value = null;
  try {
    testResult.value = await notificationsStore.testChannelUrl({
      channel_type: form.value.channel_type,
      webhook_url: form.value.webhook_url.trim(),
    });
  } finally {
    testing.value = false;
  }
}

function handleSave() {
  if (!isValid.value) return;
  if (isEdit.value) {
    const body: NotificationChannelUpdate = {
      name: form.value.name.trim(),
      channel_type: form.value.channel_type,
    };
    const url = form.value.webhook_url.trim();
    if (url) body.webhook_url = url;
    emit("update", body);
    return;
  }
  const body: NotificationChannelCreate = {
    name: form.value.name.trim(),
    channel_type: form.value.channel_type,
    webhook_url: form.value.webhook_url.trim(),
  };
  emit("create", body);
}
</script>

<style scoped>
/* Dialog chrome — matches CreateScheduleModal so the two feel like one family. */
.channel-dialog :deep(.el-dialog) {
  border-radius: var(--border-radius-lg);
  overflow: hidden;
}
.channel-dialog :deep(.el-dialog__header) {
  margin-right: 0;
  padding-bottom: var(--spacing-2);
  border-bottom: 1px solid var(--el-border-color-lighter);
}
.channel-dialog :deep(.el-dialog__body) {
  padding-top: var(--spacing-3);
  padding-bottom: var(--spacing-1);
}

.channel-form :deep(.el-form-item) {
  margin-bottom: var(--spacing-3);
}
.channel-form :deep(.el-form-item__label) {
  padding-bottom: 3px;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--el-text-color-regular);
  line-height: 1.3;
}

.dialog-header {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
}
.dialog-header-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  flex-shrink: 0;
  border-radius: var(--border-radius-md);
  background: var(--el-color-primary-light-9);
  color: var(--el-color-primary);
  font-size: 15px;
}
.dialog-header-text {
  display: flex;
  flex-direction: column;
  line-height: 1.25;
}
.dialog-title {
  font-size: var(--font-size-xl);
  font-weight: var(--font-weight-semibold);
  color: var(--el-text-color-primary);
}
.dialog-subtitle {
  font-size: var(--font-size-sm);
  color: var(--el-text-color-secondary);
}

/* Channel-type cards */
.type-cards {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
.type-card {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  width: 100%;
  padding: var(--spacing-2) var(--spacing-3);
  border: 1px solid var(--el-border-color);
  border-radius: var(--border-radius-md);
  background: var(--el-bg-color);
  cursor: pointer;
  text-align: left;
  transition:
    border-color 0.15s ease,
    background 0.15s ease,
    box-shadow 0.15s ease;
}
.type-card:hover {
  border-color: var(--el-color-primary-light-5);
  background: var(--el-fill-color-light);
}
.type-card.active {
  border-color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
  box-shadow: inset 0 0 0 1px var(--el-color-primary);
}
.type-card-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 30px;
  height: 30px;
  flex-shrink: 0;
  border-radius: var(--border-radius-md);
  background: var(--el-fill-color);
  color: var(--el-text-color-regular);
  font-size: 14px;
  transition:
    background 0.15s ease,
    color 0.15s ease;
}
.type-card.active .type-card-icon {
  background: var(--el-color-primary);
  color: #fff;
}
.type-card-text {
  display: flex;
  flex-direction: column;
  gap: 2px;
  flex: 1;
  min-width: 0;
}
.type-card-title {
  font-size: var(--font-size-md);
  font-weight: var(--font-weight-medium);
  color: var(--el-text-color-primary);
}
.type-card-sub {
  font-size: var(--font-size-sm);
  color: var(--el-text-color-secondary);
}
.type-card-check {
  color: var(--el-color-primary);
  font-size: 16px;
  flex-shrink: 0;
}

.hint-text {
  font-size: var(--font-size-sm);
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}

.test-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-2) var(--spacing-3);
  margin-bottom: var(--spacing-2);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: var(--border-radius-md);
  background: var(--el-fill-color-lighter);
}

.test-result {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  font-size: var(--font-size-sm);
  overflow-wrap: anywhere;
}
.test-result.ok {
  color: var(--el-color-success);
  font-weight: var(--font-weight-medium);
}
.test-result.failed {
  color: var(--el-color-error);
}
.test-result.hint {
  color: var(--el-text-color-secondary);
}

.btn-icon {
  margin-right: 6px;
}
</style>
