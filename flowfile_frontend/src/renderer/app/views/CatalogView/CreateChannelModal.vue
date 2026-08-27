<template>
  <el-dialog
    :model-value="visible"
    width="540px"
    align-center
    class="channel-dialog catalog-dialog"
    @close="$emit('close')"
  >
    <template #header="{ titleId }">
      <div class="dialog-header">
        <span class="dialog-header-icon">
          <i :class="isEdit ? 'fa-solid fa-pen-to-square' : 'fa-solid fa-bell'"></i>
        </span>
        <div class="dialog-header-text">
          <span :id="titleId" class="dialog-title">{{
            isEdit ? "Edit channel" : "Add channel"
          }}</span>
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
            :aria-pressed="form.channel_type === t.value"
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

      <el-form-item :label="urlLabel">
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
          <i v-if="!testing" class="fa-solid fa-paper-plane" /> Send test
        </el-button>
        <span v-if="testResult && testResult.ok" class="test-result ok" role="status">
          <i class="fa-solid fa-circle-check" /> Test message sent
        </span>
        <span v-else-if="testResult" class="test-result failed" role="status">
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
    help: "Channel ··· menu → Workflows → “Post to a channel when a webhook request is received”",
    placeholder: "https://....logic.azure.com/workflows/...",
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

// Switching type in edit mode makes the saved URL wrong (it belongs to the old
// provider), so the blank-keeps-current shortcut no longer applies.
const typeChanged = computed(
  () =>
    isEdit.value &&
    !!props.editChannel &&
    form.value.channel_type !== props.editChannel.channel_type,
);

const urlLabel = computed(() => {
  if (!isEdit.value) return "Webhook URL";
  if (typeChanged.value)
    return `Webhook URL (required — the saved URL belongs to ${channelTypeTitle(props.editChannel?.channel_type)})`;
  return "Webhook URL (leave blank to keep current)";
});

const urlPlaceholder = computed(() =>
  isEdit.value && !typeChanged.value
    ? "Leave blank to keep the saved URL"
    : activeType.value.placeholder,
);

function channelTypeTitle(type: NotificationChannelType | undefined): string {
  return CHANNEL_TYPES.find((t) => t.value === type)?.title ?? "the previous type";
}

const canTest = computed(() => form.value.webhook_url.trim().length > 0);

const isValid = computed(() => {
  if (!form.value.name.trim()) return false;
  // Editing may keep the stored URL — unless the type changed; creating always needs one.
  if (isEdit.value) return !typeChanged.value || form.value.webhook_url.trim().length > 0;
  return form.value.webhook_url.trim().length > 0;
});

function resetTest() {
  testSeq += 1; // a verdict for the previous URL/type no longer applies
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
    testSeq += 1; // discard any in-flight test from a previous open
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

// Guards a slow test response from resurfacing after the dialog is closed,
// reopened, or the URL edited in the meantime. `testSeq` invalidates verdicts;
// `lastRunSeq` lets the newest run (and only it) clear the spinner.
let testSeq = 0;
let lastRunSeq = 0;

async function runTest() {
  if (!canTest.value) return;
  const seq = ++testSeq;
  lastRunSeq = seq;
  testing.value = true;
  testResult.value = null;
  try {
    const result = await notificationsStore.testChannelUrl({
      channel_type: form.value.channel_type,
      webhook_url: form.value.webhook_url.trim(),
    });
    if (seq === testSeq && props.visible) testResult.value = result;
  } finally {
    if (seq === lastRunSeq) testing.value = false;
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
/* Dialog chrome, header, form rhythm and the type-card picker live in the
   shared `.catalog-view .catalog-dialog` block (CatalogView.vue), used by
   this dialog and CreateScheduleModal. Only channel-specific styles remain. */
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
  color: var(--color-success);
  font-weight: var(--font-weight-medium);
}
.test-result.failed {
  color: var(--color-danger);
}
.test-result.hint {
  color: var(--el-text-color-secondary);
}

/* Element Plus renders the button label in a flex span, which drops the
   whitespace between icon and text — restore the gap for icon+label buttons. */
.channel-dialog :deep(.el-button > span) {
  gap: 6px;
}
</style>
