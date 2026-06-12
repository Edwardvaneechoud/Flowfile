<template>
  <el-dialog
    :model-value="modelValue"
    :title="`Share ${resourceLabel}`"
    width="540px"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <div v-loading="loading" class="share-dialog">
      <p class="resource-name">
        <i class="fa-solid fa-share-nodes"></i>
        <strong>{{ resourceName }}</strong>
      </p>

      <el-alert
        v-if="resourceType === 'secret'"
        type="warning"
        :closable="false"
        show-icon
        title="Sharing a secret lets the group's flows use this credential at run time. Members can never view its value, but their flows can use it."
        class="mb-2"
      />

      <!-- Add a new share -->
      <div class="add-share">
        <el-select
          v-model="selectedGroupId"
          placeholder="Select a group"
          class="group-select"
          filterable
          :no-data-text="noGroupsText"
        >
          <el-option
            v-for="group in availableGroups"
            :key="group.id"
            :label="group.name"
            :value="group.id"
          />
        </el-select>
        <el-button
          type="primary"
          :disabled="!selectedGroupId || adding"
          :loading="adding"
          @click="addShare"
        >
          Share
        </el-button>
      </div>

      <!-- Permission picker with plain-language explanation -->
      <div v-if="resourceType !== 'secret'" class="permission-row">
        <el-radio-group v-model="selectedPermission" size="small">
          <el-radio-button value="use">Use</el-radio-button>
          <el-radio-button v-if="canChooseManage" value="manage">Manage</el-radio-button>
        </el-radio-group>
        <span class="permission-help">{{ permissionHelp }}</span>
      </div>

      <p v-if="error" class="error-text">{{ error }}</p>

      <!-- Existing shares -->
      <el-divider />
      <p class="section-label">Shared with</p>
      <div v-if="shares.length === 0" class="empty">Not shared with any group yet.</div>
      <ul v-else class="share-list">
        <li v-for="share in shares" :key="share.id" class="share-row">
          <span class="group-name">
            <i class="fa-solid fa-user-group"></i>
            {{ share.group_name }}
          </span>
          <span class="permission-tag">
            <el-tag size="small" :type="share.permission === 'manage' ? 'warning' : 'info'">
              {{ share.permission === "manage" ? "Manage" : "Use" }}
            </el-tag>
          </span>
          <el-button
            text
            type="danger"
            size="small"
            :aria-label="`Revoke ${share.group_name}`"
            @click="removeShare(share)"
          >
            Revoke
          </el-button>
        </li>
      </ul>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from "vue";
import { UserGroupsApi } from "../../api/userGroups.api";
import { useSharingStore } from "../../stores/sharing-store";
import { useAuthStore } from "../../stores/auth-store";
import type { PermissionLevel, ResourceType, Share, UserGroup } from "../../types/sharing.types";

const props = withDefaults(
  defineProps<{
    modelValue: boolean;
    resourceType: ResourceType;
    resourceId: number;
    resourceName: string;
    // True when the current user owns the resource or is an admin: only then can
    // they mint manage-level grants (the backend enforces this too).
    canManageGrants?: boolean;
  }>(),
  { canManageGrants: false },
);

const emit = defineEmits<{ (e: "update:modelValue", value: boolean): void }>();

const sharing = useSharingStore();
const authStore = useAuthStore();

const loading = ref(false);
const adding = ref(false);
const error = ref("");
const shares = ref<Share[]>([]);
const groups = ref<UserGroup[]>([]);
const selectedGroupId = ref<number | null>(null);
const selectedPermission = ref<PermissionLevel>("use");

const RESOURCE_LABELS: Record<ResourceType, string> = {
  secret: "secret",
  database_connection: "database connection",
  cloud_connection: "cloud connection",
  ga_connection: "Google Analytics connection",
  kafka_connection: "Kafka connection",
  catalog_namespace: "namespace",
  catalog_table: "table",
  flow: "flow",
  visualization: "visualization",
  dashboard: "dashboard",
  global_artifact: "model",
};

const resourceLabel = computed(() => RESOURCE_LABELS[props.resourceType]);
// Secrets are use-only; managers can only re-share at use level (backend rejects
// the rest), so the manage option is meaningful only for owners/admins.
const canChooseManage = computed(() => props.resourceType !== "secret" && props.canManageGrants);

const availableGroups = computed(() =>
  groups.value.filter((group) => !shares.value.some((s) => s.group_id === group.id)),
);

const noGroupsText = computed(() => {
  // Groups exist but every one already has access (vs. genuinely being in none).
  if (groups.value.length > 0) {
    return "Already shared with all available groups";
  }
  return authStore.isAdmin
    ? "No groups exist yet — create one in User Groups"
    : "You are not in any groups";
});

const permissionHelp = computed(() => {
  const noun = resourceLabel.value;
  return selectedPermission.value === "manage"
    ? `Members can use this ${noun} in flows, and also edit and re-share it.`
    : `Members can use this ${noun} in their flows. They cannot edit or re-share it.`;
});

async function onOpen() {
  error.value = "";
  selectedGroupId.value = null;
  selectedPermission.value = "use";
  loading.value = true;
  try {
    // Fetch groups and existing shares in parallel — they're independent.
    // Admins can share to ANY group, so list all; everyone else lists their own.
    // Always fresh — a group created after the page loaded must show up.
    const [groupList, shareList] = await Promise.all([
      authStore.isAdmin ? UserGroupsApi.list(true) : sharing.loadMyGroups(true),
      sharing.listShares(props.resourceType, props.resourceId),
    ]);
    groups.value = groupList;
    shares.value = shareList;
  } catch (e: any) {
    error.value = e?.response?.data?.detail || "Failed to load sharing info.";
  } finally {
    loading.value = false;
  }
}

// Parents mount this dialog with v-if AND open it in the same tick, so el-dialog's
// @open never fires on that initial already-open mount. Load on modelValue instead
// (immediate covers the mount-while-open case; reloads on each subsequent open).
watch(
  () => props.modelValue,
  (open) => {
    if (open) void onOpen();
  },
  { immediate: true },
);

async function addShare() {
  if (!selectedGroupId.value) return;
  adding.value = true;
  error.value = "";
  try {
    const created = await sharing.createShare({
      resource_type: props.resourceType,
      resource_id: props.resourceId,
      group_id: selectedGroupId.value,
      permission: canChooseManage.value ? selectedPermission.value : "use",
    });
    shares.value.push(created);
    selectedGroupId.value = null;
  } catch (e: any) {
    error.value = e?.response?.data?.detail || "Failed to share.";
  } finally {
    adding.value = false;
  }
}

async function removeShare(share: Share) {
  try {
    await sharing.removeShare(share.id);
    shares.value = shares.value.filter((s) => s.id !== share.id);
  } catch (e: any) {
    error.value = e?.response?.data?.detail || "Failed to revoke.";
  }
}
</script>

<style scoped>
.share-dialog {
  min-height: 120px;
}
.resource-name {
  margin: 0 0 12px;
  font-size: 14px;
}
.resource-name i {
  margin-right: 6px;
  color: var(--el-color-primary);
}
.add-share {
  display: flex;
  gap: 8px;
  align-items: center;
}
.group-select {
  flex: 1;
}
.permission-row {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 6px;
  margin-top: 12px;
}
/* Keep Use|Manage as a horizontal segmented control, never wrapping. */
.permission-row :deep(.el-radio-group) {
  flex-wrap: nowrap;
}
.permission-help {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  line-height: 1.4;
}
.section-label {
  font-weight: 600;
  margin: 0 0 8px;
  font-size: 13px;
}
.share-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.share-row {
  display: flex;
  align-items: center;
  gap: 10px;
}
.group-name {
  flex: 1;
  font-size: 13px;
}
.group-name i {
  margin-right: 6px;
  color: var(--el-text-color-secondary);
}
.empty {
  color: var(--el-text-color-secondary);
  font-size: 13px;
}
.error-text {
  color: var(--el-color-danger);
  font-size: 12px;
  margin: 6px 0 0;
}
.mb-2 {
  margin-bottom: 12px;
}
</style>
