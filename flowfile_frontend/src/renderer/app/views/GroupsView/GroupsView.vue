<template>
  <div class="groups-view">
    <div class="header">
      <div>
        <h2 class="page-title">User Groups</h2>
        <p class="page-description">
          Groups are who you share secrets, connections, and catalog resources with.
        </p>
      </div>
      <el-button v-if="isAdmin" type="primary" @click="openCreate">
        <i class="fa-solid fa-plus" />&nbsp;Create group
      </el-button>
    </div>

    <el-table v-loading="loading" :data="groups" class="groups-table" empty-text="No groups yet">
      <el-table-column prop="name" label="Name" min-width="160" />
      <el-table-column prop="description" label="Description" min-width="200" />
      <el-table-column label="Members" width="110">
        <template #default="{ row }">{{ row.member_count }}</template>
      </el-table-column>
      <el-table-column label="My role" width="120">
        <template #default="{ row }">
          <el-tag v-if="row.my_role" size="small">{{ row.my_role }}</el-tag>
          <span v-else>—</span>
        </template>
      </el-table-column>
      <el-table-column label="Actions" width="200">
        <template #default="{ row }">
          <el-button size="small" text type="primary" @click="openDetail(row.id)">Manage</el-button>
          <el-button
            v-if="canAdminister(row)"
            size="small"
            text
            type="danger"
            @click="confirmDelete(row)"
          >
            Delete
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- Create group -->
    <el-dialog v-model="showCreate" title="Create group" width="440px">
      <el-form label-position="top">
        <el-form-item label="Name">
          <el-input v-model="newGroup.name" placeholder="data-team" />
        </el-form-item>
        <el-form-item label="Description">
          <el-input v-model="newGroup.description" type="textarea" :rows="2" />
        </el-form-item>
      </el-form>
      <p v-if="createError" class="error-text">{{ createError }}</p>
      <template #footer>
        <el-button @click="showCreate = false">Cancel</el-button>
        <el-button
          type="primary"
          :loading="creating"
          :disabled="!newGroup.name"
          @click="createGroup"
        >
          Create
        </el-button>
      </template>
    </el-dialog>

    <!-- Group detail / member management -->
    <el-dialog
      v-model="showDetail"
      :title="detail ? `Group: ${detail.name}` : 'Group'"
      width="600px"
    >
      <div v-if="detail" v-loading="detailLoading">
        <div class="add-member">
          <el-select v-model="addUserId" placeholder="Add a user" filterable class="user-select">
            <el-option
              v-for="user in addableUsers"
              :key="user.id"
              :label="user.username"
              :value="user.id"
            />
          </el-select>
          <el-select v-model="addRole" class="role-select">
            <el-option label="Member" value="member" />
            <el-option label="Manager" value="manager" />
            <el-option v-if="canAdminister(detail)" label="Owner" value="owner" />
          </el-select>
          <el-button type="primary" :disabled="!addUserId" @click="addMember">Add</el-button>
        </div>
        <p v-if="detailError" class="error-text">{{ detailError }}</p>

        <el-table :data="detail.members" class="members-table">
          <el-table-column prop="username" label="User" min-width="140" />
          <el-table-column label="Role" width="160">
            <template #default="{ row }">
              <el-select
                v-if="canManageMembers"
                :model-value="row.role"
                size="small"
                @change="(role: string) => changeRole(row, role)"
              >
                <el-option label="Member" value="member" />
                <el-option label="Manager" value="manager" />
                <el-option v-if="canAdminister(detail)" label="Owner" value="owner" />
              </el-select>
              <el-tag v-else size="small">{{ row.role }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="" width="90">
            <template #default="{ row }">
              <el-button
                v-if="canManageMembers"
                size="small"
                text
                type="danger"
                @click="removeMember(row)"
              >
                Remove
              </el-button>
            </template>
          </el-table-column>
        </el-table>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ElMessage, ElMessageBox } from "element-plus";
import { computed, onMounted, ref } from "vue";
import { UserGroupsApi } from "../../api/userGroups.api";
import { useAuthStore } from "../../stores/auth-store";
import type { GroupRole, UserGroup, UserGroupDetail } from "../../types/sharing.types";
import userService, { type User } from "../../services/user.service";

const authStore = useAuthStore();
const isAdmin = computed(() => authStore.isAdmin);

const loading = ref(false);
const groups = ref<UserGroup[]>([]);
const allUsers = ref<User[]>([]);

const showCreate = ref(false);
const creating = ref(false);
const createError = ref("");
const newGroup = ref({ name: "", description: "" });

const showDetail = ref(false);
const detailLoading = ref(false);
const detailError = ref("");
const detail = ref<UserGroupDetail | null>(null);
const addUserId = ref<number | null>(null);
const addRole = ref<GroupRole>("member");

function canAdminister(group: UserGroup): boolean {
  return isAdmin.value || group.my_role === "owner";
}

const canManageMembers = computed(
  () =>
    !!detail.value && (isAdmin.value || ["owner", "manager"].includes(detail.value.my_role ?? "")),
);

const addableUsers = computed(() => {
  const existing = new Set((detail.value?.members ?? []).map((m) => m.user_id));
  return allUsers.value.filter((u) => !existing.has(u.id));
});

async function loadGroups() {
  loading.value = true;
  try {
    groups.value = await UserGroupsApi.list(isAdmin.value);
  } catch {
    ElMessage.error("Failed to load groups.");
  } finally {
    loading.value = false;
  }
}

function openCreate() {
  newGroup.value = { name: "", description: "" };
  createError.value = "";
  showCreate.value = true;
}

async function createGroup() {
  creating.value = true;
  createError.value = "";
  try {
    await UserGroupsApi.create({
      name: newGroup.value.name,
      description: newGroup.value.description,
    });
    showCreate.value = false;
    await loadGroups();
  } catch (e: any) {
    createError.value = e?.response?.data?.detail || "Failed to create group.";
  } finally {
    creating.value = false;
  }
}

async function confirmDelete(group: UserGroup) {
  try {
    await ElMessageBox.confirm(
      `Delete group "${group.name}"? This revokes all its grants.`,
      "Delete group",
      {
        type: "warning",
      },
    );
  } catch {
    return;
  }
  try {
    await UserGroupsApi.remove(group.id);
    await loadGroups();
  } catch (e: any) {
    ElMessage.error(e?.response?.data?.detail || "Failed to delete group.");
  }
}

async function openDetail(groupId: number) {
  showDetail.value = true;
  detailLoading.value = true;
  detailError.value = "";
  addUserId.value = null;
  addRole.value = "member";
  try {
    detail.value = await UserGroupsApi.get(groupId);
    if (allUsers.value.length === 0 && isAdmin.value) {
      allUsers.value = await userService.getUsers();
    }
  } catch (e: any) {
    detailError.value = e?.response?.data?.detail || "Failed to load group.";
  } finally {
    detailLoading.value = false;
  }
}

async function refreshDetail() {
  if (detail.value) detail.value = await UserGroupsApi.get(detail.value.id);
}

async function addMember() {
  if (!detail.value || !addUserId.value) return;
  try {
    await UserGroupsApi.addMember(detail.value.id, addUserId.value, addRole.value);
    addUserId.value = null;
    await refreshDetail();
  } catch (e: any) {
    detailError.value = e?.response?.data?.detail || "Failed to add member.";
  }
}

async function changeRole(member: { user_id: number }, role: string) {
  if (!detail.value) return;
  try {
    await UserGroupsApi.updateMember(detail.value.id, member.user_id, role as GroupRole);
    await refreshDetail();
  } catch (e: any) {
    detailError.value = e?.response?.data?.detail || "Failed to change role.";
    await refreshDetail();
  }
}

async function removeMember(member: { user_id: number; username: string }) {
  if (!detail.value) return;
  try {
    await UserGroupsApi.removeMember(detail.value.id, member.user_id);
    await refreshDetail();
  } catch (e: any) {
    detailError.value = e?.response?.data?.detail || "Failed to remove member.";
  }
}

onMounted(() => {
  void loadGroups();
});
</script>

<style scoped>
.groups-view {
  padding: 16px 24px;
}
.header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 16px;
}
.page-title {
  margin: 0;
}
.page-description {
  margin: 4px 0 0;
  color: var(--el-text-color-secondary);
  font-size: 13px;
}
.groups-table {
  width: 100%;
}
.add-member {
  display: flex;
  gap: 8px;
  margin-bottom: 12px;
}
.user-select {
  flex: 1;
}
.role-select {
  width: 130px;
}
.members-table {
  width: 100%;
}
.error-text {
  color: var(--el-color-danger);
  font-size: 12px;
  margin: 6px 0;
}
</style>
