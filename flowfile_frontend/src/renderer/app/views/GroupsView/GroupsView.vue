<template>
  <div class="groups-container">
    <div class="mb-3">
      <h2 class="page-title">User Groups</h2>
      <p class="page-description">
        A group is a named set of users you share resources with — secrets, connections, tables,
        flows, models, and visuals. Sharing a resource with a group grants it to every member at
        once. Only administrators can create groups.
      </p>
    </div>

    <!-- Roles explainer -->
    <button type="button" class="roles-toggle" @click="showRolesHelp = !showRolesHelp">
      <i :class="showRolesHelp ? 'fa-solid fa-chevron-down' : 'fa-solid fa-chevron-right'" />
      What do the member roles mean?
    </button>
    <div v-if="showRolesHelp" class="roles-help">
      <p class="roles-help-intro">
        A member's role controls <strong>group administration only</strong> — who can manage the
        group itself. It is separate from the <em>use</em>/<em>manage</em> permission a resource is
        shared at.
      </p>
      <ul class="roles-list">
        <li>
          <span class="role-pill role-owner">Owner</span>
          <span>
            Full control of the group: rename or delete it, and add, remove, or change the role of
            any member — including promoting or removing other owners. A group always keeps at least
            one owner.
          </span>
        </li>
        <li>
          <span class="role-pill role-manager">Manager</span>
          <span>
            Manages membership: add or remove members and set them to member or manager. Cannot
            promote/remove owners, rename, or delete the group.
          </span>
        </li>
        <li>
          <span class="role-pill role-member">Member</span>
          <span>
            Belongs to the group and can use everything shared with it. No group-administration
            abilities.
          </span>
        </li>
      </ul>
    </div>

    <!-- Add Group Card -->
    <div v-if="isAdmin" class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Add New Group</h3>
      </div>
      <div class="card-content">
        <form class="form" @submit.prevent="createGroup">
          <div class="form-grid">
            <div class="form-field">
              <label for="new-group-name" class="form-label">Name</label>
              <input
                id="new-group-name"
                v-model="newGroup.name"
                type="text"
                class="form-input"
                placeholder="data-team"
                required
              />
            </div>
            <div class="form-field">
              <label for="new-group-description" class="form-label">Description (optional)</label>
              <input
                id="new-group-description"
                v-model="newGroup.description"
                type="text"
                class="form-input"
                placeholder="What is this group for?"
              />
            </div>
          </div>
          <p v-if="createError" class="error-text">{{ createError }}</p>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary" :disabled="!newGroup.name || creating">
              <i class="fa-solid fa-plus"></i>
              {{ creating ? "Creating..." : "Create Group" }}
            </button>
          </div>
        </form>
      </div>
    </div>

    <!-- Groups List Card -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Groups ({{ groups.length }})</h3>
      </div>
      <div class="card-content">
        <div v-if="loading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading groups...</p>
        </div>

        <div v-else-if="groups.length === 0" class="empty-state">
          <i class="fa-solid fa-users"></i>
          <p class="empty-state__title">No groups yet</p>
          <p class="empty-state__subtitle">
            {{
              isAdmin
                ? "Create a group above to share resources with a set of users at once."
                : "An administrator can create groups and add you to them."
            }}
          </p>
        </div>

        <div v-else class="groups-table-container">
          <table class="groups-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Description</th>
                <th>Members</th>
                <th>My role</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="group in groups" :key="group.id">
                <td>
                  <div class="group-cell">
                    <i class="fa-solid fa-users"></i>
                    <span>{{ group.name }}</span>
                  </div>
                </td>
                <td>{{ group.description || "—" }}</td>
                <td>{{ group.member_count }}</td>
                <td>
                  <span v-if="group.my_role" :class="['role-pill', `role-${group.my_role}`]">
                    {{ group.my_role }}
                  </span>
                  <span v-else>—</span>
                </td>
                <td>
                  <div class="action-buttons">
                    <button
                      type="button"
                      class="btn btn-sm btn-secondary"
                      @click="openDetail(group.id)"
                    >
                      {{ canManage(group) ? "Manage" : "View" }}
                    </button>
                    <button
                      v-if="canAdminister(group)"
                      type="button"
                      class="btn btn-sm btn-danger"
                      title="Delete group"
                      @click="confirmDelete(group)"
                    >
                      <i class="fa-solid fa-trash-alt"></i>
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Group detail / member management -->
    <el-dialog
      v-model="showDetail"
      :title="detail ? `Group: ${detail.name}` : 'Group'"
      width="600px"
    >
      <div v-if="detail" v-loading="detailLoading">
        <p class="detail-roles-note">
          <strong>Owner</strong> runs the group, <strong>Manager</strong> manages members,
          <strong>Member</strong> just gets access to what's shared with the group.
        </p>
        <div v-if="canManageMembers" class="add-member">
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

const showRolesHelp = ref(false);
const loading = ref(false);
const groups = ref<UserGroup[]>([]);
const allUsers = ref<User[]>([]);

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

function canManage(group: UserGroup): boolean {
  return isAdmin.value || ["owner", "manager"].includes(group.my_role ?? "");
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

async function createGroup() {
  creating.value = true;
  createError.value = "";
  try {
    await UserGroupsApi.create({
      name: newGroup.value.name,
      description: newGroup.value.description,
    });
    newGroup.value = { name: "", description: "" };
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
.groups-container {
  padding: var(--spacing-4);
  max-width: 1200px;
  margin: 0 auto;
}
.roles-toggle {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  margin-bottom: 12px;
  padding: 0;
  background: none;
  border: none;
  color: var(--el-color-primary);
  font-size: 13px;
  cursor: pointer;
}
.roles-help {
  margin-bottom: 16px;
  padding: 12px 16px;
  background: var(--el-fill-color-light);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 8px;
}
.roles-help-intro {
  margin: 0 0 10px;
  font-size: 13px;
  color: var(--el-text-color-secondary);
}
.roles-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 10px;
}
.roles-list li {
  display: flex;
  gap: 10px;
  align-items: baseline;
  font-size: 13px;
  color: var(--el-text-color-regular);
  line-height: 1.45;
}
.role-pill {
  flex-shrink: 0;
  display: inline-block;
  width: 72px;
  text-align: center;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 600;
  text-transform: capitalize;
}
.role-owner {
  color: var(--el-color-warning);
  background: var(--el-color-warning-light-9);
}
.role-manager {
  color: var(--el-color-primary);
  background: var(--el-color-primary-light-9);
}
.role-member {
  color: var(--el-text-color-secondary);
  background: var(--el-fill-color);
}
.form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: var(--spacing-4);
}
.groups-table-container {
  overflow-x: auto;
}
.groups-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
}
.groups-table th,
.groups-table td {
  padding: var(--spacing-3);
  text-align: left;
  border-bottom: 1px solid var(--color-border-light);
}
.groups-table th {
  background-color: var(--color-background-muted);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}
.groups-table tr:hover {
  background-color: var(--color-background-hover);
}
.group-cell {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}
.group-cell i {
  color: var(--color-accent);
}
.action-buttons {
  display: flex;
  gap: var(--spacing-2);
}
.detail-roles-note {
  margin: 0 0 12px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  line-height: 1.5;
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
