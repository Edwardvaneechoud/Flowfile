<template>
  <div class="admin-container">
    <!-- Status Message -->
    <Transition name="fade">
      <div v-if="statusMessage" :class="['status-message', `status-${statusMessage.type}`]">
        <i :class="statusMessage.type === 'success' ? 'fa-solid fa-check-circle' : 'fa-solid fa-exclamation-circle'"></i>
        <span>{{ statusMessage.text }}</span>
      </div>
    </Transition>

    <div class="mb-3">
      <h2 class="page-title">User Management</h2>
      <p class="page-description">Manage users and their permissions</p>
    </div>

    <!-- Add User Card -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Add New User</h3>
      </div>
      <div class="card-content">
        <form class="form" @submit.prevent="handleAddUser">
          <div class="form-grid">
            <div class="form-field">
              <label for="new-username" class="form-label">Username</label>
              <input
                id="new-username"
                v-model="newUser.username"
                type="text"
                class="form-input"
                placeholder="Enter username"
                required
              />
            </div>

            <div class="form-field">
              <label for="new-password" class="form-label">Password</label>
              <div class="password-field">
                <input
                  id="new-password"
                  v-model="newUser.password"
                  :type="showNewPassword ? 'text' : 'password'"
                  class="form-input"
                  placeholder="Enter password"
                  required
                />
                <button
                  type="button"
                  class="toggle-visibility"
                  aria-label="Toggle password visibility"
                  @click="showNewPassword = !showNewPassword"
                >
                  <i :class="showNewPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
                </button>
              </div>
              <ul v-if="newUser.password" class="password-requirements">
                <li :class="{ valid: passwordChecks.minLength }">
                  <i :class="passwordChecks.minLength ? 'fa-solid fa-check' : 'fa-solid fa-times'"></i>
                  8+ characters
                </li>
                <li :class="{ valid: passwordChecks.hasNumber }">
                  <i :class="passwordChecks.hasNumber ? 'fa-solid fa-check' : 'fa-solid fa-times'"></i>
                  Number
                </li>
                <li :class="{ valid: passwordChecks.hasSpecial }">
                  <i :class="passwordChecks.hasSpecial ? 'fa-solid fa-check' : 'fa-solid fa-times'"></i>
                  Special char
                </li>
              </ul>
            </div>

            <div class="form-field">
              <label for="new-email" class="form-label">Email (optional)</label>
              <input
                id="new-email"
                v-model="newUser.email"
                type="email"
                class="form-input"
                placeholder="user@example.com"
              />
            </div>

            <div class="form-field">
              <label for="new-fullname" class="form-label">Full Name (optional)</label>
              <input
                id="new-fullname"
                v-model="newUser.full_name"
                type="text"
                class="form-input"
                placeholder="John Doe"
              />
            </div>

            <div class="form-field checkbox-field">
              <label class="checkbox-label">
                <input
                  v-model="newUser.is_admin"
                  type="checkbox"
                  class="form-checkbox"
                />
                <span>Administrator</span>
              </label>
            </div>
          </div>

          <div class="form-actions">
            <button
              type="submit"
              class="btn btn-primary"
              :disabled="!newUser.username || !isPasswordValid || isSubmitting"
            >
              <i class="fa-solid fa-user-plus"></i>
              {{ isSubmitting ? "Creating..." : "Create User" }}
            </button>
          </div>
        </form>
      </div>
    </div>

    <!-- Users List Card -->
    <div class="card mb-3">
      <div class="card-header">
        <h3 class="card-title">Users ({{ filteredUsers.length }})</h3>
        <div v-if="users.length > 0" class="search-container">
          <input
            v-model="searchTerm"
            type="text"
            placeholder="Search users..."
            class="search-input"
            aria-label="Search users"
          />
          <i class="fa-solid fa-search search-icon"></i>
        </div>
      </div>
      <div class="card-content">
        <div v-if="isLoading" class="loading-state">
          <div class="loading-spinner"></div>
          <p>Loading users...</p>
        </div>

        <div v-else-if="!isLoading && users.length === 0" class="empty-state">
          <i class="fa-solid fa-users"></i>
          <p>No users found</p>
        </div>

        <!-- Users Table -->
        <div v-else-if="filteredUsers.length > 0" class="users-table-container">
          <table class="users-table">
            <thead>
              <tr>
                <th>Username</th>
                <th>Email</th>
                <th>Full Name</th>
                <th>Admin</th>
                <th>Status</th>
                <th>Password</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="user in filteredUsers" :key="user.id" :class="{ 'disabled-row': user.disabled }">
                <td>
                  <div class="user-cell">
                    <i class="fa-solid fa-user"></i>
                    <span>{{ user.username }}</span>
                  </div>
                </td>
                <td>{{ user.email || '-' }}</td>
                <td>{{ user.full_name || '-' }}</td>
                <td>
                  <span :class="['badge', user.is_admin ? 'badge-primary' : 'badge-secondary']">
                    {{ user.is_admin ? 'Admin' : 'User' }}
                  </span>
                </td>
                <td>
                  <span :class="['badge', user.disabled ? 'badge-danger' : 'badge-success']">
                    {{ user.disabled ? 'Disabled' : 'Active' }}
                  </span>
                </td>
                <td>
                  <span v-if="user.must_change_password" class="badge badge-warning">
                    <i class="fa-solid fa-exclamation-triangle"></i> Must Change
                  </span>
                  <span v-else class="badge badge-muted">
                    <i class="fa-solid fa-check"></i> OK
                  </span>
                </td>
                <td>
                  <div class="action-buttons">
                    <button
                      type="button"
                      class="btn btn-sm btn-secondary"
                      title="Edit user"
                      @click="openEditModal(user)"
                    >
                      <i class="fa-solid fa-edit"></i>
                    </button>
                    <button
                      v-if="!user.must_change_password"
                      type="button"
                      class="btn btn-sm btn-warning"
                      title="Force password change"
                      @click="handleForcePasswordChange(user)"
                    >
                      <i class="fa-solid fa-key"></i>
                    </button>
                    <button
                      v-if="user.id !== currentUserId"
                      type="button"
                      class="btn btn-sm btn-danger"
                      title="Delete user"
                      @click="openDeleteModal(user)"
                    >
                      <i class="fa-solid fa-trash-alt"></i>
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- No Results State -->
        <div v-else class="empty-state">
          <i class="fa-solid fa-search"></i>
          <p>No users found matching "{{ searchTerm }}"</p>
        </div>
      </div>
    </div>

    <!-- Edit User Modal -->
    <div v-if="showEditModal" class="modal-overlay" @click="closeEditModal">
      <div class="modal-container" @click.stop>
        <div class="modal-header">
          <h3 class="modal-title">Edit User: {{ editUser?.username }}</h3>
          <button class="modal-close" aria-label="Close" @click="closeEditModal">
            <i class="fa-solid fa-times"></i>
          </button>
        </div>
        <div class="modal-content">
          <form @submit.prevent="handleUpdateUser">
            <div class="form-field">
              <label class="form-label">Email</label>
              <input
                v-model="editFormData.email"
                type="email"
                class="form-input"
                placeholder="user@example.com"
              />
            </div>

            <div class="form-field">
              <label class="form-label">Full Name</label>
              <input
                v-model="editFormData.full_name"
                type="text"
                class="form-input"
                placeholder="John Doe"
              />
            </div>

            <div class="form-field">
              <label class="form-label">New Password (leave blank to keep current)</label>
              <div class="password-field">
                <input
                  v-model="editFormData.password"
                  :type="showEditPassword ? 'text' : 'password'"
                  class="form-input"
                  placeholder="Enter new password"
                />
                <button
                  type="button"
                  class="toggle-visibility"
                  aria-label="Toggle password visibility"
                  @click="showEditPassword = !showEditPassword"
                >
                  <i :class="showEditPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
                </button>
              </div>
            </div>

            <div class="form-field checkbox-field" v-if="editUser?.id !== currentUserId">
              <label class="checkbox-label">
                <input
                  v-model="editFormData.is_admin"
                  type="checkbox"
                  class="form-checkbox"
                />
                <span>Administrator</span>
              </label>
            </div>

            <div class="form-field checkbox-field" v-if="editUser?.id !== currentUserId">
              <label class="checkbox-label">
                <input
                  v-model="editFormData.disabled"
                  type="checkbox"
                  class="form-checkbox"
                />
                <span>Disabled</span>
              </label>
            </div>
          </form>
        </div>
        <div class="modal-actions">
          <button class="btn btn-secondary" @click="closeEditModal">Cancel</button>
          <button class="btn btn-primary" :disabled="isUpdating" @click="handleUpdateUser">
            <i v-if="isUpdating" class="fas fa-spinner fa-spin"></i>
            {{ isUpdating ? "Saving..." : "Save Changes" }}
          </button>
        </div>
      </div>
    </div>

    <!-- Delete Confirmation Modal -->
    <div v-if="showDeleteModal" class="modal-overlay" @click="closeDeleteModal">
      <div class="modal-container" @click.stop>
        <div class="modal-header">
          <h3 class="modal-title">Delete User</h3>
          <button class="modal-close" aria-label="Close" @click="closeDeleteModal">
            <i class="fa-solid fa-times"></i>
          </button>
        </div>
        <div class="modal-content">
          <p>
            Are you sure you want to delete the user <strong>{{ userToDelete?.username }}</strong>?
          </p>
          <p class="warning-text">
            This will also delete all their secrets, database connections, and cloud connections.
            This action cannot be undone.
          </p>
        </div>
        <div class="modal-actions">
          <button class="btn btn-secondary" @click="closeDeleteModal">Cancel</button>
          <button class="btn btn-danger-filled" :disabled="isDeleting" @click="handleDeleteUser">
            <i v-if="isDeleting" class="fas fa-spinner fa-spin"></i>
            {{ isDeleting ? "Deleting..." : "Delete User" }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from "vue";
import { useAuthStore } from "../../stores/auth-store";
import userService, { type User, type UserCreate, type UserUpdate } from "../../services/user.service";

const authStore = useAuthStore();
const currentUserId = computed(() => authStore.currentUser?.id);

// State
const users = ref<User[]>([]);
const isLoading = ref(false);
const searchTerm = ref("");

// New user form
const newUser = ref<UserCreate>({
  username: "",
  password: "",
  email: "",
  full_name: "",
  is_admin: false,
});
const showNewPassword = ref(false);
const isSubmitting = ref(false);

// Password validation
const passwordChecks = computed(() => ({
  minLength: newUser.value.password.length >= 8,
  hasNumber: /\d/.test(newUser.value.password),
  hasSpecial: /[!@#$%^&*()_+\-=[\]{}|;:,.<>?]/.test(newUser.value.password),
}));

const isPasswordValid = computed(() =>
  passwordChecks.value.minLength &&
  passwordChecks.value.hasNumber &&
  passwordChecks.value.hasSpecial
);

// Edit modal
const showEditModal = ref(false);
const editUser = ref<User | null>(null);
const editFormData = ref<UserUpdate>({});
const showEditPassword = ref(false);
const isUpdating = ref(false);

// Delete modal
const showDeleteModal = ref(false);
const userToDelete = ref<User | null>(null);
const isDeleting = ref(false);

// Computed
const filteredUsers = computed(() => {
  if (!searchTerm.value) return users.value;
  const term = searchTerm.value.toLowerCase();
  return users.value.filter(
    (user) =>
      user.username.toLowerCase().includes(term) ||
      user.email?.toLowerCase().includes(term) ||
      user.full_name?.toLowerCase().includes(term)
  );
});

// Load users
const loadUsers = async () => {
  isLoading.value = true;
  try {
    users.value = await userService.getUsers();
  } catch (error) {
    console.error("Failed to load users:", error);
    showStatus('error', "Failed to load users. Please try again.");
  } finally {
    isLoading.value = false;
  }
};

// Status message state
const statusMessage = ref<{ type: 'success' | 'error'; text: string } | null>(null);

const showStatus = (type: 'success' | 'error', text: string) => {
  statusMessage.value = { type, text };
  setTimeout(() => { statusMessage.value = null; }, 4000);
};

const getErrorMessage = (error: unknown): string => {
  const axiosError = error as { response?: { data?: { detail?: string } } };
  return axiosError.response?.data?.detail || (error instanceof Error ? error.message : "An error occurred");
};

// Create user
const handleAddUser = async () => {
  if (!newUser.value.username || !newUser.value.password) return;

  isSubmitting.value = true;
  try {
    await userService.createUser(newUser.value);
    newUser.value = { username: "", password: "", email: "", full_name: "", is_admin: false };
    showNewPassword.value = false;
    await loadUsers();
    showStatus('success', "User created successfully");
  } catch (error: unknown) {
    showStatus('error', getErrorMessage(error));
  } finally {
    isSubmitting.value = false;
  }
};

// Edit modal
const openEditModal = (user: User) => {
  editUser.value = user;
  editFormData.value = {
    email: user.email || "",
    full_name: user.full_name || "",
    is_admin: user.is_admin,
    disabled: user.disabled,
    password: "",
  };
  showEditModal.value = true;
};

const closeEditModal = () => {
  showEditModal.value = false;
  editUser.value = null;
  editFormData.value = {};
  showEditPassword.value = false;
};

const handleUpdateUser = async () => {
  if (!editUser.value) return;

  isUpdating.value = true;
  try {
    const updateData: UserUpdate = {};
    if (editFormData.value.email !== editUser.value.email) {
      updateData.email = editFormData.value.email;
    }
    if (editFormData.value.full_name !== editUser.value.full_name) {
      updateData.full_name = editFormData.value.full_name;
    }
    if (editFormData.value.is_admin !== editUser.value.is_admin) {
      updateData.is_admin = editFormData.value.is_admin;
    }
    if (editFormData.value.disabled !== editUser.value.disabled) {
      updateData.disabled = editFormData.value.disabled;
    }
    if (editFormData.value.password) {
      updateData.password = editFormData.value.password;
    }

    await userService.updateUser(editUser.value.id, updateData);
    closeEditModal();
    await loadUsers();
    showStatus('success', "User updated successfully");
  } catch (error: unknown) {
    showStatus('error', getErrorMessage(error));
  } finally {
    isUpdating.value = false;
  }
};

// Delete modal
const openDeleteModal = (user: User) => {
  userToDelete.value = user;
  showDeleteModal.value = true;
};

const closeDeleteModal = () => {
  showDeleteModal.value = false;
  userToDelete.value = null;
};

const handleDeleteUser = async () => {
  if (!userToDelete.value) return;

  isDeleting.value = true;
  try {
    await userService.deleteUser(userToDelete.value.id);
    closeDeleteModal();
    await loadUsers();
    showStatus('success', "User deleted successfully");
  } catch (error: unknown) {
    showStatus('error', getErrorMessage(error));
  } finally {
    isDeleting.value = false;
  }
};

// Force password change
const handleForcePasswordChange = async (user: User) => {
  try {
    await userService.updateUser(user.id, { must_change_password: true });
    await loadUsers();
    showStatus('success', `${user.username} will be required to change password on next login`);
  } catch (error: unknown) {
    showStatus('error', getErrorMessage(error));
  }
};

// Load users on mount
onMounted(() => {
  loadUsers();
});
</script>

<style scoped>
.admin-container {
  padding: var(--spacing-4);
  max-width: 1200px;
  margin: 0 auto;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: var(--spacing-4);
}

.checkbox-field {
  display: flex;
  align-items: center;
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  cursor: pointer;
}

.form-checkbox {
  width: 18px;
  height: 18px;
  cursor: pointer;
}

.users-table-container {
  overflow-x: auto;
}

.users-table {
  width: 100%;
  border-collapse: collapse;
  font-size: var(--font-size-sm);
}

.users-table th,
.users-table td {
  padding: var(--spacing-3);
  text-align: left;
  border-bottom: 1px solid var(--color-border-light);
}

.users-table th {
  background-color: var(--color-background-muted);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.users-table tr:hover {
  background-color: var(--color-background-hover);
}

.users-table .disabled-row {
  opacity: 0.6;
}

.user-cell {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.user-cell i {
  color: var(--color-accent);
}

.action-buttons {
  display: flex;
  gap: var(--spacing-2);
}

.password-field {
  position: relative;
  display: flex;
  align-items: center;
}

.password-field .form-input {
  padding-right: 40px;
}

.toggle-visibility {
  position: absolute;
  right: 8px;
  background: none;
  border: none;
  cursor: pointer;
  color: var(--color-text-secondary);
  padding: 4px;
}

.toggle-visibility:hover {
  color: var(--color-text-primary);
}

.password-requirements {
  list-style: none;
  padding: 0;
  margin: var(--spacing-2) 0 0 0;
  font-size: var(--font-size-xs);
  display: flex;
  gap: var(--spacing-3);
}

.password-requirements li {
  display: flex;
  align-items: center;
  gap: var(--spacing-1);
  color: var(--color-text-secondary);
}

.password-requirements li i {
  width: 12px;
  font-size: 10px;
  color: var(--color-danger);
}

.password-requirements li.valid {
  color: var(--color-success);
}

.password-requirements li.valid i {
  color: var(--color-success);
}

/* Uses centralized .status-message from _modals.css, just add margin */
.status-message {
  margin-bottom: var(--spacing-4);
}

.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}
</style>
