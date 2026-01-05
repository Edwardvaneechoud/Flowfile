<template>
  <Teleport to="body">
    <div v-if="show" class="modal-overlay" @click.self="handleBackdropClick">
      <div class="modal-container">
        <div class="modal-header">
          <h3 class="modal-title">
            <i class="fa-solid fa-key"></i>
            {{ isForced ? 'Password Change Required' : 'Change Password' }}
          </h3>
          <button v-if="!isForced" class="modal-close" aria-label="Close" @click="close">
            <i class="fa-solid fa-times"></i>
          </button>
        </div>

        <div class="modal-content">
          <p v-if="isForced" class="warning-text">
            You must change your password before continuing. This is required for security purposes.
          </p>

          <form class="form" @submit.prevent="handleSubmit">
            <div class="form-field">
              <label for="current-password" class="form-label">Current Password</label>
              <div class="password-field">
                <input
                  id="current-password"
                  v-model="formData.currentPassword"
                  :type="showCurrentPassword ? 'text' : 'password'"
                  class="form-input"
                  :class="{ 'is-error': errors.currentPassword }"
                  placeholder="Enter current password"
                  required
                />
                <button
                  type="button"
                  class="toggle-visibility"
                  aria-label="Toggle password visibility"
                  @click="showCurrentPassword = !showCurrentPassword"
                >
                  <i :class="showCurrentPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
                </button>
              </div>
              <span v-if="errors.currentPassword" class="form-error">{{ errors.currentPassword }}</span>
            </div>

            <div class="form-field">
              <label for="new-password" class="form-label">New Password</label>
              <div class="password-field">
                <input
                  id="new-password"
                  v-model="formData.newPassword"
                  :type="showNewPassword ? 'text' : 'password'"
                  class="form-input"
                  :class="{ 'is-error': errors.newPassword }"
                  placeholder="Enter new password (min 6 characters)"
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
              <span v-if="errors.newPassword" class="form-error">{{ errors.newPassword }}</span>
              <span class="form-hint">Password must be at least 6 characters</span>
            </div>

            <div class="form-field">
              <label for="confirm-password" class="form-label">Confirm New Password</label>
              <div class="password-field">
                <input
                  id="confirm-password"
                  v-model="formData.confirmPassword"
                  :type="showConfirmPassword ? 'text' : 'password'"
                  class="form-input"
                  :class="{ 'is-error': errors.confirmPassword }"
                  placeholder="Confirm new password"
                  required
                />
                <button
                  type="button"
                  class="toggle-visibility"
                  aria-label="Toggle password visibility"
                  @click="showConfirmPassword = !showConfirmPassword"
                >
                  <i :class="showConfirmPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
                </button>
              </div>
              <span v-if="errors.confirmPassword" class="form-error">{{ errors.confirmPassword }}</span>
            </div>

            <div v-if="serverError" class="form-field">
              <div class="warning-text">{{ serverError }}</div>
            </div>
          </form>
        </div>

        <div class="modal-actions">
          <button v-if="!isForced" class="btn btn-secondary" @click="close">Cancel</button>
          <button
            class="btn btn-primary"
            :disabled="isSubmitting || !isFormValid"
            @click="handleSubmit"
          >
            <i v-if="isSubmitting" class="fa-solid fa-spinner fa-spin"></i>
            {{ isSubmitting ? 'Changing...' : 'Change Password' }}
          </button>
        </div>
      </div>
    </div>
  </Teleport>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue';
import axios from 'axios';

const props = defineProps<{
  show: boolean;
  isForced?: boolean;
}>();

const emit = defineEmits<{
  (e: 'close'): void;
  (e: 'success'): void;
}>();

const formData = ref({
  currentPassword: '',
  newPassword: '',
  confirmPassword: '',
});

const errors = ref({
  currentPassword: '',
  newPassword: '',
  confirmPassword: '',
});

const showCurrentPassword = ref(false);
const showNewPassword = ref(false);
const showConfirmPassword = ref(false);
const isSubmitting = ref(false);
const serverError = ref('');

const isFormValid = computed(() => {
  return (
    formData.value.currentPassword.length > 0 &&
    formData.value.newPassword.length >= 6 &&
    formData.value.confirmPassword === formData.value.newPassword
  );
});

// Reset form when modal opens/closes
watch(() => props.show, (newVal) => {
  if (newVal) {
    resetForm();
  }
});

const resetForm = () => {
  formData.value = { currentPassword: '', newPassword: '', confirmPassword: '' };
  errors.value = { currentPassword: '', newPassword: '', confirmPassword: '' };
  showCurrentPassword.value = false;
  showNewPassword.value = false;
  showConfirmPassword.value = false;
  serverError.value = '';
};

const validateForm = (): boolean => {
  errors.value = { currentPassword: '', newPassword: '', confirmPassword: '' };
  let isValid = true;

  if (!formData.value.currentPassword) {
    errors.value.currentPassword = 'Current password is required';
    isValid = false;
  }

  if (!formData.value.newPassword) {
    errors.value.newPassword = 'New password is required';
    isValid = false;
  } else if (formData.value.newPassword.length < 6) {
    errors.value.newPassword = 'Password must be at least 6 characters';
    isValid = false;
  }

  if (!formData.value.confirmPassword) {
    errors.value.confirmPassword = 'Please confirm your new password';
    isValid = false;
  } else if (formData.value.confirmPassword !== formData.value.newPassword) {
    errors.value.confirmPassword = 'Passwords do not match';
    isValid = false;
  }

  return isValid;
};

const handleSubmit = async () => {
  if (!validateForm()) return;

  isSubmitting.value = true;
  serverError.value = '';

  try {
    await axios.post('/auth/users/me/change-password', {
      current_password: formData.value.currentPassword,
      new_password: formData.value.newPassword,
    });

    emit('success');
    close();
  } catch (error: unknown) {
    const axiosError = error as { response?: { data?: { detail?: string } } };
    serverError.value = axiosError.response?.data?.detail || 'Failed to change password';
  } finally {
    isSubmitting.value = false;
  }
};

const handleBackdropClick = () => {
  if (!props.isForced) {
    close();
  }
};

const close = () => {
  if (!props.isForced) {
    emit('close');
  }
};
</script>

<style scoped>
/* Use global styles from _modals.css and _forms.css */
/* Only add component-specific styles here */

.modal-title i {
  margin-right: var(--spacing-2);
  color: var(--color-accent);
}

.form {
  margin-top: var(--spacing-2);
}
</style>
