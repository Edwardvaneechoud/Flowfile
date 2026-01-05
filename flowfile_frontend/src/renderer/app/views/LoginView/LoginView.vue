<template>
  <div class="login-container">
    <div class="login-card">
      <div class="login-header">
        <div class="logo-container">
          <img src="/flowfile_logo.png" alt="Flowfile" class="logo" />
        </div>
        <h1 class="login-title">Welcome to Flowfile</h1>
        <p class="login-subtitle">Sign in to continue</p>
      </div>

      <form class="login-form" @submit.prevent="handleLogin">
        <div v-if="error" class="error-message">
          <i class="fa-solid fa-circle-exclamation"></i>
          <span>{{ error }}</span>
        </div>

        <div class="form-field">
          <label for="username" class="form-label">Username</label>
          <div class="input-wrapper">
            <i class="fa-solid fa-user input-icon"></i>
            <input
              id="username"
              v-model="username"
              type="text"
              class="form-input with-icon"
              placeholder="Enter your username"
              required
              autocomplete="username"
              :disabled="isLoading"
            />
          </div>
        </div>

        <div class="form-field">
          <label for="password" class="form-label">Password</label>
          <div class="input-wrapper">
            <i class="fa-solid fa-lock input-icon"></i>
            <input
              id="password"
              v-model="password"
              :type="showPassword ? 'text' : 'password'"
              class="form-input with-icon"
              placeholder="Enter your password"
              required
              autocomplete="current-password"
              :disabled="isLoading"
            />
            <button
              type="button"
              class="toggle-password"
              tabindex="-1"
              @click="showPassword = !showPassword"
            >
              <i :class="showPassword ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
            </button>
          </div>
        </div>

        <button
          type="submit"
          class="btn btn-primary login-button"
          :disabled="isLoading || !username || !password"
        >
          <span v-if="isLoading" class="loading-spinner"></span>
          <span v-else>Sign In</span>
        </button>
      </form>

      <div class="login-footer">
        <p class="footer-text">
          Flowfile - Visual Data Processing
        </p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useAuthStore } from '../../stores/auth-store'

const router = useRouter()
const authStore = useAuthStore()

const username = ref('')
const password = ref('')
const showPassword = ref(false)
const isLoading = ref(false)
const error = ref('')

const handleLogin = async () => {
  if (!username.value || !password.value) {
    return
  }

  isLoading.value = true
  error.value = ''

  try {
    const success = await authStore.login(username.value, password.value)

    if (success) {
      router.push({ name: 'designer' })
    } else {
      error.value = authStore.authError || 'Invalid username or password'
    }
  } catch (err) {
    error.value = 'An error occurred during login. Please try again.'
    console.error('Login error:', err)
  } finally {
    isLoading.value = false
  }
}
</script>

<style scoped>
.login-container {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  background: linear-gradient(135deg, var(--color-background-secondary) 0%, var(--color-background-primary) 100%);
  padding: var(--spacing-4);
}

.login-card {
  width: 100%;
  max-width: 400px;
  background-color: var(--color-background-primary);
  border-radius: var(--border-radius-xl);
  box-shadow: var(--shadow-xl);
  padding: var(--spacing-8);
  border: 1px solid var(--color-border-light);
}

.login-header {
  text-align: center;
  margin-bottom: var(--spacing-8);
}

.logo-container {
  display: flex;
  justify-content: center;
  margin-bottom: var(--spacing-4);
}

.logo {
  width: 80px;
  height: 80px;
  object-fit: contain;
}

.login-title {
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-2);
}

.login-subtitle {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  margin: 0;
}

.login-form {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.error-message {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  background-color: var(--color-danger-light);
  border: 1px solid var(--color-danger);
  border-radius: var(--border-radius-md);
  color: var(--color-danger);
  font-size: var(--font-size-sm);
}

.error-message i {
  flex-shrink: 0;
}

.form-field {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-1-5);
}

.form-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.input-wrapper {
  position: relative;
  display: flex;
  align-items: center;
}

.input-icon {
  position: absolute;
  left: var(--spacing-3);
  color: var(--color-text-muted);
  font-size: var(--font-size-sm);
  pointer-events: none;
}

.form-input {
  width: 100%;
  height: 44px;
  padding: var(--spacing-2) var(--spacing-3);
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
}

.form-input.with-icon {
  padding-left: var(--spacing-10);
  padding-right: var(--spacing-10);
}

.form-input:focus {
  outline: none;
  border-color: var(--color-accent);
  box-shadow: 0 0 0 3px var(--color-accent-subtle);
}

.form-input:disabled {
  background-color: var(--color-background-muted);
  cursor: not-allowed;
}

.form-input::placeholder {
  color: var(--color-text-muted);
}

.toggle-password {
  position: absolute;
  right: var(--spacing-3);
  background: none;
  border: none;
  color: var(--color-text-muted);
  cursor: pointer;
  padding: var(--spacing-1);
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color var(--transition-fast);
}

.toggle-password:hover {
  color: var(--color-text-secondary);
}

.login-button {
  width: 100%;
  height: 44px;
  margin-top: var(--spacing-4);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  display: flex;
  align-items: center;
  justify-content: center;
  gap: var(--spacing-2);
}

.login-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.loading-spinner {
  width: 18px;
  height: 18px;
  border: 2px solid transparent;
  border-top-color: currentColor;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.login-footer {
  text-align: center;
  margin-top: var(--spacing-8);
  padding-top: var(--spacing-4);
  border-top: 1px solid var(--color-border-light);
}

.footer-text {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  margin: 0;
}
</style>
