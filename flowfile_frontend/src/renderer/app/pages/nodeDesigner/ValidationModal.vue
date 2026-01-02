<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container" @click.stop>
      <div class="modal-header modal-header-error">
        <h3 class="modal-title">
          <i class="fa-solid fa-triangle-exclamation"></i>
          Validation Errors
        </h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <p class="validation-intro">Please fix the following issues before saving:</p>
        <ul class="validation-errors-list">
          <li v-for="(error, index) in errors" :key="index" class="validation-error-item">
            <i class="fa-solid fa-circle-xmark"></i>
            {{ error.message }}
          </li>
        </ul>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" @click="emit('close')">
          OK
        </button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { ValidationError } from './types';

defineProps<{
  show: boolean;
  errors: ValidationError[];
}>();

const emit = defineEmits<{
  (e: 'close'): void;
}>();
</script>

<style scoped>
.modal-header-error {
  background: #fef2f2;
  border-bottom-color: #fecaca;
}

.modal-header-error .modal-title {
  color: #dc2626;
}

.modal-header-error .modal-title i {
  margin-right: 0.5rem;
}

.validation-intro {
  margin: 0 0 1rem 0;
  color: var(--text-secondary);
}

.validation-errors-list {
  list-style: none;
  padding: 0;
  margin: 0;
}

.validation-error-item {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  padding: 0.5rem 0;
  border-bottom: 1px solid var(--border-color);
  color: var(--text-primary);
}

.validation-error-item:last-child {
  border-bottom: none;
}

.validation-error-item i {
  color: #dc2626;
  margin-top: 0.125rem;
}
</style>
