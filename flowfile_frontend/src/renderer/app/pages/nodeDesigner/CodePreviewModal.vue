<template>
  <div v-if="show" class="modal-overlay" @click="emit('close')">
    <div class="modal-container modal-large" @click.stop>
      <div class="modal-header">
        <h3 class="modal-title">Generated Python Code</h3>
        <button class="modal-close" @click="emit('close')">
          <i class="fa-solid fa-times"></i>
        </button>
      </div>
      <div class="modal-content">
        <div class="code-preview">
          <pre><code>{{ code }}</code></pre>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn btn-secondary" @click="copyCode">
          <i class="fa-solid fa-copy"></i>
          Copy Code
        </button>
        <button class="btn btn-primary" @click="emit('close')">Close</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
defineProps<{
  show: boolean;
  code: string;
}>();

const emit = defineEmits<{
  (e: "close"): void;
}>();

function copyCode() {
  navigator.clipboard.writeText(arguments[0]);
  alert("Code copied to clipboard!");
}
</script>

<style scoped>
.code-preview {
  background: var(--color-code-bg);
  border-radius: var(--border-radius-sm);
  overflow-x: auto;
}

.code-preview pre {
  margin: 0;
  padding: 1rem;
}

.code-preview code {
  font-family: var(--font-family-mono);
  font-size: 0.8125rem;
  color: var(--color-code-text);
  white-space: pre;
}
</style>
