<template>
  <div class="setup-container">
    <div class="setup-card">
      <div class="setup-header">
        <div class="logo-container">
          <img src="/images/flowfile.svg" alt="Flowfile" class="logo" />
        </div>
        <h1 class="setup-title">Initial Setup Required</h1>
        <p class="setup-subtitle">Configure your master encryption key to get started</p>
      </div>

      <div class="setup-content">
        <div class="info-box">
          <i class="fa-solid fa-shield-halved info-icon"></i>
          <div class="info-text">
            <h3>What is the Master Key?</h3>
            <p>
              The master key encrypts all secrets stored in Flowfile (API keys, passwords, tokens).
              It must be configured before the application can be used.
            </p>
          </div>
        </div>

        <div v-if="error" class="error-message">
          <i class="fa-solid fa-circle-exclamation"></i>
          <span>{{ error }}</span>
        </div>

        <div v-if="!generatedKey" class="generate-section">
          <button
            class="btn btn-primary generate-button"
            :disabled="isGenerating"
            @click="handleGenerateKey"
          >
            <span v-if="isGenerating" class="loading-spinner"></span>
            <i v-else class="fa-solid fa-key"></i>
            <span>{{ isGenerating ? "Generating..." : "Generate Master Key" }}</span>
          </button>
        </div>

        <div v-else class="key-result">
          <div class="key-display">
            <label class="key-label">Your Generated Master Key:</label>
            <div class="key-value-wrapper">
              <code class="key-value">{{ generatedKey.key }}</code>
              <button class="copy-button" title="Copy to clipboard" @click="copyKey">
                <i :class="copied ? 'fa-solid fa-check' : 'fa-solid fa-copy'"></i>
              </button>
            </div>
          </div>

          <div class="instructions-box">
            <h4>Configuration Instructions:</h4>
            <div class="instruction-content">
              <p>Add this line to a <code>.env</code> file in your project root:</p>
              <div class="code-block">
                <code>FLOWFILE_MASTER_KEY="{{ generatedKey.key }}"</code>
                <button class="copy-button small" @click="copyEnvVar">
                  <i :class="copiedEnv ? 'fa-solid fa-check' : 'fa-solid fa-copy'"></i>
                </button>
              </div>
              <p class="hint">
                Then restart: <code>docker-compose down && docker-compose up</code>
              </p>
            </div>
          </div>

          <div class="warning-box">
            <i class="fa-solid fa-triangle-exclamation"></i>
            <div>
              <strong>Important:</strong> Back up this key securely. If lost, all stored secrets
              become unrecoverable.
            </div>
          </div>
        </div>
      </div>

      <div class="setup-footer">
        <p class="footer-text">Flowfile - Visual Data Processing</p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from "vue";
import setupService, { type GeneratedKey } from "../../services/setup.service";

const isGenerating = ref(false);
const error = ref("");
const generatedKey = ref<GeneratedKey | null>(null);
const copied = ref(false);
const copiedEnv = ref(false);

const handleGenerateKey = async () => {
  isGenerating.value = true;
  error.value = "";

  try {
    generatedKey.value = await setupService.generateKey();
  } catch (err) {
    error.value = "Failed to generate key. Please check if the backend is running.";
    console.error("Generate key error:", err);
  } finally {
    isGenerating.value = false;
  }
};

const copyToClipboard = async (text: string, flagRef: typeof copied) => {
  try {
    // navigator.clipboard is only available in secure contexts (HTTPS or localhost).
    // When accessing Docker deployment over plain HTTP from another machine, fall back
    // to the legacy execCommand approach.
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
    }
    flagRef.value = true;
    setTimeout(() => {
      flagRef.value = false;
    }, 2000);
  } catch (err) {
    console.error("Failed to copy:", err);
  }
};

const copyKey = () => {
  if (generatedKey.value) {
    copyToClipboard(generatedKey.value.key, copied);
  }
};

const copyEnvVar = () => {
  if (generatedKey.value) {
    copyToClipboard(`FLOWFILE_MASTER_KEY="${generatedKey.value.key}"`, copiedEnv);
  }
};
</script>

<style scoped>
.setup-container {
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 100vh;
  background: linear-gradient(
    135deg,
    var(--color-background-secondary) 0%,
    var(--color-background-primary) 100%
  );
  padding: var(--spacing-4);
}

.setup-card {
  width: 100%;
  max-width: 600px;
  background-color: var(--color-background-primary);
  border-radius: var(--border-radius-xl);
  box-shadow: var(--shadow-xl);
  padding: var(--spacing-8);
  border: 1px solid var(--color-border-light);
}

.setup-header {
  text-align: center;
  margin-bottom: var(--spacing-6);
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

.setup-title {
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  margin-bottom: var(--spacing-2);
}

.setup-subtitle {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
  margin: 0;
}

.setup-content {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.info-box {
  display: flex;
  gap: var(--spacing-3);
  padding: var(--spacing-4);
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-info);
  border-radius: var(--border-radius-md);
}

.info-icon {
  font-size: var(--font-size-xl);
  color: var(--color-info);
  flex-shrink: 0;
}

.info-text h3 {
  margin: 0 0 var(--spacing-1) 0;
  font-size: var(--font-size-base);
  color: var(--color-text-primary);
}

.info-text p {
  margin: 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
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

.generate-section {
  display: flex;
  justify-content: center;
  padding: var(--spacing-4) 0;
}

.generate-button {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-6);
  font-size: var(--font-size-base);
}

.key-result {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-4);
}

.key-display {
  display: flex;
  flex-direction: column;
  gap: var(--spacing-2);
}

.key-label {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.key-value-wrapper {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  padding: var(--spacing-3);
}

.key-value {
  flex: 1;
  font-family: var(--font-family-mono);
  font-size: var(--font-size-sm);
  word-break: break-all;
  color: var(--color-text-primary);
}

.copy-button {
  background: none;
  border: none;
  color: var(--color-text-muted);
  cursor: pointer;
  padding: var(--spacing-2);
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color var(--transition-fast);
  border-radius: var(--border-radius-sm);
}

.copy-button:hover {
  color: var(--color-text-secondary);
  background-color: var(--color-background-muted);
}

.copy-button.small {
  padding: var(--spacing-1);
}

.instructions-box {
  background-color: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-md);
  padding: var(--spacing-4);
}

.instructions-box h4 {
  margin: 0 0 var(--spacing-3) 0;
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.instruction-content p {
  margin: 0 0 var(--spacing-2) 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.instruction-content code {
  background-color: var(--color-background-muted);
  padding: var(--spacing-0-5) var(--spacing-1);
  border-radius: var(--border-radius-sm);
  font-size: var(--font-size-xs);
}

.instruction-content .hint {
  margin-top: var(--spacing-3);
  color: var(--color-text-muted);
}

.code-block {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  padding: var(--spacing-2) var(--spacing-3);
}

.code-block code {
  flex: 1;
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  word-break: break-all;
  background: none;
  padding: 0;
}

.warning-box {
  display: flex;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  background-color: var(--color-warning-light);
  border: 1px solid var(--color-warning);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  color: var(--color-warning-dark);
}

.warning-box i {
  flex-shrink: 0;
  color: var(--color-warning);
}

.setup-footer {
  text-align: center;
  margin-top: var(--spacing-6);
  padding-top: var(--spacing-4);
  border-top: 1px solid var(--color-border-light);
}

.footer-text {
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
  margin: 0;
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
</style>
