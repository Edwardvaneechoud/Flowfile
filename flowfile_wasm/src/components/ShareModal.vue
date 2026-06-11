<template>
  <Teleport to="body">
    <Transition name="modal">
      <div v-if="isOpen" class="modal-overlay" @click.self="$emit('close')">
        <div class="modal-container">
          <div class="modal-header">
            <div class="modal-title-group">
              <div class="modal-icon">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/>
                </svg>
              </div>
              <div>
                <h2>Share flow via link</h2>
                <p class="modal-subtitle">Anyone who opens the link gets a copy of this flow</p>
              </div>
            </div>
            <button class="close-btn" title="Close" @click="$emit('close')">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </div>

          <div class="modal-content">
            <div v-if="isGenerating" class="state-note">Generating link…</div>
            <div v-else-if="generateError" class="note note--warning">
              Failed to generate the share link. Please try again.
            </div>
            <template v-else-if="shareUrl">
              <div class="url-row">
                <input
                  ref="urlInput"
                  class="field-input url-input"
                  type="text"
                  readonly
                  :value="shareUrl"
                  @focus="($event.target as HTMLInputElement).select()"
                />
                <button class="btn-primary copy-btn" @click="copyUrl">
                  {{ copied ? 'Copied' : 'Copy' }}
                </button>
              </div>

              <div v-if="shareUrl.length > LONG_URL_THRESHOLD" class="note note--warning">
                Long link — chat and email apps may truncate it. For very large flows, use
                Export to file instead.
              </div>

              <div v-if="excludedFiles.length" class="note">
                Not included in the link (recipients will be asked to re-select them):
                {{ excludedFiles.map((f) => f.fileName).join(', ') }}
              </div>

              <p class="privacy-note">
                The link itself contains the flow and any inlined data — anyone you send it to
                can see them.
              </p>
            </template>
          </div>

          <div class="modal-footer">
            <button class="btn-secondary" @click="$emit('close')">Close</button>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { useShareLink } from '../composables/useShareLink'

const LONG_URL_THRESHOLD = 8000

const props = defineProps<{ isOpen: boolean }>()
defineEmits<{ close: [] }>()

const { generateShareUrl } = useShareLink()

const isGenerating = ref(false)
const generateError = ref(false)
const shareUrl = ref('')
const excludedFiles = ref<Array<{ nodeId: number; fileName: string }>>([])
const copied = ref(false)
const urlInput = ref<HTMLInputElement | null>(null)

watch(
  () => props.isOpen,
  async (open) => {
    if (!open) return
    isGenerating.value = true
    generateError.value = false
    shareUrl.value = ''
    excludedFiles.value = []
    copied.value = false
    try {
      const result = await generateShareUrl()
      shareUrl.value = result.url
      excludedFiles.value = result.excludedFiles
    } catch (e) {
      console.error('Failed to generate share link:', e)
      generateError.value = true
    } finally {
      isGenerating.value = false
    }
  }
)

async function copyUrl() {
  try {
    await navigator.clipboard.writeText(shareUrl.value)
  } catch {
    // Clipboard API needs a secure context; fall back to select + execCommand.
    if (!urlInput.value) return
    urlInput.value.select()
    document.execCommand('copy')
  }
  copied.value = true
  setTimeout(() => (copied.value = false), 1600)
}
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  inset: 0;
  z-index: 9999;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 24px;
}

.modal-container {
  background: var(--color-background-primary);
  border-radius: 12px;
  width: 100%;
  max-width: 520px;
  display: flex;
  flex-direction: column;
  box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
  border: 1px solid var(--color-border-primary);
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 20px 24px;
  border-bottom: 1px solid var(--color-border-light);
}

.modal-title-group { display: flex; align-items: center; gap: 12px; }

.modal-icon {
  width: 40px;
  height: 40px;
  border-radius: 10px;
  background: var(--color-accent-subtle);
  color: var(--color-accent);
  display: flex;
  align-items: center;
  justify-content: center;
}
.modal-icon svg { width: 20px; height: 20px; }

.modal-title-group h2 { font-size: 18px; font-weight: 600; margin: 0; color: var(--color-text-primary); }
.modal-subtitle { font-size: 13px; color: var(--color-text-secondary); margin: 2px 0 0; }

.close-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border: none;
  border-radius: 6px;
  background: var(--color-background-secondary);
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all 0.2s;
}
.close-btn:hover { background: var(--color-background-hover); color: var(--color-text-primary); }
.close-btn svg { width: 16px; height: 16px; }

.modal-content { padding: 24px; display: flex; flex-direction: column; gap: 12px; }

.state-note { font-size: 14px; color: var(--color-text-secondary); }

.url-row { display: flex; gap: 8px; }

.field-input {
  width: 100%;
  padding: 10px 12px;
  font-size: 14px;
  color: var(--color-text-primary);
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 8px;
}
.field-input:focus { outline: none; border-color: var(--color-accent); box-shadow: 0 0 0 2px var(--color-focus-ring-accent); }
.url-input { flex: 1; font-size: 13px; text-overflow: ellipsis; }

.copy-btn { white-space: nowrap; min-width: 76px; }

.note {
  font-size: 13px;
  color: var(--color-text-secondary);
  background: var(--color-background-secondary);
  border: 1px solid var(--color-border-light);
  border-radius: 8px;
  padding: 10px 12px;
}
.note--warning {
  color: var(--color-warning, #b45309);
  background: var(--color-warning-subtle, rgba(245, 158, 11, 0.1));
  border-color: var(--color-warning-border, rgba(245, 158, 11, 0.3));
}

.privacy-note { font-size: 12px; color: var(--color-text-tertiary, var(--color-text-secondary)); margin: 0; }

.modal-footer {
  padding: 16px 24px;
  border-top: 1px solid var(--color-border-light);
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

.btn-secondary {
  padding: 10px 16px;
  background: var(--color-background-secondary);
  color: var(--color-text-primary);
  border: 1px solid var(--color-border-light);
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
}
.btn-secondary:hover { background: var(--color-background-hover); }

.btn-primary {
  padding: 10px 16px;
  background: var(--color-accent);
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
}
.btn-primary:hover:not(:disabled) { background: var(--color-accent-hover, #1d4ed8); }

.modal-enter-active, .modal-leave-active { transition: all 0.3s ease; }
.modal-enter-from, .modal-leave-to { opacity: 0; }
.modal-enter-from .modal-container, .modal-leave-to .modal-container { transform: scale(0.95) translateY(10px); }
</style>
