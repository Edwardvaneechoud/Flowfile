<template>
    <Teleport to="body">
      <Transition name="modal">
        <div v-if="isOpen" class="modal-overlay" @click.self="$emit('close')">
          <div class="modal-container">
            <!-- Modal Header -->
            <div class="modal-header">
              <div class="modal-title-group">
                <div class="modal-icon">
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/>
                    <polyline points="14 2 14 8 20 8"/>
                    <line x1="12" y1="18" x2="12" y2="12"/>
                    <line x1="9" y1="15" x2="15" y2="15"/>
                  </svg>
                </div>
                <div>
                  <h2>Missing Files</h2>
                  <p class="modal-subtitle">The following files need to be re-uploaded</p>
                </div>
              </div>
              <button class="close-btn" @click="$emit('close')" title="Close">
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                  <line x1="18" y1="6" x2="6" y2="18"/>
                  <line x1="6" y1="6" x2="18" y2="18"/>
                </svg>
              </button>
            </div>
  
            <!-- Modal Content -->
            <div class="modal-content">
              <p class="info-text">
                Your flow references files that aren't available in this session. 
                Please upload the required files to continue.
              </p>
  
              <div class="files-list">
                <div 
                  v-for="file in missingFiles" 
                  :key="file.nodeId" 
                  class="file-item"
                  :class="{ 'file-resolved': resolvedFiles.has(file.nodeId) }"
                >
                  <div class="file-info">
                    <div class="file-icon">
                      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/>
                        <polyline points="14 2 14 8 20 8"/>
                      </svg>
                    </div>
                    <div class="file-details">
                      <span class="file-name">{{ file.fileName }}</span>
                      <span class="node-id">Node #{{ file.nodeId }}</span>
                    </div>
                  </div>
                  
                  <div class="file-actions">
                    <template v-if="resolvedFiles.has(file.nodeId)">
                      <span class="resolved-badge">
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <polyline points="20 6 9 17 4 12"/>
                        </svg>
                        Uploaded
                      </span>
                    </template>
                    <template v-else>
                      <label class="upload-btn">
                        <input 
                          type="file" 
                          accept=".csv,.txt"
                          @change="(e) => handleFileUpload(e, file.nodeId)"
                        />
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>
                          <polyline points="17 8 12 3 7 8"/>
                          <line x1="12" y1="3" x2="12" y2="15"/>
                        </svg>
                        Upload
                      </label>
                    </template>
                  </div>
                </div>
              </div>
            </div>
  
            <!-- Modal Footer -->
            <div class="modal-footer">
              <button class="btn-secondary" @click="$emit('close')">
                Skip for Now
              </button>
              <button 
                class="btn-primary" 
                :disabled="resolvedFiles.size !== missingFiles.length"
                @click="handleComplete"
              >
                Continue
              </button>
            </div>
          </div>
        </div>
      </Transition>
    </Teleport>
  </template>
  
  <script setup lang="ts">
  import { ref, watch } from 'vue'
  import { useFlowStore } from '@/stores/flow-store'
  
  interface MissingFile {
    nodeId: number
    fileName: string
  }
  
  const props = defineProps<{
    isOpen: boolean
    missingFiles: MissingFile[]
  }>()
  
  const emit = defineEmits<{
    close: []
    complete: []
  }>()
  
  const flowStore = useFlowStore()
  const resolvedFiles = ref<Set<number>>(new Set())
  
  async function handleFileUpload(event: Event, nodeId: number) {
    const input = event.target as HTMLInputElement
    const file = input.files?.[0]
    
    if (!file) return
  
    try {
      const content = await file.text()
      flowStore.updateNodeFile(nodeId, file.name, content)
      resolvedFiles.value.add(nodeId)
      // Trigger reactivity
      resolvedFiles.value = new Set(resolvedFiles.value)
    } catch (error) {
      console.error('Failed to read file:', error)
    }
  }

  watch(() => props.isOpen, (isOpen) => {
  if (isOpen) {
    resolvedFiles.value = new Set()
  }
})


  
  function handleComplete() {
    emit('complete')
    emit('close')
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
    max-width: 500px;
    max-height: calc(100vh - 48px);
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
  
  .modal-title-group {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  
  .modal-icon {
    width: 40px;
    height: 40px;
    border-radius: 10px;
    background: rgba(249, 115, 22, 0.1);
    color: #f97316;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  
  .modal-icon svg {
    width: 20px;
    height: 20px;
  }
  
  .modal-title-group h2 {
    font-size: 18px;
    font-weight: 600;
    margin: 0;
    color: var(--color-text-primary);
  }
  
  .modal-subtitle {
    font-size: 13px;
    color: var(--color-text-secondary);
    margin: 2px 0 0;
  }
  
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
  
  .close-btn:hover {
    background: var(--color-background-hover);
    color: var(--color-text-primary);
  }
  
  .close-btn svg {
    width: 16px;
    height: 16px;
  }
  
  .modal-content {
    flex: 1;
    overflow-y: auto;
    padding: 24px;
  }
  
  .info-text {
    font-size: 14px;
    line-height: 1.5;
    color: var(--color-text-secondary);
    margin: 0 0 20px;
  }
  
  .files-list {
    display: flex;
    flex-direction: column;
    gap: 12px;
  }
  
  .file-item {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    background: var(--color-background-secondary);
    border: 1px solid var(--color-border-light);
    border-radius: 8px;
    transition: all 0.2s;
  }
  
  .file-item.file-resolved {
    background: rgba(34, 197, 94, 0.05);
    border-color: rgba(34, 197, 94, 0.2);
  }
  
  .file-info {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  
  .file-icon {
    width: 36px;
    height: 36px;
    border-radius: 8px;
    background: rgba(59, 130, 246, 0.1);
    color: #3b82f6;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  
  .file-icon svg {
    width: 18px;
    height: 18px;
  }
  
  .file-details {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  
  .file-name {
    font-size: 14px;
    font-weight: 500;
    color: var(--color-text-primary);
  }
  
  .node-id {
    font-size: 12px;
    color: var(--color-text-secondary);
  }
  
  .file-actions {
    flex-shrink: 0;
  }
  
  .upload-btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 12px;
    background: var(--color-accent);
    color: white;
    border: none;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s;
  }
  
  .upload-btn:hover {
    background: var(--color-accent-hover, #1d4ed8);
  }
  
  .upload-btn input {
    display: none;
  }
  
  .upload-btn svg {
    width: 14px;
    height: 14px;
  }
  
  .resolved-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 12px;
    background: rgba(34, 197, 94, 0.1);
    color: #22c55e;
    border-radius: 6px;
    font-size: 13px;
    font-weight: 500;
  }
  
  .resolved-badge svg {
    width: 14px;
    height: 14px;
  }
  
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
  
  .btn-secondary:hover {
    background: var(--color-background-hover);
  }
  
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
  
  .btn-primary:hover:not(:disabled) {
    background: var(--color-accent-hover, #1d4ed8);
  }
  
  .btn-primary:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  
  /* Transitions */
  .modal-enter-active,
  .modal-leave-active {
    transition: all 0.3s ease;
  }
  
  .modal-enter-from,
  .modal-leave-to {
    opacity: 0;
  }
  
  .modal-enter-from .modal-container,
  .modal-leave-to .modal-container {
    transform: scale(0.95) translateY(10px);
  }
  </style>
  