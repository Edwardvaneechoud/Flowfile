<template>
    <div class="secret-manager-container">
      <div class="page-header">
        <h2 class="page-title">Secret Manager</h2>
        <p class="page-description">Securely store and manage credentials for your integrations</p>
      </div>
  
      <!-- Add Secret Card -->
      <div class="settings-card">
        <div class="card-header">
          <h3 class="section-title">Add New Secret</h3>
        </div>
        <div class="card-content">
          <!-- Use the addSecret method defined directly in script setup -->
          <form @submit.prevent="addSecret" class="add-secret-form">
            <div class="form-grid">
              <div class="form-field">
                <label for="secret-name">Secret Name</label>
                <input
                  id="secret-name"
                  v-model="newSecret.name"
                  type="text"
                  class="text-input"
                  placeholder="api_key, database_password, etc."
                  required
                />
              </div>
  
              <div class="form-field">
                <label for="secret-value">Secret Value</label>
                <div class="password-field">
                  <input
                    id="secret-value"
                    v-model="newSecret.value"
                    :type="showNewSecret ? 'text' : 'password'"
                    class="text-input"
                    placeholder="Enter secret value"
                    required
                  />
                  <button
                    type="button"
                    class="toggle-visibility"
                    @click="showNewSecret = !showNewSecret"
                    aria-label="Toggle new secret visibility"
                  >
                    <i :class="showNewSecret ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
                  </button>
                </div>
              </div>
            </div>
  
            <div class="form-actions">
              <button
                type="submit"
                class="add-button"
                :disabled="!newSecret.name || !newSecret.value || isSubmitting"
              >
                <i class="fa-solid fa-plus"></i>
                {{ isSubmitting ? 'Adding...' : 'Add Secret' }}
              </button>
            </div>
          </form>
        </div>
      </div>
  
      <!-- Secret List Card -->
      <div class="settings-card">
        <div class="card-header">
           <!-- Reference filteredSecrets directly -->
          <h3 class="section-title">Your Secrets ({{ filteredSecrets.length }})</h3>
          <div class="search-container" v-if="secrets.length > 0">
            <input
              type="text"
              v-model="searchTerm"
              placeholder="Search secrets..."
              class="search-input"
              aria-label="Search secrets"
            />
            <i class="fa-solid fa-search search-icon"></i>
          </div>
        </div>
        <div class="card-content">
          <!-- Reference isLoading directly -->
          <div v-if="isLoading" class="loading-container">
            <div class="loading-spinner"></div>
            <p>Loading secrets...</p>
          </div>
  
          <div v-else-if="!isLoading && secrets.length === 0" class="empty-state">
            <i class="fa-solid fa-lock empty-icon"></i>
            <p>You haven't added any secrets yet</p>
            <p class="empty-hint">Secrets are securely stored and can be used in your flows</p>
          </div>
  
          <div v-else-if="filteredSecrets.length > 0" class="secrets-list">
             <!-- Reference filteredSecrets directly -->
            <div
              v-for="secret in filteredSecrets"
              :key="secret.name"
              class="secret-item"
            >
              <div class="secret-info">
                <div class="secret-name">
                  <i class="fa-solid fa-key secret-icon"></i>
                  <span>{{ secret.name }}</span>
                </div>
                <div class="secret-value">
                  <input
                    :type="'password'"
                    :value="'••••••••••••••••'"
                    readonly
                    class="secret-input"
                    aria-label="Masked secret value"
                  />
                   <!-- Reference visibleSecrets and toggleSecretVisibility directly -->
                  <button
                    type="button"
                    class="toggle-visibility"
                    @click="toggleSecretVisibility(secret.name)"
                    :aria-label="`Toggle visibility icon for ${secret.name}`"
                  >
                    <i :class="visibleSecrets.includes(secret.name) ? 'fa-solid fa-eye-slash' : 'fa-solid fa-eye'"></i>
                  </button>
                </div>
              </div>
               <div class="secret-actions">
                  <!-- Reference copyMessage directly -->
                  <span v-if="copyMessage && copyMessage.includes(secret.name)" class="copy-feedback">{{ copyMessage }}</span>
                 <button
                   type="button"
                   class="copy-button"
                   @click="copySecretToClipboard(secret.name)"
                   :aria-label="`Copy value for ${secret.name}`"
                 >
                   <i class="fa-solid fa-copy"></i>
                   <span>Copy Value</span>
                 </button>
                 <button
                   type="button"
                   class="delete-button"
                   @click="confirmDelete(secret.name)"
                   :aria-label="`Delete secret ${secret.name}`"
                 >
                   <i class="fa-solid fa-trash-alt"></i>
                   <span>Delete</span>
                 </button>
               </div>
            </div>
          </div>
  
           <div v-else class="empty-state">
             <i class="fa-solid fa-search empty-icon"></i>
             <!-- Reference searchTerm directly -->
             <p>No secrets found matching "{{ searchTerm }}"</p>
           </div>
  
        </div>
      </div>
  
      <!-- Usage Guide Card (Static Content) -->
      <div class="settings-card">
         <div class="card-header">
           <h3 class="section-title">How to Use Secrets</h3>
         </div>
         <div class="card-content">
           <div class="usage-guide">
             <div class="usage-step">
               <div class="step-number">1</div>
               <div class="step-content">
                 <h4>Store your credentials securely</h4>
                 <p>Add API keys, passwords, and other sensitive information as secrets</p>
               </div>
             </div>
             <div class="usage-step">
               <div class="step-number">2</div>
               <div class="step-content">
                 <h4>Reference secrets in your flows</h4>
                 <!-- Ensure example code is correct -->
                 <p>Use <code>${{ '{' }}secret.YOUR_SECRET_NAME}</code> to safely reference secrets</p>
               </div>
             </div>
             <div class="usage-step">
               <div class="step-number">3</div>
               <div class="step-content">
                 <h4>Keep your data secure</h4>
                 <p>Secret values are encrypted at rest and never exposed in your flow definitions</p>
               </div>
             </div>
           </div>
         </div>
       </div>
  
  
      <!-- Delete Confirmation Modal -->
       <!-- Reference showDeleteModal, cancelDelete directly -->
      <div class="modal-overlay" v-if="showDeleteModal" @click="cancelDelete">
         <!-- Stop propagation on container to prevent closing when clicking inside -->
        <div class="modal-container" @click.stop>
          <div class="modal-header">
            <h3>Delete Secret</h3>
            <!-- Use cancelDelete for close button -->
            <button class="modal-close" @click="cancelDelete" aria-label="Close delete confirmation">
              <i class="fa-solid fa-times"></i>
            </button>
          </div>
          <div class="modal-content">
             <!-- Reference secretToDelete directly -->
            <p>Are you sure you want to delete the secret <strong>{{ secretToDelete }}</strong>?</p>
            <p class="warning-text">This action cannot be undone and may break any flows that use this secret.</p>
          </div>
          <div class="modal-actions">
             <!-- Use cancelDelete for cancel button -->
            <button class="cancel-button" @click="cancelDelete">Cancel</button>
            <!-- Reference deleteSecret, isDeleting directly -->
            <button
               class="confirm-delete-button"
               @click="deleteSecret"
               :disabled="isDeleting"
             >
              <i v-if="isDeleting" class="fas fa-spinner fa-spin mr-2"></i> <!-- Optional spinner -->
              {{ isDeleting ? 'Deleting...' : 'Delete Secret' }}
            </button>
          </div>
        </div>
      </div>
    </div>
  </template>
  
  <script setup lang="ts">
  import { ref, computed, onMounted, type Ref } from 'vue';
  // Import API functions and types directly
  import { fetchSecretsApi, addSecretApi, getSecretValueApi, deleteSecretApi } from './secretManager/secretApi'; // Adjust path if needed
  import type { Secret, SecretInput } from './secretManager/secretTypes'; // Adjust path if needed
  
  // --- State ---
  const secrets: Ref<Secret[]> = ref([]);
  const newSecret = ref<SecretInput>({ name: '', value: '' });
  const isLoading = ref(true);
  const isSubmitting = ref(false);
  const isDeleting = ref(false);
  const showNewSecret = ref(false); // For the 'Add New Secret' form password visibility
  const visibleSecrets = ref<string[]>([]); // Stores names of secrets whose visibility icon is 'eye-slash'
  const searchTerm = ref('');
  const showDeleteModal = ref(false);
  const secretToDelete = ref('');
  const copyMessage = ref(''); // Feedback message after copying
  
  // --- Computed Properties ---
  const filteredSecrets = computed(() => {
    const sortedSecrets = [...secrets.value].sort((a, b) => a.name.localeCompare(b.name));
    if (!searchTerm.value) {
      return sortedSecrets;
    }
    const term = searchTerm.value.toLowerCase();
    return sortedSecrets.filter(secret =>
      secret.name.toLowerCase().includes(term)
    );
  });
  
  // --- Methods ---
  const loadSecrets = async () => {
    isLoading.value = true;
    visibleSecrets.value = []; // Reset visibility on reload
    try {
      // Call API function
      secrets.value = await fetchSecretsApi();
      // Optional: show success notification
      // console.log('Secrets loaded successfully');
    } catch (error) {
      console.error('Failed to load secrets:', error);
      secrets.value = []; // Clear secrets on error to prevent stale data display
      // Optional: show error notification to user
      alert('Failed to load secrets. Please try again.');
    } finally {
      isLoading.value = false;
    }
  };
  
  const addSecret = async () => {
    if (!newSecret.value.name || !newSecret.value.value) return;
  
    // Basic validation: Check if secret name already exists (case-sensitive)
    if (secrets.value.some(s => s.name === newSecret.value.name)) {
         alert(`Secret with name "${newSecret.value.name}" already exists.`);
         return;
    }
  
    isSubmitting.value = true;
    try {
      // Call API function
      await addSecretApi({ ...newSecret.value }); // Pass a copy
      await loadSecrets(); // Reload the list after adding
      // Reset form state
      newSecret.value = { name: '', value: '' };
      showNewSecret.value = false;
      // Optional: show success notification
      // console.log(`Secret "${newSecret.value.name}" added successfully.`);
       alert(`Secret "${newSecret.value.name}" added successfully.`); // Simple feedback
    } catch (error: any) {
      console.error('Failed to add secret:', error);
      // Display specific error from API if available, otherwise generic message
      const errorMsg = error.message || 'An unknown error occurred while adding the secret.';
      alert(`Error adding secret: ${errorMsg}`);
    } finally {
      isSubmitting.value = false;
    }
  };
  
  const toggleSecretVisibility = (secretName: string) => {
    // Toggles the visibility icon state for a specific secret in the list
    const index = visibleSecrets.value.indexOf(secretName);
    if (index === -1) {
      visibleSecrets.value.push(secretName);
    } else {
      visibleSecrets.value.splice(index, 1);
    }
    // Note: This does NOT fetch or display the actual value, only controls the icon.
  };
  
  const copySecretToClipboard = async (secretName: string) => {
      // Clear previous message immediately
      copyMessage.value = '';
    try {
      // Call API function to get the real value
      const secretValue = await getSecretValueApi(secretName);
  
      await navigator.clipboard.writeText(secretValue);
  
      // Provide user feedback
      copyMessage.value = `Value for '${secretName}' copied!`;
      // console.log(`Value for '${secretName}' copied!`); // Log for debugging
      setTimeout(() => {
        copyMessage.value = ''; // Clear the message after a delay
      }, 2500);
  
    } catch (error) {
      console.error('Failed to copy secret:', error);
      alert('Failed to retrieve or copy secret value.');
      copyMessage.value = `Failed to copy ${secretName}.`; // Error feedback
       setTimeout(() => { copyMessage.value = ''; }, 3000);
    }
  };
  
  const confirmDelete = (secretName: string) => {
    secretToDelete.value = secretName;
    showDeleteModal.value = true;
  };
  
  const cancelDelete = () => {
    showDeleteModal.value = false;
    secretToDelete.value = '';
  };
  
  const deleteSecret = async () => {
    if (!secretToDelete.value) return;
  
    isDeleting.value = true;
    try {
      const nameToDelete = secretToDelete.value; // Store before clearing state
      // Call API function
      await deleteSecretApi(nameToDelete);
      await loadSecrets(); // Refresh the list
      cancelDelete(); // Close modal and clear state
      // Optional: show success notification
      alert(`Secret "${nameToDelete}" deleted successfully.`);
    } catch (error) {
      console.error('Failed to delete secret:', error);
      alert('Failed to delete secret. Please try again.');
      // Keep modal open on error? Or close? User preference. Let's close it.
      cancelDelete();
    } finally {
      isDeleting.value = false;
    }
  };
  
  // --- Lifecycle Hooks ---
  onMounted(() => {
    loadSecrets(); // Load secrets when the component mounts
  });
  
  </script>
  
  <style scoped>
    /* Styles remain the same as in the original component */
    /* ... (Paste the original CSS here) ... */
  
    /* Main Container */
    .secret-manager-container {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: #1a202c;
      max-width: 100%;
      padding: 1.5rem;
    }
  
    /* Page Header */
    .page-header {
      margin-bottom: 2rem;
    }
  
    .page-title {
      font-size: 1.5rem;
      font-weight: 600;
      color: #2d3748;
      margin: 0 0 0.5rem 0;
    }
  
    .page-description {
      font-size: 0.95rem;
      color: #718096;
      margin: 0;
    }
  
    /* Card styling */
    .settings-card {
      background-color: #ffffff;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
      margin-bottom: 1.5rem;
      border: 1px solid #edf2f7;
      overflow: hidden;
    }
  
    .card-header {
      padding: 1rem 1.25rem;
      border-bottom: 1px solid #edf2f7;
      background-color: #fafafa;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap; /* Allow wrapping for search */
      gap: 0.5rem; /* Add gap for wrapping */
    }
  
    .section-title {
      margin: 0;
      font-size: 1rem;
      font-weight: 600;
      color: #2d3748;
    }
  
    .card-content {
      padding: 1.25rem;
    }
  
    /* Form Elements */
    .add-secret-form {
      display: flex;
      flex-direction: column;
      gap: 1.25rem;
    }
  
    .form-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); /* Responsive grid */
      gap: 1rem;
    }
  
    .form-field {
      display: flex;
      flex-direction: column;
      gap: 0.5rem;
    }
  
    .form-field label {
      font-size: 0.875rem;
      font-weight: 500;
      color: #4a5568;
    }
  
    .text-input {
      width: 100%;
      padding: 0.6rem 0.75rem;
      font-size: 0.875rem;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      background-color: #fff;
      color: #4a5568;
      outline: none;
      transition: border-color 0.2s ease, box-shadow 0.2s ease;
      box-sizing: border-box; /* Include padding and border in element's total width/height */
    }
  
    .text-input:focus {
      border-color: #3182ce;
      box-shadow: 0 0 0 2px rgba(49, 130, 206, 0.15);
    }
  
    .password-field {
      position: relative;
      display: flex;
      align-items: center;
    }
  
    .password-field .text-input {
      padding-right: 2.5rem; /* Make space for the button */
    }
  
    .toggle-visibility {
      position: absolute;
      right: 0.5rem;
      background: none;
      border: none;
      color: #718096;
      cursor: pointer;
      font-size: 1rem; /* Slightly larger for easier clicking */
      padding: 0.25rem;
      display: flex; /* Align icon nicely */
      align-items: center;
      justify-content: center;
      height: 100%; /* Align vertically */
    }
  
    .toggle-visibility:hover {
      color: #4a5568;
    }
    .toggle-visibility i {
        display: block; /* Prevent potential layout shifts */
    }
  
  
    .form-actions {
      display: flex;
      justify-content: flex-end;
      margin-top: 0.5rem; /* Add some space above the button */
    }
  
    /* Buttons */
    .add-button {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.6rem 1rem;
      background-color: #3182ce;
      color: white;
      border: none;
      border-radius: 6px;
      font-size: 0.875rem;
      font-weight: 500;
      cursor: pointer;
      transition: background-color 0.2s ease, opacity 0.2s ease;
    }
  
    .add-button:hover:not(:disabled) {
      background-color: #2b6cb0;
    }
  
    .add-button:disabled {
      opacity: 0.7;
      cursor: not-allowed;
    }
  
    /* Search */
    .search-container {
      position: relative;
      width: 100%; /* Take available space */
      max-width: 16rem; /* Limit max width */
    }
  
    .search-input {
      width: 100%;
      padding: 0.4rem 0.75rem 0.4rem 2.25rem; /* Increased left padding for icon */
      font-size: 0.875rem;
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      background-color: #f8fafc;
      color: #4a5568;
      outline: none;
      box-sizing: border-box;
    }
    .search-input:focus {
       border-color: #3182ce;
       background-color: #fff;
    }
  
  
    .search-icon {
      position: absolute;
      left: 0.75rem; /* Adjusted position */
      top: 50%;
      transform: translateY(-50%);
      color: #a0aec0;
      font-size: 0.875rem;
      pointer-events: none; /* Prevent icon from intercepting clicks */
    }
  
    /* Empty State & Loading */
    .empty-state,
    .loading-container {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 3rem 1rem;
      text-align: center;
      color: #718096;
    }
  
    .empty-icon {
      font-size: 2.5rem;
      color: #cbd5e0;
      margin-bottom: 1rem;
    }
  
    .empty-state p {
      margin: 0.25rem 0;
    }
    .empty-state p:first-of-type {
        font-weight: 500;
        color: #4a5568;
    }
  
    .empty-hint {
      font-size: 0.875rem;
      opacity: 0.8;
    }
  
    .loading-spinner {
      width: 2rem;
      height: 2rem;
      border: 3px solid #e2e8f0; /* Light grey */
      border-top-color: #3182ce; /* Blue */
      border-radius: 50%;
      animation: spin 1s linear infinite;
      margin-bottom: 1rem;
    }
  
    @keyframes spin {
      to { transform: rotate(360deg); }
    }
  
    /* Secrets List */
    .secrets-list {
      display: flex;
      flex-direction: column;
      gap: 1rem;
    }
  
    .secret-item {
      display: flex;
      flex-direction: column; /* Stack info and actions vertically */
      gap: 0.75rem;
      padding: 1rem;
      background-color: #f8fafc;
      border-radius: 6px;
      border: 1px solid #edf2f7;
      transition: box-shadow 0.2s ease, border-color 0.2s ease;
    }
  
    .secret-item:hover {
      box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
      border-color: #e2e8f0;
    }
  
    .secret-info {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); /* Responsive columns */
      gap: 1rem;
      align-items: center; /* Align items vertically */
    }
  
    .secret-name {
      display: flex;
      align-items: center;
      gap: 0.6rem; /* Slightly more space */
      font-weight: 500;
      color: #4a5568;
      word-break: break-all; /* Prevent long names from breaking layout */
    }
  
    .secret-icon {
      color: #3182ce;
      font-size: 0.9rem; /* Slightly larger icon */
      flex-shrink: 0; /* Prevent icon from shrinking */
    }
  
    .secret-value {
      position: relative;
      display: flex;
      align-items: center;
    }
  
    .secret-input {
      width: 100%;
      padding: 0.4rem 2.5rem 0.4rem 0.75rem; /* Adjust padding for button */
      font-size: 0.875rem;
      font-family: monospace; /* Use monospace for masked value */
      border: 1px solid #e2e8f0;
      border-radius: 6px;
      background-color: #fff;
      color: #4a5568;
      box-sizing: border-box;
      cursor: default; /* Indicate it's not editable */
    }
    /* Style for the masked text */
    .secret-input[type="password"] {
        letter-spacing: 0.15em; /* Space out the dots */
    }
  
    .secret-actions {
      display: flex;
      justify-content: flex-end;
      align-items: center; /* Align buttons vertically */
      gap: 0.75rem;
      flex-wrap: wrap; /* Allow buttons to wrap on small screens */
      margin-top: 0.5rem; /* Space between info and actions */
      border-top: 1px solid #edf2f7; /* Separator line */
      padding-top: 0.75rem; /* Space above buttons */
    }
  
    .copy-feedback {
        font-size: 0.8125rem;
        color: #2f855a; /* Green */
        margin-right: auto; /* Push buttons to the right */
        font-style: italic;
        /* Add transition for fade in/out? */
        transition: opacity 0.3s ease-in-out;
    }
    /* Hide feedback initially or when empty */
    .copy-feedback:empty {
        opacity: 0;
    }
  
  
    .copy-button, .delete-button {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem; /* Adjust gap */
      padding: 0.4rem 0.8rem; /* Adjust padding */
      border-radius: 6px;
      font-size: 0.8125rem;
      font-weight: 500;
      cursor: pointer;
      transition: background-color 0.2s ease, border-color 0.2s ease;
      white-space: nowrap; /* Prevent button text wrapping */
    }
    .copy-button i, .delete-button i {
        font-size: 0.875rem; /* Consistent icon size */
    }
  
    .copy-button {
      background-color: #ebf8ff;
      color: #3182ce;
      border: 1px solid #bee3f8;
    }
  
    .copy-button:hover {
      background-color: #bee3f8;
      border-color: #90cdf4;
    }
  
    .delete-button {
      background-color: #fff5f5;
      color: #e53e3e;
      border: 1px solid #fed7d7;
    }
  
    .delete-button:hover {
      background-color: #fed7d7;
      border-color: #feb2b2;
    }
  
    /* Usage Guide */
    .usage-guide {
      display: flex;
      flex-direction: column;
      gap: 1.5rem; /* Increased gap */
    }
  
    .usage-step {
      display: flex;
      gap: 1rem;
      align-items: flex-start;
    }
  
    .step-number {
      display: flex;
      align-items: center;
      justify-content: center;
      width: 2.25rem; /* Larger circle */
      height: 2.25rem;
      background-color: #ebf8ff;
      color: #3182ce;
      border-radius: 50%;
      font-weight: 600;
      font-size: 0.875rem; /* Slightly smaller number font */
      flex-shrink: 0;
      border: 1px solid #bee3f8; /* Subtle border */
    }
  
    .step-content h4 {
      margin: 0 0 0.35rem 0; /* Adjusted margin */
      font-size: 0.95rem;
      font-weight: 600;
      color: #2d3748;
    }
  
    .step-content p {
      margin: 0;
      font-size: 0.875rem;
      line-height: 1.5; /* Improve readability */
      color: #718096;
    }
  
    .step-content code {
      background-color: #edf2f7; /* Slightly darker background */
      padding: 0.125rem 0.3rem; /* Adjust padding */
      border-radius: 4px; /* More rounded corners */
      font-size: 0.8125rem;
      color: #2d3748; /* Darker text */
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      border: 1px solid #e2e8f0; /* Subtle border */
      /* Fix for template literal within code tag */
      white-space: pre-wrap;
    }
  
    /* Modal */
    .modal-overlay {
      position: fixed;
      inset: 0; /* Replaces top/left/right/bottom */
      background-color: rgba(0, 0, 0, 0.6); /* Darker overlay */
      display: flex;
      justify-content: center;
      align-items: center;
      z-index: 1000;
      padding: 1rem; /* Add padding for small screens */
    }
  
    .modal-container {
      background-color: white;
      border-radius: 8px;
      width: 100%;
      max-width: 32rem; /* 512px */
      max-height: 90vh;
      display: flex; /* Use flex for layout */
      flex-direction: column; /* Stack header/content/actions */
      box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
      overflow: hidden; /* Prevent content overflow */
    }
  
    .modal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 1rem 1.25rem;
      border-bottom: 1px solid #edf2f7;
      flex-shrink: 0; /* Prevent header from shrinking */
    }
  
    .modal-header h3 {
      margin: 0;
      font-size: 1.125rem;
      font-weight: 600;
      color: #2d3748;
    }
  
    .modal-close {
      background: none;
      border: none;
      color: #a0aec0; /* Lighter grey */
      cursor: pointer;
      font-size: 1.25rem; /* Larger close icon */
      padding: 0.25rem;
      line-height: 1; /* Ensure icon aligns well */
      transition: color 0.2s ease;
    }
    .modal-close:hover {
        color: #718096;
    }
  
    .modal-content {
      padding: 1.25rem;
      overflow-y: auto; /* Allow content scrolling */
      color: #4a5568;
      line-height: 1.6;
    }
    .modal-content p {
        margin-bottom: 1rem;
    }
    .modal-content p:last-child {
        margin-bottom: 0;
    }
    .modal-content strong {
        color: #2d3748;
        font-weight: 600;
    }
  
    .warning-text {
      color: #c53030; /* Darker red */
      font-size: 0.875rem;
      font-weight: 500;
      background-color: #fff5f5;
      border-left: 4px solid #e53e3e;
      padding: 0.75rem 1rem;
      border-radius: 4px;
    }
  
    .modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 0.75rem;
      padding: 1rem 1.25rem;
      border-top: 1px solid #edf2f7;
      background-color: #f9fafb; /* Slightly off-white */
      flex-shrink: 0; /* Prevent footer from shrinking */
    }
  
    .cancel-button {
      padding: 0.5rem 1rem; /* More padding */
      background-color: white;
      border: 1px solid #d1d5db; /* Slightly darker border */
      border-radius: 6px;
      font-size: 0.875rem;
      font-weight: 500;
      color: #374151; /* Darker grey */
      cursor: pointer;
      transition: background-color 0.2s ease, border-color 0.2s ease;
    }
    .cancel-button:hover {
        background-color: #f9fafb;
        border-color: #adb5bd;
    }
  
    .confirm-delete-button {
      padding: 0.5rem 1rem;
      background-color: #e53e3e;
      color: white;
      border: none;
      border-radius: 6px;
      font-size: 0.875rem;
      font-weight: 500;
      cursor: pointer;
      transition: background-color 0.2s ease, opacity 0.2s ease;
      /* Add space for spinner */
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
    }
  
    .confirm-delete-button:hover:not(:disabled) {
      background-color: #c53030;
    }
  
    .confirm-delete-button:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
  
    /* Responsive adjustments */
    @media (max-width: 768px) {
     /* No changes needed for form-grid/secret-info as they now use auto-fit */
  
      .card-header {
          /* Allow title and search to stack nicely */
          flex-direction: column;
          align-items: flex-start;
      }
      .search-container {
          max-width: none; /* Allow search to take full width */
      }
  
      .secret-actions {
        justify-content: flex-start; /* Align buttons left on small screens */
        padding-top: 1rem;
      }
      .copy-feedback {
          width: 100%; /* Take full width */
          margin-right: 0;
          margin-bottom: 0.5rem; /* Space below feedback */
          text-align: left;
      }
      .modal-actions {
          flex-direction: column-reverse; /* Stack buttons vertically, primary action last */
          gap: 0.5rem;
      }
      .cancel-button, .confirm-delete-button {
          width: 100%; /* Full width buttons in modal */
          justify-content: center; /* Center text in button */
      }
    }
  
  </style>