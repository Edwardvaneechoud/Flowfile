// secretManager/composables/useSecrets.ts
import { ref, computed } from 'vue';
import type { Ref } from 'vue';
import { fetchSecretsApi, addSecretApi, getSecretValueApi, deleteSecretApi } from './secretApi';
import type { Secret, SecretInput } from './secretTypes';

export function useSecretManager() {
  const secrets: Ref<Secret[]> = ref([]);
  const isLoading = ref(true);
  const searchTerm = ref('');
  const visibleSecrets = ref<string[]>([]);
  const copyMessage = ref('');

  // Filtered and sorted secrets
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

  // Load secrets from API
  const loadSecrets = async () => {
    isLoading.value = true;
    visibleSecrets.value = []; // Reset visibility on reload
    try {
      secrets.value = await fetchSecretsApi();
    } catch (error) {
      console.error('Failed to load secrets:', error);
      secrets.value = []; // Clear secrets on error
      throw error; // Allow caller to handle notification
    } finally {
      isLoading.value = false;
    }
  };

  // Add a new secret
  const addSecret = async (secretInput: SecretInput) => {
    // Basic validation: Check if secret name already exists (case-sensitive)
    if (secrets.value.some(s => s.name === secretInput.name)) {
      throw new Error(`Secret with name "${secretInput.name}" already exists.`);
    }

    try {
      await addSecretApi({ ...secretInput }); // Pass a copy
      await loadSecrets(); // Reload the list after adding
      return secretInput.name; // Return name for success feedback
    } catch (error) {
      console.error('Failed to add secret:', error);
      throw error; // Allow caller to handle notification
    }
  };

  // Toggle secret visibility icon
  const toggleSecretVisibility = (secretName: string) => {
    const index = visibleSecrets.value.indexOf(secretName);
    if (index === -1) {
      visibleSecrets.value.push(secretName);
    } else {
      visibleSecrets.value.splice(index, 1);
    }
  };

  // Copy secret value to clipboard
  const copySecretToClipboard = async (secretName: string) => {
    copyMessage.value = ''; // Clear previous message
    try {
      const secretValue = await getSecretValueApi(secretName);
      await navigator.clipboard.writeText(secretValue);
      copyMessage.value = `Value for '${secretName}' copied!`;
      
      // Auto-clear message after delay
      setTimeout(() => {
        copyMessage.value = '';
      }, 2500);
      
      return true;
    } catch (error) {
      console.error('Failed to copy secret:', error);
      copyMessage.value = `Failed to copy ${secretName}.`;
      
      // Auto-clear error message after delay
      setTimeout(() => {
        copyMessage.value = '';
      }, 3000);
      
      throw error; // Allow caller to handle notification
    }
  };

  // Delete a secret
  const deleteSecret = async (secretName: string) => {
    try {
      await deleteSecretApi(secretName);
      await loadSecrets(); // Refresh the list
      return secretName; // Return name for success feedback
    } catch (error) {
      console.error('Failed to delete secret:', error);
      throw error; // Allow caller to handle notification
    }
  };

  return {
    secrets,
    filteredSecrets,
    isLoading,
    searchTerm,
    visibleSecrets,
    copyMessage,
    loadSecrets,
    addSecret,
    toggleSecretVisibility,
    copySecretToClipboard,
    deleteSecret
  };
}