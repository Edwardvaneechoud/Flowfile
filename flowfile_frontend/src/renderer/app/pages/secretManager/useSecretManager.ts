// src/components/secrets/useSecretManager.ts

import { ref, computed, onMounted, type Ref } from 'vue';
import type { Secret, SecretInput } from './secretTypes';
import { fetchSecretsApi, addSecretApi, getSecretValueApi, deleteSecretApi } from './secretApi';

export function useSecretManager() {
  const secrets: Ref<Secret[]> = ref([]);
  const newSecret = ref<SecretInput>({ name: '', value: '' });
  const isLoading = ref(true);
  const isSubmitting = ref(false);
  const isDeleting = ref(false);
  const showNewSecret = ref(false);
  const visibleSecrets = ref<string[]>([]);
  const searchTerm = ref('');
  const showDeleteModal = ref(false);
  const secretToDelete = ref('');
  const copyMessage = ref('');

  const filteredSecrets = computed(() => {
    if (!searchTerm.value) {
        return [...secrets.value].sort((a, b) => a.name.localeCompare(b.name));
    }
    const term = searchTerm.value.toLowerCase();
    const filtered = secrets.value.filter(secret =>
      secret.name.toLowerCase().includes(term)
    );
    // Also sort the filtered results
    return filtered.sort((a, b) => a.name.localeCompare(b.name));
  });

  // Methods
  const loadSecrets = async () => {
    isLoading.value = true;
    visibleSecrets.value = []; // Reset visibility on reload
    try {
      secrets.value = await fetchSecretsApi();
      // toast.success('Secrets loaded successfully');
    } catch (error) {
      console.error('Failed to load secrets:', error);
      secrets.value = []; // Clear secrets on error
    } finally {
      isLoading.value = false;
    }
  };

  const addSecret = async () => {
    if (!newSecret.value.name || !newSecret.value.value) return;

    if (secrets.value.some(s => s.name === newSecret.value.name)) {
         alert(`Secret with name "${newSecret.value.name}" already exists.`);
         return;
    }

    isSubmitting.value = true;
    try {
      await addSecretApi(newSecret.value);
      await loadSecrets(); // Reload the list after adding
      newSecret.value = { name: '', value: '' }; // Reset form
      showNewSecret.value = false; // Hide password field
    } catch (error: any) {
      console.error('Failed to add secret:', error);
      alert(error.message || 'An unknown error occurred while adding the secret.'); // Display specific error
    } finally {
      isSubmitting.value = false;
    }
  };

  const toggleSecretVisibility = (secretName: string) => {
    const index = visibleSecrets.value.indexOf(secretName);
    if (index === -1) {
      visibleSecrets.value.push(secretName);
    } else {
      visibleSecrets.value.splice(index, 1);
    }
  };

  const copySecretToClipboard = async (secretName: string) => {
    try {
      const secretValue = await getSecretValueApi(secretName);

      await navigator.clipboard.writeText(secretValue);

      copyMessage.value = `Value for '${secretName}' copied!`;
      setTimeout(() => {
        copyMessage.value = '';
      }, 2500);

    } catch (error) {
      console.error('Failed to copy secret:', error);
      alert('Failed to retrieve or copy secret value.');
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
      const nameToDelete = secretToDelete.value;
      await deleteSecretApi(nameToDelete);
      await loadSecrets();
      showDeleteModal.value = false;
      secretToDelete.value = '';
    } catch (error) {
      console.error('Failed to delete secret:', error);
      alert('Failed to delete secret. Please try again.');
    } finally {
      isDeleting.value = false;
    }
  };

  onMounted(() => {
    loadSecrets();
  });

  // Return everything the component template needs
  return {
    secrets,
    newSecret,
    isLoading,
    isSubmitting,
    isDeleting,
    showNewSecret,
    visibleSecrets,
    searchTerm,
    showDeleteModal,
    secretToDelete,
    copyMessage,
    filteredSecrets,
    addSecret,
    toggleSecretVisibility,
    copySecretToClipboard,
    confirmDelete,
    cancelDelete, // Expose cancel function for modal overlay click
    deleteSecret,
    loadSecrets // Expose if manual refresh is needed
  };
}