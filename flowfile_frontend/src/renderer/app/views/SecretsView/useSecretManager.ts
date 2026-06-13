import { ref, computed } from "vue";
import type { Ref } from "vue";
import { fetchSecretsApi, addSecretApi, getSecretValueApi, deleteSecretApi } from "./secretApi";
import type { Secret, SecretInput } from "./secretTypes";

export function useSecretManager() {
  const secrets: Ref<Secret[]> = ref([]);
  const isLoading = ref(true);
  const searchTerm = ref("");
  const visibleSecrets = ref<string[]>([]);
  const copyMessage = ref("");

  const filteredSecrets = computed(() => {
    const sortedSecrets = [...secrets.value].sort((a, b) => a.name.localeCompare(b.name));
    if (!searchTerm.value) {
      return sortedSecrets;
    }
    const term = searchTerm.value.toLowerCase();
    return sortedSecrets.filter((secret) => secret.name.toLowerCase().includes(term));
  });

  const loadSecrets = async () => {
    isLoading.value = true;
    visibleSecrets.value = [];
    try {
      secrets.value = await fetchSecretsApi();
    } catch (error) {
      console.error("Failed to load secrets:", error);
      secrets.value = [];
      throw error;
    } finally {
      isLoading.value = false;
    }
  };

  const addSecret = async (secretInput: SecretInput) => {
    // Only own secrets block creation — a same-named group-shared secret is
    // shadowed by your own (the backend resolves own-first), so don't reject it.
    if (secrets.value.some((s) => s.name === secretInput.name && s.access?.is_owner !== false)) {
      throw new Error(`Secret with name "${secretInput.name}" already exists.`);
    }

    try {
      await addSecretApi({ ...secretInput });
      await loadSecrets();
      return secretInput.name;
    } catch (error) {
      console.error("Failed to add secret:", error);
      throw error;
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
    copyMessage.value = "";
    try {
      const secretValue = await getSecretValueApi(secretName);
      await navigator.clipboard.writeText(secretValue);
      copyMessage.value = `Value for '${secretName}' copied!`;

      setTimeout(() => {
        copyMessage.value = "";
      }, 2500);

      return true;
    } catch (error) {
      console.error("Failed to copy secret:", error);
      copyMessage.value = `Failed to copy ${secretName}.`;

      setTimeout(() => {
        copyMessage.value = "";
      }, 3000);

      throw error;
    }
  };

  const deleteSecret = async (secretName: string) => {
    try {
      await deleteSecretApi(secretName);
      await loadSecrets();
      return secretName;
    } catch (error) {
      console.error("Failed to delete secret:", error);
      throw error;
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
    deleteSecret,
  };
}
