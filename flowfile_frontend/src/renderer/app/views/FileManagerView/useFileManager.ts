import { ref, computed } from "vue";
import type { Ref } from "vue";
import { FileManagerApi } from "../../api/fileManager.api";
import type { ManagedFile } from "../../api/fileManager.api";

export function useFileManager() {
  const files: Ref<ManagedFile[]> = ref([]);
  const isLoading = ref(true);
  const searchTerm = ref("");

  const filteredFiles = computed(() => {
    const sorted = [...files.value].sort((a, b) =>
      a.name.localeCompare(b.name),
    );
    if (!searchTerm.value) return sorted;
    const term = searchTerm.value.toLowerCase();
    return sorted.filter((f) => f.name.toLowerCase().includes(term));
  });

  const loadFiles = async () => {
    isLoading.value = true;
    try {
      files.value = await FileManagerApi.listFiles();
    } catch (error) {
      console.error("Failed to load files:", error);
      files.value = [];
      throw error;
    } finally {
      isLoading.value = false;
    }
  };

  const uploadFile = async (
    file: File,
    onProgress?: (percent: number) => void,
  ) => {
    const result = await FileManagerApi.uploadFile(file, onProgress);
    await loadFiles();
    return result;
  };

  const deleteFile = async (filename: string) => {
    await FileManagerApi.deleteFile(filename);
    await loadFiles();
    return filename;
  };

  return {
    files,
    filteredFiles,
    isLoading,
    searchTerm,
    loadFiles,
    uploadFile,
    deleteFile,
  };
}
