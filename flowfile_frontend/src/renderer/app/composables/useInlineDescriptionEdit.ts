import { nextTick, ref } from "vue";
import { ElMessage } from "element-plus";

interface DescriptionEntity {
  id: number;
  description?: string | null;
}

export interface UseInlineDescriptionEditOptions {
  save: (id: number, description: string | null) => Promise<void>;
  errorMessage?: string;
}

export function useInlineDescriptionEdit(opts: UseInlineDescriptionEditOptions) {
  const editingId = ref<number | null>(null);
  const editValue = ref("");
  const inputRef = ref<HTMLInputElement | null>(null);

  function start(entity: DescriptionEntity) {
    editingId.value = entity.id;
    editValue.value = entity.description ?? "";
    nextTick(() => inputRef.value?.focus());
  }

  function cancel() {
    editingId.value = null;
  }

  async function save(entity: DescriptionEntity) {
    if (editingId.value !== entity.id) return;
    const trimmed = editValue.value.trim();
    const previous = entity.description ?? "";
    editingId.value = null;
    if (trimmed === previous) return;
    try {
      await opts.save(entity.id, trimmed || null);
    } catch (e: any) {
      ElMessage.error(
        e?.response?.data?.detail ?? opts.errorMessage ?? "Failed to update description",
      );
    }
  }

  return { editingId, editValue, inputRef, start, cancel, save };
}
