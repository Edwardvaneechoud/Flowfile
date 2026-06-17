import { ref, watch, type Ref } from "vue";

const KEY_PREFIX = "flowfile.section.";

/**
 * Open/closed state for a single collapsible section.
 *
 * When `persistKey` is provided the state is remembered across reloads in
 * localStorage (namespaced under `flowfile.section.`), mirroring the guarded
 * read/write approach in pythonScript/useCollapsedSections.ts. Without a key it
 * is plain in-memory state seeded from `defaultOpen`.
 */
export function useSectionToggle(
  persistKey: string | undefined,
  defaultOpen: boolean,
): { open: Ref<boolean>; toggle: () => void } {
  const open = ref(readInitial(persistKey, defaultOpen));

  if (persistKey) {
    watch(open, (next) => {
      try {
        localStorage.setItem(KEY_PREFIX + persistKey, next ? "1" : "0");
      } catch {
        // localStorage unavailable (private mode, quota) — ignore
      }
    });
  }

  function toggle() {
    open.value = !open.value;
  }

  return { open, toggle };
}

function readInitial(persistKey: string | undefined, defaultOpen: boolean): boolean {
  if (!persistKey) return defaultOpen;
  try {
    const raw = localStorage.getItem(KEY_PREFIX + persistKey);
    if (raw === "1") return true;
    if (raw === "0") return false;
    return defaultOpen;
  } catch {
    return defaultOpen;
  }
}
