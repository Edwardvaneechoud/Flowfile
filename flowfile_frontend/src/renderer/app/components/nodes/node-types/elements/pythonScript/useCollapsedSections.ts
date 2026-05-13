import { reactive, computed, watch, type WritableComputedRef } from "vue";

const STORAGE_KEY = "flowfile.pythonScript.sections";

export type SectionId = "kernel" | "outputs" | "artifacts";

type CollapsedState = Record<SectionId, boolean>;

const DEFAULT_STATE: CollapsedState = {
  kernel: true,
  outputs: true,
  artifacts: true,
};

function readFromStorage(): CollapsedState {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return { ...DEFAULT_STATE };
    const parsed = JSON.parse(raw) as Partial<Record<SectionId, unknown>>;
    return {
      kernel: typeof parsed.kernel === "boolean" ? parsed.kernel : DEFAULT_STATE.kernel,
      outputs: typeof parsed.outputs === "boolean" ? parsed.outputs : DEFAULT_STATE.outputs,
      artifacts:
        typeof parsed.artifacts === "boolean" ? parsed.artifacts : DEFAULT_STATE.artifacts,
    };
  } catch {
    return { ...DEFAULT_STATE };
  }
}

export function useCollapsedSections(): {
  collapsed: CollapsedState;
  activeNames: WritableComputedRef<string[]>;
} {
  const collapsed = reactive<CollapsedState>(readFromStorage());

  watch(
    collapsed,
    (next) => {
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
      } catch {
        // localStorage unavailable (private mode, quota) — ignore
      }
    },
    { deep: true },
  );

  // el-collapse v-model takes the array of EXPANDED panel names. We use string[]
  // (not SectionId[]) for the wire type so it lines up with Element Plus's signature.
  const activeNames = computed<string[]>({
    get() {
      return (Object.keys(collapsed) as SectionId[]).filter((id) => !collapsed[id]);
    },
    set(next) {
      const expanded = new Set(next);
      (Object.keys(collapsed) as SectionId[]).forEach((id) => {
        collapsed[id] = !expanded.has(id);
      });
    },
  });

  return { collapsed, activeNames };
}
