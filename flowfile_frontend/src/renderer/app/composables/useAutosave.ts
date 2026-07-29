// Thin Vue seam over the pure autosave engine; all logic lives in autosaveEngine.ts.
import { onBeforeUnmount, ref, type Ref } from "vue";
import {
  createAutosaveEngine,
  type AutosaveEngine,
  type AutosaveEngineOptions,
  type AutosaveState,
} from "./autosaveEngine";

export function useAutosave<P>(opts: AutosaveEngineOptions<P>): {
  engine: AutosaveEngine;
  state: Ref<AutosaveState>;
} {
  const state = ref<AutosaveState>({
    status: "idle",
    lastSavedAt: null,
    lastError: null,
    blockKind: null,
    retryCount: 0,
    dirtySince: null,
  });
  const engine = createAutosaveEngine<P>({
    ...opts,
    onChange: (next) => {
      state.value = next;
      opts.onChange?.(next);
    },
  });
  state.value = engine.state();
  onBeforeUnmount(() => engine.dispose());
  return { engine, state };
}
