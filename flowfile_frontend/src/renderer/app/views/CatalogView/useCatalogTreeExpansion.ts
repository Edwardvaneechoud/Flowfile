import { reactive } from "vue";

import { loadTreeExpansion, persistTreeExpansion } from "./catalogTreeState";

// Shared across all CatalogTreeNode / TreeSection instances; hydrated once.
const state = reactive<Record<string, boolean>>({});
let hydrated = false;

export function useCatalogTreeExpansion() {
  if (!hydrated) {
    Object.assign(state, loadTreeExpansion());
    hydrated = true;
  }

  // `key in state` so an explicit persisted `false` beats a `true` default.
  const isExpanded = (key: string, fallback: boolean): boolean =>
    key in state ? state[key] : fallback;

  const setExpanded = (key: string, value: boolean): void => {
    state[key] = value;
    persistTreeExpansion(state);
  };

  return { isExpanded, setExpanded };
}
