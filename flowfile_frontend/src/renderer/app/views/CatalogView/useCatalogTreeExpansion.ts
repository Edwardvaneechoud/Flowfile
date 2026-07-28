import { reactive } from "vue";

import { loadTreeExpansion, persistTreeExpansion } from "./catalogTreeState";

// Shared across all CatalogTreeNode / TreeSection instances; hydrated once.
const state = reactive<Record<string, boolean>>({});
// Search-time expansion is derived from the query, so it is deliberately kept out
// of the persisted map. Only an explicit toggle writes here; a new query clears it.
const searchState = reactive<Record<string, boolean>>({});
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

  // While searching, `fallback` is "does this subtree match" — the user can still
  // override it per node, but nothing here survives the search.
  const isExpandedDuringSearch = (key: string, fallback: boolean): boolean =>
    key in searchState ? searchState[key] : fallback;

  const setExpandedDuringSearch = (key: string, value: boolean): void => {
    searchState[key] = value;
  };

  const resetSearchOverrides = (): void => {
    for (const key of Object.keys(searchState)) delete searchState[key];
  };

  return {
    isExpanded,
    setExpanded,
    isExpandedDuringSearch,
    setExpandedDuringSearch,
    resetSearchOverrides,
  };
}
