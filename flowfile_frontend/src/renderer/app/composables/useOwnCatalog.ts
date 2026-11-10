import { ref } from "vue";
import { CatalogApi } from "../api";
import { offersOwnCatalog } from "../views/CatalogView/hubRows";

const offered = ref(true);

/**
 * Whether this Flowfile's own catalog is a place a flow is saved or registered in. Core says no on a server that
 * joined a hub, where the organisation's catalog is the hub's; the answer is shared and re-read by `refresh`.
 */
export function useOwnCatalog() {
  const refresh = async (): Promise<boolean> => {
    try {
      offered.value = offersOwnCatalog(await CatalogApi.getNamespaceTree());
    } catch (error) {
      console.error("Failed to read the catalog tree", error);
    }
    return offered.value;
  };
  return { ownCatalogOffered: offered, refreshOwnCatalog: refresh };
}
