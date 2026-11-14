import { ref } from "vue";
import { CatalogApi } from "../api";
import { useHubConnectionStore } from "../stores/hub-connection-store";
import { offersOwnCatalog } from "../views/CatalogView/hubRows";

const offered = ref(true);

/** Whether a flow is saved or registered in this Flowfile's own catalog: not on a server that joined a hub. */
export function useOwnCatalog() {
  const refresh = async (): Promise<boolean> => {
    const hub = useHubConnectionStore();
    if (hub.connection === null) await hub.load();
    if (hub.connection !== null && !hub.joined) {
      offered.value = true;
      return true;
    }
    try {
      offered.value = offersOwnCatalog(await CatalogApi.getNamespaceTree());
    } catch (error) {
      console.error("Failed to read the catalog tree", error);
    }
    return offered.value;
  };
  return { ownCatalogOffered: offered, refreshOwnCatalog: refresh };
}
