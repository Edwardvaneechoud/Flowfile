import { useRouter } from "vue-router";
import { useFlowOpener } from "../../composables/useFlowOpener";
import { useCatalogStore } from "../../stores/catalog-store";
import { useNotebookStore } from "../../stores/notebook-store";
import type { CatalogTarget, SearchHitAction } from "./searchNavigation";

// Only the newest open (palette or results page) may navigate, so a slow refresh never pulls the user back.
let openSeq = 0;

/** Carries out a hit's action; resolves false when it could not (the flow opener has already said why). */
export function useOpenSearchHit() {
  const router = useRouter();
  const notebookStore = useNotebookStore();
  const catalogStore = useCatalogStore();
  const { openFlow } = useFlowOpener();

  // The catalog page reuses an overview up to 30 s old, which may predate the item search just found.
  async function refreshCatalogFor(target: CatalogTarget | undefined): Promise<void> {
    if (!target || !catalogStore.overviewLoaded || catalogStore.holdsItem(target.kind, target.id))
      return;
    await catalogStore.initialize(true);
  }

  async function run(action: SearchHitAction): Promise<boolean> {
    const mine = ++openSeq;
    const from = router.currentRoute.value.fullPath;
    const superseded = () => mine !== openSeq || router.currentRoute.value.fullPath !== from;
    switch (action.type) {
      case "route":
        await refreshCatalogFor(action.catalogTarget);
        if (superseded()) return false;
        await router.push(action.to);
        return true;
      case "notebook":
        // Same as the catalog tree's open: add the notebook's tab, then show the notebook tab.
        void notebookStore.openNotebook(action.notebookId);
        await router.push({ name: "catalog", query: { tab: "notebook" } });
        return true;
      case "designer": {
        const flowId = await openFlow(action.flowPath, {
          name: action.name,
          catalogRef: action.catalogRef,
          catalogId: action.catalogId,
        });
        if (flowId === null || superseded()) return false;
        await router.push({ name: "designer" });
        return true;
      }
    }
  }

  return { run };
}
