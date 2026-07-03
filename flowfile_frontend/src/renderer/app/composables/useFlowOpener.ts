import { ElNotification } from "element-plus";
import { FlowApi } from "../api";
import { useNodeStore } from "../stores/column-store";
import { useRecentFlows } from "./useRecentFlows";

const notifyError = (title: string, message: string) =>
  ElNotification({ title, message, type: "error", position: "top-left" });

// Shared open-flow contract for HomeView and DesignerView: record the flow in
// recents on a successful open, PRUNE it on failure — moved/deleted files must
// not keep resurfacing on the home screen.
export function useFlowOpener() {
  const nodeStore = useNodeStore();
  const { recordFlow, removeFlow } = useRecentFlows();

  async function openFlow(
    flowPath: string,
    meta?: { name?: string; catalogRef?: string; catalogId?: number },
  ): Promise<number | null> {
    if (meta?.catalogId != null) {
      try {
        const openSession = (await FlowApi.getAllFlows()).find(
          (s) => s.source_registration_id != null && s.source_registration_id === meta.catalogId,
        );
        if (openSession) {
          nodeStore.setFlowId(openSession.flow_id);
          recordFlow({
            path: flowPath,
            name: meta.name,
            catalogRef: meta.catalogRef,
            catalogId: meta.catalogId,
          });
          return openSession.flow_id;
        }
      } catch {
        /* fall through to import */
      }
    }
    try {
      const flowId = await FlowApi.importFlow(flowPath);
      // importFlow is typed Promise<number> but the backend can return an
      // empty body — guard both.
      if (flowId === undefined || flowId === null) {
        removeFlow(flowPath);
        notifyError(
          "Couldn't open flow",
          `Server returned no flow id for ${flowPath}. The file may be missing or unreadable.`,
        );
        return null;
      }
      // setFlowId triggers the Canvas watcher which loads the flow.
      nodeStore.setFlowId(flowId);
      recordFlow({
        path: flowPath,
        name: meta?.name,
        catalogRef: meta?.catalogRef,
        catalogId: meta?.catalogId,
      });
      return flowId;
    } catch (error: any) {
      removeFlow(flowPath);
      const detail = error?.response?.data?.detail ?? error?.message ?? String(error);
      notifyError("Couldn't open flow", `Failed to open ${flowPath}: ${detail}`);
      return null;
    }
  }

  return { openFlow };
}
