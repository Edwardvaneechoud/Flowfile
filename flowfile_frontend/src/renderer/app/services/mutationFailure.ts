import { ElMessage } from "element-plus";
import { detailMessage } from "../composables/saveError";
import { useFlowStore } from "../stores/flow-store";
import { isRefusedMutation } from "./mutationChannel";

/** A failed graph mutation: say why, then resync the canvas from core instead of patching it back. */
export function recoverFromFailedMutation(error: unknown, fallback: string): void {
  console.error(fallback, error);
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  ElMessage.error(detail ? detailMessage(error, fallback) : fallback);
  // The mutation channel already resynced a request core refused.
  if (!isRefusedMutation(error)) useFlowStore().requestReload();
}
