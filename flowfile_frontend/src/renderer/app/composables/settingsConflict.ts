/**
 * A settings save core refused with 409 `NODE_SETTINGS_CHANGED`: the node's settings moved in another
 * window since this drawer read them (`NodeData.settings_fingerprint`, echoed by the save as
 * `expected_settings_fingerprint`). The user picks between the draft and the live settings; a stale
 * drawer never overwrites a push or a save made elsewhere on its own.
 */
import { ElMessage, ElMessageBox } from "element-plus";
import { useEditorStore } from "../stores/editor-store";
import { useNodeStore } from "../stores/node-store";
import { extractSaveErrorMessage } from "./saveError";

export const NODE_SETTINGS_CHANGED = "NODE_SETTINGS_CHANGED";

export type ConflictChoice = "keep-mine" | "discard-mine" | "cancel";
export type ConflictOutcome = "saved" | "discarded" | "kept-open";

export function isSettingsConflict(error: unknown): boolean {
  const response = (error as { response?: { status?: number; data?: { detail?: unknown } } })
    ?.response;
  const detail = response?.data?.detail as { code?: string } | null | undefined;
  return response?.status === 409 && detail?.code === NODE_SETTINGS_CHANGED;
}

/** The fingerprint this drawer's node data loaded with, while the store still holds that node. */
export function loadedSettingsFingerprint(nodeId: number | string): string | undefined {
  const data = useNodeStore().nodeData;
  if (!data || Number(data.node_id) !== Number(nodeId)) return undefined;
  return data.settings_fingerprint ?? undefined;
}

export async function askSettingsConflict(error: unknown): Promise<ConflictChoice> {
  try {
    await ElMessageBox.confirm(
      `${extractSaveErrorMessage(error)} Keep your changes and overwrite them, or discard yours?`,
      "Settings changed in another window",
      {
        confirmButtonText: "Keep mine",
        cancelButtonText: "Discard mine",
        distinguishCancelAndClose: true,
        type: "warning",
      },
    );
    return "keep-mine";
  } catch (action) {
    return action === "cancel" ? "discard-mine" : "cancel";
  }
}

/** Drop the open drawer and its node-data cache, so the next open of the node reads the live settings. */
export function discardDrawerDraft(): void {
  const editorStore = useEditorStore();
  const nodeStore = useNodeStore();
  editorStore.clearCloseFunction();
  editorStore.activeDrawerComponent = null;
  editorStore.isDrawerOpen = false;
  nodeStore.nodeId = -1;
  nodeStore.nodeData = null;
  ElMessage.info({
    message: "Your changes were discarded. Open the node again to see its current settings.",
    showClose: true,
  });
}

/**
 * Resolve a refused save. "Keep mine" runs `overwrite` (the same save without its expectation),
 * "Discard mine" runs `discard` (the drawer goes), closing the box keeps the drawer as it is.
 */
export async function resolveSettingsConflict(
  error: unknown,
  overwrite: () => Promise<void>,
  discard: () => void = discardDrawerDraft,
): Promise<ConflictOutcome> {
  const choice = await askSettingsConflict(error);
  if (choice === "keep-mine") {
    try {
      await overwrite();
      return "saved";
    } catch (retryError) {
      ElMessage.error({
        message: extractSaveErrorMessage(retryError),
        showClose: true,
        duration: 6000,
      });
      return "kept-open";
    }
  }
  if (choice === "discard-mine") {
    discard();
    return "discarded";
  }
  return "kept-open";
}
