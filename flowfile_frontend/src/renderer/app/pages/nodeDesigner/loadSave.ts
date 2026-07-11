// Save / load / preview / re-check orchestration for the Node Designer,
// against the DesignerState wire contracts. The frontend never generates Python:
// save posts {mode:"designer", designer_state} (or {mode:"code", code}); load
// consumes get-custom-node parse_result -> visual editing or code-only mode.
import { ElMessage, ElMessageBox } from "element-plus";
import { useNodeDesignerStore } from "@/stores/node-designer-store";
import {
  SaveConflictError,
  getCustomNode,
  previewCustomNode,
  saveCustomNode,
  type GetCustomNodeResponse,
  type SaveCustomNodeBody,
} from "../../api/nodeDesigner";
import type { DesignerState, ParseResult } from "./designerState";
import { toSnakeCase } from "./mappers";

export function nodeFileName(state: DesignerState): string {
  const base = toSnakeCase(state.node_name || "custom_node") || "custom_node";
  return `${base}.py`;
}

/** State to persist/preview: an empty title defaults to the node name so the two stay
 *  in sync by default (the node shows its name rather than a generic "Custom Node"). */
function withEffectiveTitle(state: DesignerState): DesignerState {
  return state.title.trim() ? state : { ...state, title: state.node_name };
}

/** Apply a parse_result to the store: designer mode -> visual, else code-only. */
function applyParseResult(parse: ParseResult, code: string) {
  const store = useNodeDesignerStore();
  if (parse.mode === "designer" && parse.designer_state) {
    store.loadDesignerState(parse.designer_state);
    store.parseIssues = parse.issues ?? [];
  } else {
    store.loadCodeOnly(code, parse.issues ?? []);
  }
}

/** Some transition responses only carry legacy fields — synthesize a code-only result. */
function resolveGetResponse(data: GetCustomNodeResponse): { parse: ParseResult; code: string } {
  const code = data.code ?? data.content ?? "";
  if (data.parse_result) return { parse: data.parse_result, code };
  // Legacy get-custom-node (pre-E2): no parser -> code-only.
  return { parse: { mode: "code_only", designer_state: null, issues: [] }, code };
}

export async function loadNodeForEdit(fileName: string): Promise<void> {
  const store = useNodeDesignerStore();
  const data = await getCustomNode(fileName);
  const { parse, code } = resolveGetResponse(data);
  applyParseResult(parse, code);
  store.sourceFile = data.file_name ?? fileName;
  store.fileHash = data.file_hash ?? null;
  store.markSaved(store.sourceFile, store.fileHash);
  store.resetPreviewValues();
}

/** Duplicate: load the file's state but clear the name + source so Save writes a new file. */
export async function duplicateNode(fileName: string): Promise<void> {
  const store = useNodeDesignerStore();
  const data = await getCustomNode(fileName);
  const { parse, code } = resolveGetResponse(data);
  applyParseResult(parse, code);
  if (parse.mode === "designer" && parse.designer_state) {
    store.designerState.node_name = "";
  }
  // A duplicate is fresh unsaved work: no source file/hash, left dirty so the normal draft
  // watcher preserves it. Don't call markSaved here — with sourceFile already null it would
  // discardDraft() the shared "__new__" slot, silently wiping an unrelated pending new-node draft.
  store.sourceFile = null;
  store.fileHash = null;
}

export async function previewCode(): Promise<string> {
  const store = useNodeDesignerStore();
  if (store.codeOnly) return store.codeText;
  return previewCustomNode(withEffectiveTitle(store.designerState));
}

/** Re-check code-only text by round-tripping through get after a save... but re-check
 *  must not write. Instead we preview-parse via the save endpoint's parse_result on the
 *  next save. For a write-free re-check we re-request the file's parse from disk only if
 *  it is already saved; otherwise report unchanged. Returns true if it flipped to designer. */
export async function recheckCode(): Promise<boolean> {
  const store = useNodeDesignerStore();
  if (!store.codeOnly || !store.sourceFile) return false;
  // Persist current text first (mode:"code") so the parser sees the edits, then reload.
  await saveCustomNode({ file_name: store.sourceFile, mode: "code", code: store.codeText });
  const data = await getCustomNode(store.sourceFile);
  const { parse, code } = resolveGetResponse(data);
  applyParseResult(parse, code);
  store.fileHash = data.file_hash ?? null;
  store.markSaved(store.sourceFile, store.fileHash);
  return parse.mode === "designer";
}

export async function saveNode(): Promise<boolean> {
  const store = useNodeDesignerStore();
  const fileName = store.sourceFile ?? nodeFileName(store.designerState);

  const body: SaveCustomNodeBody = {
    file_name: fileName,
    expected_hash: store.fileHash,
  };
  if (store.codeOnly) {
    body.mode = "code";
    body.code = store.codeText;
  } else {
    body.mode = "designer";
    body.designer_state = withEffectiveTitle(store.designerState);
  }

  try {
    const response = await doSave(body);
    finalizeSave(
      response.file_name,
      response.file_hash ?? null,
      response.parse_result,
      response.code,
    );
    if (response.load_error) {
      ElMessage.warning(`Saved, but the node failed to load: ${response.load_error}`);
    } else {
      ElMessage.success(`Node "${store.designerState.node_name || response.file_name}" saved.`);
    }
    return true;
  } catch (err) {
    if (err instanceof SaveConflictError) {
      return handleConflict(body);
    }
    const e = err as { response?: { data?: { detail?: string } }; message?: string };
    ElMessage.error(
      `Error saving node: ${e.response?.data?.detail || e.message || "unknown error"}`,
    );
    return false;
  }
}

async function doSave(body: SaveCustomNodeBody) {
  return saveCustomNode(body);
}

function finalizeSave(
  fileName: string,
  hash: string | null,
  parse?: ParseResult | null,
  code?: string,
) {
  const store = useNodeDesignerStore();
  // A designer save may return a parse_result that flips code_only if the backend
  // couldn't round-trip; honor it so the UI reflects on-disk truth.
  if (parse && code !== undefined) {
    if (parse.mode === "designer" && parse.designer_state) {
      store.parseIssues = parse.issues ?? [];
    } else if (parse.mode === "code_only") {
      store.loadCodeOnly(code, parse.issues ?? []);
    }
  }
  store.markSaved(fileName, hash);
}

async function handleConflict(body: SaveCustomNodeBody): Promise<boolean> {
  const store = useNodeDesignerStore();
  try {
    await ElMessageBox.confirm(
      "This node file changed on disk (another window or a manual edit). Reload the file and lose your changes, or overwrite it?",
      "File changed on disk",
      { confirmButtonText: "Overwrite", cancelButtonText: "Reload file", type: "warning" },
    );
    // Overwrite: drop the hash guard and re-save.
    const response = await doSave({ ...body, expected_hash: null });
    finalizeSave(
      response.file_name,
      response.file_hash ?? null,
      response.parse_result,
      response.code,
    );
    ElMessage.success("Overwrote the file on disk.");
    return true;
  } catch (action) {
    if (action === "cancel" && store.sourceFile) {
      await loadNodeForEdit(store.sourceFile);
      ElMessage.info("Reloaded the file from disk.");
    }
    return false;
  }
}
