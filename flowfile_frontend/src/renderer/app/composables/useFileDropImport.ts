// Turns an OS file drop on the canvas into Read nodes: linked straight from disk
// on desktop, or uploaded to core first (with a confirmation dialog) everywhere else.
import { Position, useVueFlow, type XYPosition } from "@vue-flow/core";
import { ElMessage, ElNotification } from "element-plus";
import { computed, markRaw, nextTick, onMounted, onUnmounted, ref } from "vue";

import { FlowApi } from "../api";
import { FALLBACK_UPLOAD_LIMITS, FileManagerApi, type UploadLimits } from "../api/fileManager.api";
import { useEditorStore } from "../stores/editor-store";
import { useFlowStore } from "../stores/flow-store";
import { useNodeStore } from "../stores/node-store";
import { useTutorialStore } from "../stores/tutorial-store";
import type { OperationResponse } from "../types";
import {
  resolveDropStrategy,
  summarizeDragTypes,
  type DragSummary,
} from "../utils/fileDropStrategy";
import { DEFAULT_OUTPUT_HANDLE } from "../utils/outputHandle";
import {
  buildReceivedTable,
  detectFileType,
  extensionOf,
  READ_EXTENSIONS,
  type ReadFileType,
} from "../utils/readFileTypes";
import { getComponent, getId, getNodeTemplateByItem } from "./useDragAndDrop";
import { canLinkDroppedFiles, desktop, isDesktop, isMacDesktop } from "../../lib/desktop";

const MAX_FILES_PER_DROP = 10;
const STAGGER_X = 40;
const STAGGER_Y = 96;
// WKWebView can swallow the terminal dragleave, so the hint self-clears.
const DRAG_WATCHDOG_MS = 2000;
const PANEL_SELECTOR = ".overlay";
const CANVAS_SELECTOR = ".vue-flow";

export type DropPhase = "idle" | "confirm" | "uploading" | "error";
export type EntryStatus = "pending" | "uploading" | "done" | "error" | "skipped";

export interface DroppedFileEntry {
  key: string;
  file: File | null;
  name: string;
  size: number;
  ext: string | null;
  fileType: ReadFileType;
  status: EntryStatus;
  progress: number;
  error: string;
  serverPath: string;
  serverName: string;
}

interface ClassifiedFile {
  file: File;
  index: number;
  fileType: ReadFileType;
  ext: string | null;
}

interface DropSnapshot {
  files: File[];
  directories: string[];
  uriList: string;
}

interface ReadNodeItem {
  name: string;
  path: string;
  fileType: ReadFileType;
  ext: string | null;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value < 10 ? value.toFixed(1) : Math.round(value)} ${units[unit]}`;
}

function plural(count: number, word: string): string {
  return `${count} ${word}${count === 1 ? "" : "s"}`;
}

function hasFiles(event: DragEvent): boolean {
  return Array.from(event.dataTransfer?.types ?? []).includes("Files");
}

function fileItemTypes(dt: DataTransfer): string[] {
  return Array.from(dt.items)
    .filter((item) => item.kind === "file")
    .map((item) => item.type);
}

/** Reads every DataTransfer field the handler needs before the browser neuters it. */
function snapshotDrop(dt: DataTransfer): DropSnapshot {
  const items = Array.from(dt.items);
  // Synthetic DataTransfers return null from webkitGetAsEntry, hence strict equality.
  const dropped = Array.from(dt.files).map((file, index) => ({
    file,
    isDirectory: items[index]?.webkitGetAsEntry?.()?.isDirectory === true,
  }));
  return {
    files: dropped.filter((d) => !d.isDirectory).map((d) => d.file),
    directories: dropped.filter((d) => d.isDirectory).map((d) => d.file.name),
    uriList: dt.getData("text/uri-list"),
  };
}

function classifyFiles(files: File[]): { supported: ClassifiedFile[]; skipped: string[] } {
  const supported: ClassifiedFile[] = [];
  const skipped: string[] = [];
  files.forEach((file, index) => {
    const fileType = detectFileType(file.name);
    if (fileType) supported.push({ file, index, fileType, ext: extensionOf(file.name) });
    else skipped.push(file.name);
  });
  return { supported, skipped };
}

function uploadErrorMessage(error: unknown): string {
  const detail = (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail;
  return detail || (error as Error)?.message || "Upload failed";
}

function uploadSummaryMessage(done: number, failed: number, cancelled: number): string {
  const parts = [`${plural(done, "Read node")} added`];
  if (failed) parts.push(`${failed} failed`);
  if (cancelled) parts.push(`${cancelled} cancelled before upload`);
  return `${parts.join(", ")}.`;
}

export function useFileDropImport() {
  const { addNodes, removeNodes, screenToFlowCoordinate } = useVueFlow();
  const editorStore = useEditorStore();
  const flowStore = useFlowStore();
  const nodeStore = useNodeStore();

  const phase = ref<DropPhase>("idle");
  const isDragActive = ref(false);
  const dragSummary = ref<DragSummary>({ fileCount: 0, typeLabels: [], allUnknown: true });
  const entries = ref<DroppedFileEntry[]>([]);
  const limits = ref<UploadLimits>(FALLBACK_UPLOAD_LIMITS);

  const totalBytes = computed(() => entries.value.reduce((sum, e) => sum + e.size, 0));
  const overallProgress = computed(() => {
    const tracked = entries.value.filter((e) => e.status !== "skipped");
    if (tracked.length === 0) return 0;
    const settled = (e: DroppedFileEntry) =>
      e.status === "pending" || e.status === "uploading" ? e.progress : 100;
    return Math.round(tracked.reduce((sum, e) => sum + settled(e), 0) / tracked.length);
  });
  const uploadTargetKind = computed<"local" | "server">(() => (isDesktop ? "local" : "server"));

  let dropPoint: XYPosition = { x: 0, y: 0 };
  let activeController: AbortController | null = null;
  let dragDepth = 0;
  let watchdog: ReturnType<typeof setTimeout> | null = null;
  let limitsRequested = false;
  let dragPathsPrefetch: Promise<string[]> | null = null;
  let resolving = false;

  function resetDrag() {
    dragDepth = 0;
    isDragActive.value = false;
    if (watchdog) clearTimeout(watchdog);
    watchdog = null;
    dragPathsPrefetch = null;
  }

  function armWatchdog() {
    if (watchdog) clearTimeout(watchdog);
    watchdog = setTimeout(resetDrag, DRAG_WATCHDOG_MS);
  }

  async function ensureLimits() {
    if (limitsRequested) return;
    limitsRequested = true;
    limits.value = await FileManagerApi.getLimits();
  }

  function markDragActive() {
    isDragActive.value = true;
    void ensureLimits();
    // Read the pasteboard while the drag session is provably live, not after the drop.
    if (isMacDesktop && !dragPathsPrefetch) dragPathsPrefetch = desktop.readDragPaths();
  }

  function handleDragEnter(event: DragEvent) {
    if (!hasFiles(event)) return;
    dragDepth += 1;
    markDragActive();
  }

  function handleDragOver(event: DragEvent): boolean {
    const dt = event.dataTransfer;
    if (!dt || !hasFiles(event)) return false;
    event.preventDefault();
    dt.dropEffect = canLinkDroppedFiles ? "link" : "copy";
    dragSummary.value = summarizeDragTypes(fileItemTypes(dt));
    if (!isDragActive.value) {
      dragDepth = 1;
      markDragActive();
    }
    armWatchdog();
    return true;
  }

  function handleDragLeave(event: DragEvent) {
    if (!hasFiles(event)) return;
    dragDepth = Math.max(0, dragDepth - 1);
    if (dragDepth === 0) resetDrag();
  }

  function handleDragEnd() {
    resetDrag();
  }

  /** Drops landing on a floating panel would otherwise create the node underneath it. */
  function resolveDropPoint(event: DragEvent): XYPosition {
    const canvas = document.querySelector(CANVAS_SELECTOR);
    if (canvas && (event.target as HTMLElement | null)?.closest?.(PANEL_SELECTOR)) {
      const rect = canvas.getBoundingClientRect();
      return screenToFlowCoordinate({
        x: rect.left + rect.width / 2,
        y: rect.top + rect.height / 2,
      });
    }
    return screenToFlowCoordinate({ x: event.clientX, y: event.clientY });
  }

  function buildEntry({ file, index, fileType, ext }: ClassifiedFile): DroppedFileEntry {
    const tooLarge = file.size > limits.value.max_file_size_bytes;
    return {
      key: `${index}-${file.name}`,
      file,
      name: file.name,
      size: file.size,
      ext,
      fileType,
      status: tooLarge ? "error" : "pending",
      progress: 0,
      error: tooLarge ? `Over the ${formatBytes(limits.value.max_file_size_bytes)} limit` : "",
      serverPath: "",
      serverName: "",
    };
  }

  /** Returns true when the drop was an OS file drop this composable consumed. */
  function handleDrop(event: DragEvent): boolean {
    const dt = event.dataTransfer;
    if (!dt || !hasFiles(event)) return false;
    event.preventDefault();
    event.stopPropagation();
    const prefetchedPaths = dragPathsPrefetch;
    resetDrag();

    const { files, directories, uriList } = snapshotDrop(dt);
    const point = resolveDropPoint(event);

    if (editorStore.isRunning) {
      ElMessage.warning("Wait for the run to finish before adding files.");
      return true;
    }
    if (flowStore.flowId <= 0) {
      ElMessage.warning("Open or create a flow before dropping files.");
      return true;
    }
    if (files.length + directories.length === 0) return true;
    if (directories.length > 0) {
      ElMessage.error(`Folders can't be added: ${directories.join(", ")}. Drop files instead.`);
      if (files.length === 0) return true;
    }
    if (files.length > MAX_FILES_PER_DROP) {
      ElMessage.warning(`Drop at most ${MAX_FILES_PER_DROP} files at a time.`);
      return true;
    }

    const { supported, skipped } = classifyFiles(files);
    if (supported.length === 0) {
      ElMessage.error(
        `Nothing readable here. Supported: ${READ_EXTENSIONS.map((e) => `.${e}`).join(" ")}`,
      );
      return true;
    }
    if (skipped.length > 0) {
      ElMessage.warning(`Skipped ${skipped.join(", ")} — unsupported file type.`);
    }
    if (phase.value !== "idle" || resolving) {
      ElMessage.info("Finish the current file import first.");
      return true;
    }

    resolving = true;
    void resolveDrop(files, supported, uriList, point, prefetchedPaths);
    return true;
  }

  /** Async tail of handleDrop: awaits whichever native path source this platform has. */
  async function resolveDrop(
    files: File[],
    supported: ClassifiedFile[],
    uriList: string,
    point: XYPosition,
    prefetchedPaths: Promise<string[]> | null,
  ): Promise<void> {
    try {
      let nativePaths: string[] = [];
      if (isMacDesktop) nativePaths = await (prefetchedPaths ?? desktop.readDragPaths());
      else if (isDesktop) nativePaths = await desktop.requestDroppedFilePaths(files);
      const strategy = resolveDropStrategy({
        isDesktop,
        uriList,
        fileNames: files.map((f) => f.name),
        nativePaths,
      });
      console.debug(
        `[file-drop] ${files.length} file(s), uri-list ${uriList ? "present" : "empty"}, ` +
          `native ${nativePaths.length} path(s) → ${strategy.kind}`,
      );
      if (strategy.kind === "link") {
        const items = supported.map((c) => ({
          name: c.file.name,
          path: strategy.paths[c.index],
          fileType: c.fileType,
          ext: c.ext,
        }));
        const added = await createReadNodes(items, point);
        if (added > 0) ElMessage.success(`Added ${plural(added, "Read node")}.`);
        return;
      }

      entries.value = supported.map(buildEntry);
      dropPoint = point;
      phase.value = "confirm";
    } finally {
      resolving = false;
    }
  }

  async function uploadEntry(entry: DroppedFileEntry): Promise<void> {
    if (!entry.file) return;
    const controller = new AbortController();
    activeController = controller;
    entry.status = "uploading";
    entry.progress = 0;
    try {
      const response = await FileManagerApi.uploadFile(
        entry.file,
        (percent) => {
          entry.progress = percent;
        },
        { unique: true, signal: controller.signal },
      );
      entry.serverName = response.filename;
      entry.serverPath = response.filepath;
      entry.status = "done";
      entry.progress = 100;
    } catch (error) {
      if (controller.signal.aborted) {
        entry.status = "skipped";
      } else {
        entry.status = "error";
        entry.error = uploadErrorMessage(error);
      }
    }
  }

  function countByStatus(status: EntryStatus): number {
    return entries.value.filter((e) => e.status === status).length;
  }

  function notifyUploadSummary() {
    const done = countByStatus("done");
    const failed = countByStatus("error");
    const cancelled = countByStatus("skipped");
    if (failed === 0 && cancelled === 0) return;
    ElNotification({
      title: "File import finished",
      message: uploadSummaryMessage(done, failed, cancelled),
      type: failed > 0 ? "warning" : "info",
      duration: 6000,
    });
  }

  async function confirmUpload(): Promise<void> {
    if (phase.value !== "confirm") return;
    phase.value = "uploading";
    for (const entry of entries.value) {
      if (entry.status === "pending") await uploadEntry(entry);
    }
    activeController = null;

    const uploaded = entries.value
      .filter((e) => e.status === "done")
      .map((e) => ({ name: e.name, path: e.serverPath, fileType: e.fileType, ext: e.ext }));
    await createReadNodes(uploaded, dropPoint);

    phase.value = countByStatus("error") > 0 ? "error" : "idle";
    notifyUploadSummary();
  }

  function cancelUpload() {
    activeController?.abort();
    activeController = null;
    for (const entry of entries.value) {
      if (entry.status === "pending") entry.status = "skipped";
    }
  }

  function cancelDialog() {
    if (phase.value !== "confirm" && phase.value !== "error") return;
    phase.value = "idle";
    entries.value = [];
  }

  async function createReadNodes(items: ReadNodeItem[], point: XYPosition): Promise<number> {
    if (items.length === 0) return 0;
    const flowId = flowStore.flowId;
    const nodeTemplate = await getNodeTemplateByItem("read");
    if (!nodeTemplate) {
      ElMessage.error("The Read node is not available.");
      return 0;
    }
    const component = await getComponent(nodeTemplate);

    let added = 0;
    let lastResponse: OperationResponse | undefined;
    for (const [index, item] of items.entries()) {
      const position = { x: point.x + index * STAGGER_X, y: point.y + index * STAGGER_Y };
      // The shared counter: a local one would collide with existing node ids.
      const nodeId = getId();
      try {
        const response = await FlowApi.insertNode(flowId, nodeId, "read", position.x, position.y);
        addNodes({
          id: String(nodeId),
          type: "custom-node",
          position,
          data: {
            id: nodeId,
            label: nodeTemplate.name,
            component: markRaw(component),
            inputs: [],
            outputs: [{ id: DEFAULT_OUTPUT_HANDLE, position: Position.Right }],
            nodeTemplate,
          },
        });
        useTutorialStore().notify({ type: "node-added", nodeItem: "read", nodeId });
        await nextTick();
        await nodeStore.updateSettingsDirectly({
          flow_id: flowId,
          node_id: nodeId,
          pos_x: position.x,
          pos_y: position.y,
          cache_results: false,
          is_setup: true,
          received_file: buildReceivedTable(item),
        });
        lastResponse = response;
        added += 1;
      } catch (error) {
        console.error("Failed to add a Read node for", item.name, error);
        removeNodes([String(nodeId)]);
        ElMessage.error(`Could not add a Read node for "${item.name}".`);
      }
    }
    if (lastResponse?.history) flowStore.updateHistoryState(lastResponse.history);
    return added;
  }

  onMounted(() => window.addEventListener("blur", resetDrag));
  onUnmounted(() => {
    window.removeEventListener("blur", resetDrag);
    resetDrag();
  });

  return {
    phase,
    isDragActive,
    dragSummary,
    entries,
    limits,
    totalBytes,
    overallProgress,
    uploadTargetKind,
    handleDragEnter,
    handleDragOver,
    handleDragLeave,
    handleDragEnd,
    handleDrop,
    confirmUpload,
    cancelDialog,
    cancelUpload,
  };
}

export type FileDropImport = ReturnType<typeof useFileDropImport>;
