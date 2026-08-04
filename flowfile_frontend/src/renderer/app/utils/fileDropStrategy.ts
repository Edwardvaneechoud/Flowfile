// Decides what a dropped OS file becomes: a linked filesystem path (desktop,
// when the webview exposes text/uri-list) or an upload to core. Pure and
// DOM-free — drop handlers pass in the already-extracted DataTransfer values.
import { baseNameOf } from "./readFileTypes";

export interface DragSummary {
  fileCount: number;
  typeLabels: string[];
  allUnknown: boolean;
}

// Windows reports plain CSVs as application/vnd.ms-excel, hence the vaguer label.
const MIME_LABELS: Readonly<Record<string, string>> = Object.freeze({
  "text/csv": "CSV",
  "text/tab-separated-values": "TSV",
  "text/plain": "Text",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "Excel",
  "application/vnd.ms-excel": "Spreadsheet",
});

// Item names are unreadable during dragover, so the hint is built from MIME types alone.
export function summarizeDragTypes(itemTypes: readonly string[]): DragSummary {
  const labels = itemTypes
    .map((type) => MIME_LABELS[type.trim().toLowerCase()])
    .filter((label): label is string => Boolean(label));
  const typeLabels = [...new Set(labels)];
  return { fileCount: itemTypes.length, typeLabels, allUnknown: typeLabels.length === 0 };
}

function percentDecode(value: string): string | null {
  try {
    return decodeURIComponent(value);
  } catch {
    return null;
  }
}

function fileUriToPath(uri: string): string | null {
  if (!/^file:\/\//i.test(uri)) return null;
  const rest = percentDecode(uri.slice("file://".length));
  if (!rest) return null;
  if (!rest.startsWith("/")) return `//${rest}`;
  return /^\/[a-zA-Z]:/.test(rest) ? rest.slice(1) : rest;
}

export function parseFileUriList(text: string): string[] {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"))
    .map(fileUriToPath)
    .filter((path): path is string => path !== null);
}

export type DropStrategy =
  | { kind: "link"; paths: string[] }
  | { kind: "upload" }
  | { kind: "none" };

export interface DropContext {
  isDesktop: boolean;
  uriList: string;
  fileNames: readonly string[];
}

/**
 * Links only when the uri-list pairs up unambiguously with the dropped files:
 * equal counts and matching basenames in order. Anything else uploads rather
 * than guessing which path belongs to which file.
 */
export function resolveDropStrategy({ isDesktop, uriList, fileNames }: DropContext): DropStrategy {
  if (fileNames.length === 0) return { kind: "none" };
  if (!isDesktop) return { kind: "upload" };

  const paths = parseFileUriList(uriList);
  const pairsUp =
    paths.length === fileNames.length &&
    paths.every((path, i) => baseNameOf(path) === fileNames[i]);
  return pairsUp ? { kind: "link", paths } : { kind: "upload" };
}

export function shouldBlockStrayDrop(types: readonly string[]): boolean {
  return types.includes("Files");
}
