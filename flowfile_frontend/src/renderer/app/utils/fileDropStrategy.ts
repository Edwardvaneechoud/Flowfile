// Decides what a dropped OS file becomes: a linked filesystem path (desktop, when
// either the webview or the shell can name the paths) or an upload to core. Pure
// and DOM-free — drop handlers pass in the already-extracted drop values.
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

// Spike 2026-08-04: on macOS WKWebView getData("text/uri-list") is "" for OS file
// drops — WebKit exposes only http/https/data/blob schemes. Kept for browsers and
// webviews that do expose it; desktop linking uses desktop.readDragPaths().
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
  /** Paths the desktop shell read off the OS drag pasteboard (see desktop.readDragPaths). */
  nativePaths?: readonly string[];
}

/**
 * Pairs paths with the dropped files by basename, in `fileNames` order. Returns
 * null when the counts differ, when a name has no match, or when basenames repeat
 * — a repeated basename makes the mapping ambiguous and we never guess.
 */
export function pairPaths(paths: readonly string[], fileNames: readonly string[]): string[] | null {
  if (paths.length !== fileNames.length || paths.length === 0) return null;
  const names = paths.map(baseNameOf);
  // Repeats on either side make the mapping ambiguous — never guess.
  if (new Set(names).size !== names.length) return null;
  if (new Set(fileNames).size !== fileNames.length) return null;
  if (names.every((name, i) => name === fileNames[i])) return [...paths];

  const byName = new Map(names.map((name, i) => [name, paths[i]]));
  const reordered: string[] = [];
  for (const name of fileNames) {
    const path = byName.get(name);
    if (path === undefined) return null;
    reordered.push(path);
  }
  return reordered;
}

/**
 * Links only when a path source pairs up unambiguously with the dropped files.
 * The uri-list is tried first (browsers and non-WebKit webviews), then the paths
 * the desktop shell read natively. Anything else uploads to core instead.
 */
export function resolveDropStrategy({
  isDesktop,
  uriList,
  fileNames,
  nativePaths = [],
}: DropContext): DropStrategy {
  if (fileNames.length === 0) return { kind: "none" };
  if (!isDesktop) return { kind: "upload" };

  const paths =
    pairPaths(parseFileUriList(uriList), fileNames) ?? pairPaths(nativePaths, fileNames);
  return paths ? { kind: "link", paths } : { kind: "upload" };
}

export function shouldBlockStrayDrop(types: readonly string[]): boolean {
  return types.includes("Files");
}
