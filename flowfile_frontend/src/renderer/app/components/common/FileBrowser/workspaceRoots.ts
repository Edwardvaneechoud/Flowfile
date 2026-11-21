import { ref } from "vue";
import { normalizePath } from "../../../utils/storagePath";

/** A top folder of the file browser on a server: the person's Workspace, or the Shared folder. */
export interface BrowseRoot {
  name: string;
  path: string;
}

const trimmed = (p: string): string => {
  const n = normalizePath(p);
  return n.length > 1 && n.endsWith("/") ? n.slice(0, -1) : n;
};

/** The root that holds `path`, the deepest first (a workspace lies inside the shared folder). */
export function rootContaining(path: string, roots: BrowseRoot[]): BrowseRoot | null {
  const target = trimmed(path);
  const holding = roots.filter((root) => {
    const base = trimmed(root.path);
    return target === base || target.startsWith(base + "/");
  });
  return holding.sort((a, b) => trimmed(b.path).length - trimmed(a.path).length)[0] ?? null;
}

/** Whether `path` is one of the roots itself, where Up stops. */
export function isBrowseRoot(path: string, roots: BrowseRoot[]): boolean {
  const target = trimmed(path);
  return roots.some((root) => trimmed(root.path) === target);
}

/** The Save and Open dialogs' file tab: "Workspace" on a server, "File System" on a desktop. */
export function fileTabLabel(isMultiUser: boolean): string {
  return isMultiUser ? "Workspace" : "File System";
}

/**
 * Whether the dialogs' file tab offers "Also register in catalog": on a desktop that keeps its own catalog.
 * On a server a flow saved on the file tab is a file in no catalog; the Catalog tab saves one into a schema.
 */
export function fileTabRegisters(isMultiUser: boolean, ownCatalogOffered: boolean): boolean {
  return ownCatalogOffered && !isMultiUser;
}

/** What the file tab says on a server in place of the catalog option. */
export const WORKSPACE_FILE_NOTE =
  "A flow saved here is a file, in no catalog. The Catalog tab saves it into a schema.";

/**
 * Whether a browser offers the server's roots, Upload and Download: one of the server's disk that is not
 * held to one folder (`rootPath`), where a root's button would lead out of it.
 */
export function offersBrowseRoots(locationKind: string, rootPath: string | undefined): boolean {
  return locationKind === "local" && !rootPath;
}

/** Whether a file's name ends in one of `types`; every file matches an empty list. */
export function matchesFileTypes(name: string, types: string[]): boolean {
  const lower = name.toLowerCase();
  return types.length === 0 || types.some((type) => lower.endsWith(`.${type.toLowerCase()}`));
}

/**
 * Whether the browser lists a file. A server's folders hold files of any type, so there every file is listed
 * and one the dialog cannot pick is shown but not chosen; a desktop lists the dialog's types only.
 */
export function listsFile(name: string, types: string[], onServer: boolean): boolean {
  return onServer || matchesFileTypes(name, types);
}

/** The sentence a browser shows beside a listed file its dialog cannot pick. */
export function pickableTypesNote(types: string[]): string {
  const dotted = types.map((type) => `.${type}`);
  const named =
    dotted.length > 1
      ? `${dotted.slice(0, -1).join(", ")} and ${dotted[dotted.length - 1]}`
      : dotted[0];
  return `Only ${named} files can be chosen here.`;
}

/** The query that opens the editor's Open dialog on the person's workspace: `?open=workspace`. */
export const OPEN_WORKSPACE_QUERY = { key: "open", value: "workspace" } as const;

export function asksForWorkspace(query: Record<string, unknown>): boolean {
  return query[OPEN_WORKSPACE_QUERY.key] === OPEN_WORKSPACE_QUERY.value;
}

const workspaceRequested = ref(false);

/** Ask the next Open dialog that shows to open on its file tab (the editor's `?open=workspace`). */
export function requestWorkspaceOpen(): void {
  workspaceRequested.value = true;
}

/** Whether an Open dialog was asked to open on its file tab; the first dialog to show takes the request. */
export function takeWorkspaceRequest(): boolean {
  const asked = workspaceRequested.value;
  workspaceRequested.value = false;
  return asked;
}
