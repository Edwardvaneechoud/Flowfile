/**
 * Pop-out windows: one flow's notebook, data preview, logs or AI assistant in its own window
 * (`#/notebook?flow=<id>`, `#/popout/<kind>?flow=<id>`), opened from the designer as a second
 * browser window or a native desktop window. Pure and dependency-free, so `desktop.ts`, the client
 * id (read at module load) and the views can share them without pulling in the stores.
 */

export const POPOUT_KINDS = ["notebook", "table", "logs", "ai"] as const;

export type PopoutKind = (typeof POPOUT_KINDS)[number];

export const isPopoutKind = (value: unknown): value is PopoutKind =>
  typeof value === "string" && (POPOUT_KINDS as readonly string[]).includes(value);

/** Each kind's window route. `clientId.ts` keys its storage on these, so every kind must be here. */
export const POPOUT_ROUTES: Record<PopoutKind, string> = {
  notebook: "/notebook",
  table: "/popout/table",
  logs: "/popout/logs",
  ai: "/popout/ai",
};

export const POPOUT_TITLES: Record<PopoutKind, string> = {
  notebook: "Notebook",
  table: "Data",
  logs: "Logs",
  ai: "AI Assistant",
};

/** True for the hash of a pop-out window's URL (`#/notebook?flow=3`, `#/popout/logs?flow=3`). */
export const isPopoutHash = (hash: string): boolean =>
  POPOUT_KINDS.some((kind) => {
    const route = `#${POPOUT_ROUTES[kind]}`;
    return hash === route || hash.startsWith(`${route}?`);
  });

/** The `window.open` target name of a flow's window of one kind: reopening focuses it instead of adding one. */
export const popoutWindowName = (kind: PopoutKind, flowId: number): string =>
  `flowfile-${kind}-${flowId}`;

/** The route hash of a flow's window of one kind (`#/notebook?flow=4`): what the desktop shell opens the window on. */
export const popoutWindowHash = (
  kind: PopoutKind,
  flowId: number,
  query: Record<string, string | number> = {},
): string => {
  const params = new URLSearchParams({ flow: String(flowId) });
  for (const [key, value] of Object.entries(query)) params.set(key, String(value));
  return `#${POPOUT_ROUTES[kind]}?${params}`;
};

/** The full window URL for this page's origin and path (web mode's `window.open`). */
export const popoutWindowUrl = (
  hash: string,
  location: { origin: string; pathname: string },
): string => `${location.origin}${location.pathname}${hash}`;

/** The flow id in the route query, or -1 when it is missing or not a positive integer. */
export const parseFlowQuery = (value: unknown): number => {
  const raw = Array.isArray(value) ? value[0] : value;
  if (typeof raw !== "string" && typeof raw !== "number") return -1;
  const id = Number(raw);
  return Number.isInteger(id) && id > 0 ? id : -1;
};

/** What the window calls the flow: the label the designer's tab bar shows. */
export const flowLabel = (flow: { name: string; display_name?: string | null }): string =>
  flow.display_name || flow.name;

export const windowTitle = (kind: PopoutKind, label: string | null | undefined): string =>
  label ? `${POPOUT_TITLES[kind]} – ${label}` : POPOUT_TITLES[kind];

/** What the designer sends a window that follows the canvas: the previewed node and the selection. */
export interface SelectionMessage {
  type: "selection";
  previewNodeId: number | null;
  selectedNodeIds: number[];
}

export type PopoutMessage = SelectionMessage;

export const isPopoutMessage = (value: unknown): value is PopoutMessage => {
  const message = value as Partial<SelectionMessage> | null;
  return (
    message?.type === "selection" &&
    (message.previewNodeId === null || typeof message.previewNodeId === "number") &&
    Array.isArray(message.selectedNodeIds) &&
    message.selectedNodeIds.every((id) => typeof id === "number")
  );
};
