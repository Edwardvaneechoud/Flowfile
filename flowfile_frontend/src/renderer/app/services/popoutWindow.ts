/**
 * The notebook pop-out window: one flow's canvas notebook at `#/notebook?flow=<id>`, opened from the
 * designer's code dock as a second browser window or a native desktop window. Pure helpers only, so
 * the client id (read at module load) and the views can share them without pulling in the stores.
 */

export const POPOUT_ROUTE_PATH = "/notebook";

/** True for the hash of a pop-out window's URL (`#/notebook?flow=3`). */
export const isPopoutHash = (hash: string): boolean =>
  hash === `#${POPOUT_ROUTE_PATH}` || hash.startsWith(`#${POPOUT_ROUTE_PATH}?`);

/** The `window.open` target name of a flow's pop-out: reopening focuses it instead of adding one. */
export const notebookWindowName = (flowId: number): string => `flowfile-notebook-${flowId}`;

/** The pop-out URL for this page's origin and path (web mode; the desktop shell builds its own). */
export const notebookWindowUrl = (
  flowId: number,
  location: { origin: string; pathname: string },
): string => `${location.origin}${location.pathname}#${POPOUT_ROUTE_PATH}?flow=${flowId}`;

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

export const windowTitle = (label: string | null | undefined): string =>
  label ? `Notebook – ${label}` : "Notebook";
