import { distinctName } from "../CatalogView/chartNaming";

export const NEW_DASHBOARD_NAME = "Untitled dashboard";

/** The name a new dashboard opens with: distinct from the library's names when it is loaded. */
export function newDashboardName(libraryNames: Iterable<string>): string {
  return distinctName(NEW_DASHBOARD_NAME, libraryNames);
}

/** `pending`: not on the server, nothing sent; `failed`: the last create was refused or lost. */
export type CreationPhase = "pending" | "creating" | "created" | "failed";

export interface DashboardCreation<P, T extends { id: number }> {
  phase(): CreationPhase;
  /** The server id once created; null before. */
  id(): number | null;
  isCreated(): boolean;
  /**
   * Sends the create; a call while one is in flight gets that same request, so one create is sent
   * however many callers ask. A failure leaves the phase `failed`, and the next call sends again.
   */
  create(payload: P): Promise<T>;
}

export interface DashboardCreationOptions<P, T extends { id: number }> {
  /** An existing dashboard's id; omit for one that is not on the server yet. */
  id?: number | null;
  send: (payload: P) => Promise<T>;
  onChange?: (phase: CreationPhase, id: number | null) => void;
}

/** Tracks a dashboard from "not created yet" through its one create request. */
export function createDashboardCreation<P, T extends { id: number }>(
  opts: DashboardCreationOptions<P, T>,
): DashboardCreation<P, T> {
  let id: number | null = opts.id ?? null;
  let phase: CreationPhase = id === null ? "pending" : "created";
  let inFlight: Promise<T> | null = null;

  const setPhase = (next: CreationPhase) => {
    phase = next;
    opts.onChange?.(phase, id);
  };

  const create = (payload: P): Promise<T> => {
    if (phase === "created") {
      return Promise.reject(new Error("The dashboard is already created"));
    }
    if (inFlight) return inFlight;
    setPhase("creating");
    const request = (async () => {
      try {
        const created = await opts.send(payload);
        id = created.id;
        setPhase("created");
        return created;
      } catch (err) {
        setPhase("failed");
        throw err;
      } finally {
        inFlight = null;
      }
    })();
    inFlight = request;
    return request;
  };

  return {
    phase: () => phase,
    id: () => id,
    isCreated: () => phase === "created",
    create,
  };
}
