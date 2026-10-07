/** Core's per-flow change feed (GET editor/events) read with fetch, so the JWT rides in a header, never the URL. */

import { flowfileCorebaseURL } from "../../config/constants";
import { CLIENT_HEADER } from "./clientId";
import { readSseData } from "./logStreamClient";

export type FlowEventKind = "hello" | "graph" | "run_started" | "run_ended" | "saved" | "closed";

export interface FlowEvent {
  kind: FlowEventKind;
  flow_id: number;
  /** The flow's revision after the change; null only for `closed`. */
  revision: number | null;
  /** The X-Flowfile-Client of the request that made the change; absent on `hello`. */
  origin?: string | null;
  /** Only on `hello`: whether the flow is running right now. */
  is_running?: boolean;
}

export class FlowEventsHttpError extends Error {
  constructor(public readonly status: number) {
    super(`HTTP ${status}`);
    this.name = "FlowEventsHttpError";
  }
}

export interface FlowEventsOptions {
  flowId: number;
  token: string;
  clientId: string;
  signal: AbortSignal;
  onOpen?: () => void;
  onEvent: (event: FlowEvent) => void;
}

/** Resolves when core ends the stream; rejects on a non-2xx response, a network error or an abort. */
export const streamFlowEvents = async ({
  flowId,
  token,
  clientId,
  signal,
  onOpen,
  onEvent,
}: FlowEventsOptions): Promise<void> => {
  const url = new URL("editor/events", flowfileCorebaseURL);
  url.searchParams.set("flow_id", String(flowId));
  const response = await fetch(url, {
    headers: {
      Accept: "text/event-stream",
      Authorization: `Bearer ${token}`,
      [CLIENT_HEADER]: clientId,
    },
    signal,
  });
  if (!response.ok || !response.body) throw new FlowEventsHttpError(response.status);
  onOpen?.();
  await readSseData(response.body, (data) => {
    let event: FlowEvent;
    try {
      event = JSON.parse(data);
    } catch {
      return;
    }
    if (event && typeof event.kind === "string") onEvent(event);
  });
};
