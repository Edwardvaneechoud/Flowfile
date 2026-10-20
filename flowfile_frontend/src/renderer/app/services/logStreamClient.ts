/** Core's flow-log SSE stream (GET logs/{flow_id}) read with fetch, so the JWT rides in the Authorization header, not the URL. */

import { flowfileCorebaseURL } from "../../config/constants";

export class LogStreamHttpError extends Error {
  constructor(public readonly status: number) {
    super(`HTTP ${status}`);
    this.name = "LogStreamHttpError";
  }
}

const EVENT_SEPARATOR = /\r?\n\r?\n/;

/** The joined `data:` lines of one SSE event block, or null for a comment-only block. */
const eventData = (block: string): string | null => {
  const lines = block
    .split(/\r?\n/)
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(line.startsWith("data: ") ? 6 : 5));
  return lines.length > 0 ? lines.join("\n") : null;
};

/** Hands each event's raw `data` payload to `onData` until the body ends. */
export const readSseData = async (
  body: ReadableStream<Uint8Array>,
  onData: (data: string) => void,
): Promise<void> => {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let separator = EVENT_SEPARATOR.exec(buffer);
      while (separator) {
        const data = eventData(buffer.slice(0, separator.index));
        buffer = buffer.slice(separator.index + separator[0].length);
        if (data !== null) onData(data);
        separator = EVENT_SEPARATOR.exec(buffer);
      }
    }
  } finally {
    reader.releaseLock();
  }
};

export interface FlowLogStreamOptions {
  flowId: number;
  token: string;
  signal: AbortSignal;
  onOpen?: () => void;
  onData: (data: string) => void;
}

/**
 * Resolves when core closes the stream: at the run's end for a running flow, right after the file for
 * an idle one. Rejects on a non-2xx response, a network error or an abort.
 */
export const streamFlowLogs = async ({
  flowId,
  token,
  signal,
  onOpen,
  onData,
}: FlowLogStreamOptions): Promise<void> => {
  const response = await fetch(new URL(`logs/${flowId}`, flowfileCorebaseURL), {
    headers: { Accept: "text/event-stream", Authorization: `Bearer ${token}` },
    signal,
  });
  if (!response.ok || !response.body) throw new LogStreamHttpError(response.status);
  onOpen?.();
  await readSseData(response.body, onData);
};
