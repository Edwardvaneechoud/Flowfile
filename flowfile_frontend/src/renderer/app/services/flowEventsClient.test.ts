import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../../config/constants", () => ({ flowfileCorebaseURL: "http://127.0.0.1:63578/" }));

import { FlowEventsHttpError, streamFlowEvents, type FlowEvent } from "./flowEventsClient";

const streamOf = (...chunks: string[]): ReadableStream<Uint8Array> => {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  });
};

describe("streamFlowEvents", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("sends the token and the client id as headers, never in the URL, and parses the events", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        new Response(
          streamOf(
            ": keepalive\n\n",
            'id: 3\nevent: hello\ndata: {"kind":"hello","flow_id":7,"revision":3,"is_running":false}\n\n',
            'data: {"kind":"graph","flow_id":7,"revision":4,"origin":"tab-a"}\n\n',
            "data: not json\n\n",
          ),
        ),
      );
    vi.stubGlobal("fetch", fetchMock);
    const onOpen = vi.fn();
    const seen: FlowEvent[] = [];

    await streamFlowEvents({
      flowId: 7,
      token: "jwt-value",
      clientId: "window-1",
      signal: new AbortController().signal,
      onOpen,
      onEvent: (event) => seen.push(event),
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(String(url)).toBe("http://127.0.0.1:63578/editor/events?flow_id=7");
    expect(init.headers.Authorization).toBe("Bearer jwt-value");
    expect(init.headers["X-Flowfile-Client"]).toBe("window-1");
    expect(onOpen).toHaveBeenCalledOnce();
    expect(seen).toEqual([
      { kind: "hello", flow_id: 7, revision: 3, is_running: false },
      { kind: "graph", flow_id: 7, revision: 4, origin: "tab-a" },
    ]);
  });

  it("rejects a non-2xx response without opening", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("nope", { status: 404 })));
    const onOpen = vi.fn();

    const attempt = streamFlowEvents({
      flowId: 7,
      token: "jwt-value",
      clientId: "window-1",
      signal: new AbortController().signal,
      onOpen,
      onEvent: vi.fn(),
    });

    await expect(attempt).rejects.toBeInstanceOf(FlowEventsHttpError);
    await expect(attempt).rejects.toMatchObject({ status: 404 });
    expect(onOpen).not.toHaveBeenCalled();
  });
});
