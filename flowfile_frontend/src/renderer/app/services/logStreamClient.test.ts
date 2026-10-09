import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../../config/constants", () => ({ flowfileCorebaseURL: "http://127.0.0.1:63578/" }));

import {
  LogStreamHttpError,
  RUN_START_WAIT_SECONDS,
  readSseData,
  streamFlowLogs,
} from "./logStreamClient";

const streamOf = (...chunks: (string | Uint8Array)[]): ReadableStream<Uint8Array> => {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(typeof chunk === "string" ? encoder.encode(chunk) : chunk);
      }
      controller.close();
    },
  });
};

const collect = async (body: ReadableStream<Uint8Array>): Promise<string[]> => {
  const seen: string[] = [];
  await readSseData(body, (data) => seen.push(data));
  return seen;
};

describe("readSseData", () => {
  it("reassembles events split across chunks, including a split separator", async () => {
    const seen = await collect(streamOf('data: "fir', 'st"\n', '\ndata: "second"\n\n'));
    expect(seen).toEqual(['"first"', '"second"']);
  });

  it("accepts CRLF separators and skips comment-only blocks", async () => {
    const seen = await collect(streamOf(': keepalive\r\n\r\ndata: "a"\r\n\r\n'));
    expect(seen).toEqual(['"a"']);
  });

  it("joins multi-line data and drops an unterminated trailing event", async () => {
    const seen = await collect(streamOf("data: one\ndata:two\n\ndata: cut off"));
    expect(seen).toEqual(["one\ntwo"]);
  });

  it("decodes a multi-byte character split between chunks", async () => {
    const bytes = new TextEncoder().encode('data: "€"\n\n');
    const seen = await collect(streamOf(bytes.slice(0, 8), bytes.slice(8)));
    expect(seen).toEqual(['"€"']);
  });
});

describe("streamFlowLogs", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("sends the token as a Bearer header and never in the URL", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(streamOf('data: "line 1"\n\n', 'data: "line 2"\n\n')));
    vi.stubGlobal("fetch", fetchMock);
    const onOpen = vi.fn();
    const seen: string[] = [];

    await streamFlowLogs({
      flowId: 7,
      token: "jwt-value",
      signal: new AbortController().signal,
      onOpen,
      onData: (data) => seen.push(JSON.parse(data)),
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(String(url)).toBe("http://127.0.0.1:63578/logs/7");
    expect(init.headers.Authorization).toBe("Bearer jwt-value");
    expect(onOpen).toHaveBeenCalledOnce();
    expect(seen).toEqual(["line 1", "line 2"]);
  });

  it("asks core to wait for the run it is about to follow, and only then", async () => {
    const fetchMock = vi.fn().mockImplementation(() => Promise.resolve(new Response(streamOf())));
    vi.stubGlobal("fetch", fetchMock);
    const base = { flowId: 7, token: "jwt-value", signal: new AbortController().signal, onData: vi.fn() };

    await streamFlowLogs({ ...base, waitForRun: true });
    await streamFlowLogs(base);

    expect(String(fetchMock.mock.calls[0][0])).toBe(
      `http://127.0.0.1:63578/logs/7?wait_for_run=${RUN_START_WAIT_SECONDS}`,
    );
    expect(String(fetchMock.mock.calls[1][0])).toBe("http://127.0.0.1:63578/logs/7");
  });

  it("rejects a non-2xx response without opening", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("nope", { status: 401 })));
    const onOpen = vi.fn();

    const attempt = streamFlowLogs({
      flowId: 7,
      token: "jwt-value",
      signal: new AbortController().signal,
      onOpen,
      onData: vi.fn(),
    });

    await expect(attempt).rejects.toBeInstanceOf(LogStreamHttpError);
    await expect(attempt).rejects.toMatchObject({ status: 401 });
    expect(onOpen).not.toHaveBeenCalled();
  });

  it("rejects when aborted mid-stream", async () => {
    const controller = new AbortController();
    const body = new ReadableStream<Uint8Array>({
      start(stream) {
        stream.enqueue(new TextEncoder().encode('data: "first"\n\n'));
        controller.signal.addEventListener("abort", () => stream.error(controller.signal.reason));
      },
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(body)));

    const attempt = streamFlowLogs({
      flowId: 7,
      token: "jwt-value",
      signal: controller.signal,
      onData: () => controller.abort(),
    });

    await expect(attempt).rejects.toMatchObject({ name: "AbortError" });
  });
});
