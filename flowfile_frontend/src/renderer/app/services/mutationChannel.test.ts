import axios, {
  AxiosError,
  type AxiosInstance,
  type AxiosResponse,
  type InternalAxiosRequestConfig,
} from "axios";
import { describe, expect, it, vi } from "vitest";
import type { HistoryState } from "../types";
import {
  createMutationQueue,
  historyOf,
  installMutationChannel,
  isOrderedRequest,
  isRefusedMutation,
} from "./mutationChannel";

const history = (flowId: number | null, undoCount = 1): HistoryState => ({
  flow_id: flowId,
  can_undo: undoCount > 0,
  can_redo: false,
  undo_description: null,
  redo_description: null,
  undo_count: undoCount,
  redo_count: 0,
});

describe("isOrderedRequest", () => {
  it.each([
    ["post", "/editor/add_node/"],
    ["post", "editor/copy_node"],
    ["post", "/editor/apply_operations/"],
    ["post", "/editor/undo/"],
    ["post", "/editor/update_layout/?flow_id=1"],
    ["post", "update_settings/"],
    ["post", "/node/description/"],
    ["post", "/node/reference/"],
    ["post", "/user_defined_components/update_user_defined_node"],
    ["post", "/flow_settings"],
    ["post", "/save_flow"],
    ["post", "/save_flow_to_catalog"],
    ["post", "/flow/apply_standard_layout/"],
    ["post", "/transform/manual_input"],
    ["post", "/flow/run/"],
    ["post", "/node/trigger_fetch_data"],
    ["POST", "http://127.0.0.1:63578/editor/delete_node/"],
  ])("orders %s %s", (method, url) => {
    expect(isOrderedRequest(method, url, "http://127.0.0.1:63578/")).toBe(true);
  });

  it.each([
    ["get", "/editor/history_status/"],
    ["get", "/node/description"],
    [undefined, "/editor/flow"],
    ["post", "/flow/cancel/"],
    ["post", "/editor/code_to_polars/exported"],
    ["post", "/editor/code_to_project/save"],
    ["post", "/ai/chat"],
    ["post", "/catalog/tables"],
    ["post", "/file_manager/upload"],
    ["post", "https://example.com/editor/add_node/"],
  ])("leaves %s %s unordered", (method, url) => {
    expect(isOrderedRequest(method, url, "http://127.0.0.1:63578/")).toBe(false);
  });
});

describe("historyOf", () => {
  it("returns a history that names its flow", () => {
    expect(historyOf({ success: true, history: history(3) })).toEqual(history(3));
  });

  it("ignores responses without a flow-scoped history", () => {
    expect(historyOf({ success: true, history: history(null) })).toBeNull();
    expect(historyOf({ success: true })).toBeNull();
    expect(historyOf(true)).toBeNull();
    expect(historyOf(null)).toBeNull();
  });
});

interface PendingCall {
  config: InternalAxiosRequestConfig;
  resolve: (data: unknown) => void;
  reject: (status: number) => void;
  /** Fail without a response (network error). */
  drop: () => void;
}

/** An axios instance whose requests stay open until the test settles them. */
const makeClient = (beforeChannel?: (client: AxiosInstance) => void) => {
  const calls: PendingCall[] = [];
  const client = axios.create({ baseURL: "http://core/" });
  client.defaults.adapter = (config) =>
    new Promise<AxiosResponse>((resolve, reject) => {
      calls.push({
        config,
        resolve: (data) => resolve({ data, status: 200, statusText: "OK", headers: {}, config }),
        reject: (status) =>
          reject(
            new AxiosError("failed", "ERR_BAD_RESPONSE", config, null, {
              data: { detail: "failed" },
              status,
              statusText: "",
              headers: {},
              config,
            } as AxiosResponse),
          ),
        drop: () => reject(new AxiosError("Network Error", "ERR_NETWORK", config)),
      });
    });
  beforeChannel?.(client);
  const queue = createMutationQueue();
  const applied: HistoryState[] = [];
  const refused: unknown[] = [];
  installMutationChannel(
    client,
    queue,
    (h) => applied.push(h),
    (error) => refused.push(error),
  );
  return { client, calls, queue, applied, refused };
};

const settle = () => new Promise((resolve) => setTimeout(resolve, 0));
const urls = (calls: PendingCall[]) => calls.map((call) => call.config.url);
const callTo = (calls: PendingCall[], url: string) => {
  const call = calls.find((c) => c.config.url === url);
  if (!call) throw new Error(`no request to ${url}`);
  return call;
};

describe("installMutationChannel", () => {
  it("never lets a later mutation overtake a slow earlier one", async () => {
    const { client, calls } = makeClient();
    const first = client.post("/editor/delete_node/");
    const second = client.post("/editor/undo/");
    await settle();
    expect(urls(calls)).toEqual(["/editor/delete_node/"]);

    calls[0].resolve({ success: true });
    await first;
    await settle();
    expect(urls(calls)).toEqual(["/editor/delete_node/", "/editor/undo/"]);
    calls[1].resolve({ success: true });
    await second;
  });

  it("does not hold back reads while a mutation is in flight", async () => {
    const { client, calls } = makeClient();
    void client.post("/editor/add_node/");
    void client.get("/flow_data/v2");
    await settle();
    expect(urls(calls).sort()).toEqual(["/editor/add_node/", "/flow_data/v2"]);
  });

  it("releases the slot when a mutation fails", async () => {
    const { client, calls, queue } = makeClient();
    const failing = client.post("/editor/connect_node/").catch((error) => error);
    const next = client.post("/update_settings/");
    await settle();
    calls[0].reject(422);
    expect(await failing).toBeInstanceOf(AxiosError);
    await settle();
    expect(urls(calls)).toEqual(["/editor/connect_node/", "/update_settings/"]);
    calls[1].resolve({ success: true });
    await next;
    await expect(queue.whenIdle()).resolves.toBeUndefined();
  });

  it("keeps the slot through a 401 retry and applies its history once", async () => {
    // Mirrors axios.config: the auth refresh interceptor is installed first.
    const { client, calls, applied } = makeClient((instance) => {
      instance.interceptors.response.use(undefined, (error: AxiosError) => {
        const config = error.config as InternalAxiosRequestConfig & { _retry?: boolean };
        if (error.response?.status !== 401 || config._retry) return Promise.reject(error);
        config._retry = true;
        return instance(config);
      });
    });

    const first = client.post("/editor/add_node/");
    const second = client.post("/editor/connect_node/");
    await settle();
    calls[0].reject(401);
    await settle();
    expect(urls(calls)).toEqual(["/editor/add_node/", "/editor/add_node/"]);
    calls[1].resolve({ success: true, history: history(2, 1) });
    await first;
    await settle();
    expect(urls(calls)).toEqual([
      "/editor/add_node/",
      "/editor/add_node/",
      "/editor/connect_node/",
    ]);
    expect(applied).toEqual([history(2, 1)]);
    calls[2].resolve({ success: true });
    await second;
  });

  it("frees exactly the slot whose request transform throws", async () => {
    const { client, calls } = makeClient();
    const boom = () => {
      throw new Error("not serializable");
    };
    const active = client.post("/editor/add_node/");
    // An unordered request failing the same way must not free the mutation in flight.
    const read = client.post("/ai/chat", {}, { transformRequest: [boom] }).catch((e) => e);
    const broken = client
      .post("/editor/connect_node/", {}, { transformRequest: [boom] })
      .catch((e) => e);
    const next = client.post("/editor/delete_node/");
    expect(await read).toBeInstanceOf(Error);
    await settle();
    expect(urls(calls)).toEqual(["/editor/add_node/"]);

    calls[0].resolve({ success: true });
    await active;
    expect(await broken).toBeInstanceOf(Error);
    await settle();
    expect(urls(calls)).toEqual(["/editor/add_node/", "/editor/delete_node/"]);
    calls[1].resolve({ success: true });
    await next;
  });

  it("sends a reserved slot's request ahead of everything enqueued after it", async () => {
    const { client, calls, queue } = makeClient();
    const reserved = queue.enqueue();
    const later = client.post("/editor/delete_node/");
    await settle();
    expect(calls).toHaveLength(0);

    const layout = client.post("/editor/update_layout/", {}, { mutationSlot: reserved });
    await settle();
    expect(urls(calls)).toEqual(["/editor/update_layout/"]);
    calls[0].resolve({ success: true });
    await layout;
    await settle();
    expect(urls(calls)).toEqual(["/editor/update_layout/", "/editor/delete_node/"]);
    calls[1].resolve({ success: true });
    await later;
  });

  it("lets the queue move on when a reservation is released unused", async () => {
    const { client, calls, queue } = makeClient();
    const reserved = queue.enqueue();
    const later = client.post("/editor/delete_node/");
    await settle();
    expect(calls).toHaveLength(0);
    expect(queue.release(reserved)).toBe(true);
    expect(queue.release(reserved)).toBe(false);
    await settle();
    expect(urls(calls)).toEqual(["/editor/delete_node/"]);
    calls[0].resolve({ success: true });
    await later;
  });

  it("frees a reserved slot before its request settles, so the caller's release is a no-op", async () => {
    const { client, calls, queue, applied, refused } = makeClient();
    const moved = queue.enqueue();
    const layout = client.post("/editor/update_layout/", {}, { mutationSlot: moved });
    await settle();
    calls[0].resolve({ success: true, history: history(4) });
    await layout;
    expect(queue.release(moved)).toBe(false);
    expect(applied).toEqual([history(4)]);

    const spliced = queue.enqueue();
    const failing = client
      .post("/editor/apply_operations/", {}, { mutationSlot: spliced })
      .catch((error) => error);
    await settle();
    calls[1].reject(422);
    expect(await failing).toBeInstanceOf(AxiosError);
    expect(queue.release(spliced)).toBe(false);
    expect(refused).toHaveLength(1);
  });

  it("applies each mutation response's history in order, and nothing else", async () => {
    const { client, calls, applied } = makeClient();
    const add = client.post("/editor/add_node/");
    const read = client.get("/editor/history_status/");
    await settle();
    callTo(calls, "/editor/history_status/").resolve({ history: history(7, 99) });
    await read;
    callTo(calls, "/editor/add_node/").resolve({ success: true, history: history(7, 1) });
    await add;
    expect(applied).toEqual([history(7, 1)]);
  });

  it("asks for a resync once for every ordered request core refused", async () => {
    const { client, calls, refused } = makeClient();
    const rename = client.post("/editor/update_group/").catch((error) => error);
    const read = client.post("/ai/chat").catch((error) => error);
    await settle();
    callTo(calls, "/ai/chat").reject(500);
    await read;
    expect(refused).toHaveLength(0);
    callTo(calls, "/editor/update_group/").reject(422);
    const error = await rename;
    expect(refused).toEqual([error]);
    expect(isRefusedMutation(error)).toBe(true);
  });

  it("leaves a request that got no answer, and a 401, to their callers", async () => {
    const { client, calls, refused } = makeClient();
    const dropped = client.post("/editor/delete_node/").catch((error) => error);
    await settle();
    calls[0].drop();
    expect(isRefusedMutation(await dropped)).toBe(false);
    const unauthorized = client.post("/editor/connect_node/").catch((error) => error);
    await settle();
    calls[1].reject(401);
    expect(isRefusedMutation(await unauthorized)).toBe(false);
    expect(refused).toHaveLength(0);
  });

  it("resyncs once when a 401 retry is refused", async () => {
    const { client, calls, refused } = makeClient((instance) => {
      instance.interceptors.response.use(undefined, (error: AxiosError) => {
        const config = error.config as InternalAxiosRequestConfig & { _retry?: boolean };
        if (error.response?.status !== 401 || config._retry) return Promise.reject(error);
        config._retry = true;
        return instance(config);
      });
    });
    const move = client.post("/editor/update_layout/").catch((error) => error);
    await settle();
    calls[0].reject(401);
    await settle();
    calls[1].reject(422);
    await move;
    expect(refused).toHaveLength(1);
  });

  it("counts enqueued mutations and reports idle once all completed", async () => {
    const { client, calls, queue } = makeClient();
    const before = queue.generation();
    const request = client.post("/editor/redo/");
    await settle();
    expect(queue.generation()).toBe(before + 1);
    const idle = vi.fn();
    void queue.whenIdle().then(idle);
    await settle();
    expect(idle).not.toHaveBeenCalled();
    calls[0].resolve({ success: true });
    await request;
    await settle();
    expect(idle).toHaveBeenCalled();
  });
});
