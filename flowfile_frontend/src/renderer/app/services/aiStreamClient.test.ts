// @vitest-environment happy-dom
// The AI clients send their requests with fetch, beside axios; a 401 there, or no token at all, takes the same
// path as any request: the session is renewed, or it has ended and the tab goes to the sign-in page.
import axios from "axios";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { fakeStorage } from "../__fixtures__/mount";
import {
  fakeServer,
  loadSession,
  refused,
  renewed,
  storeSession,
  WARM_UP_MS,
  warmSession,
  watchReloads,
  within,
  type Answer,
} from "../__fixtures__/session";

const original = axios.defaults.adapter;
let reloads: ReturnType<typeof watchReloads>;
const fetched: { url: string; authorization: string | null }[] = [];

type Call = (clients: {
  stream: typeof import("./aiStreamClient");
  diff: typeof import("./aiDiffClient");
  local: typeof import("../views/AiSettingsView/localModelApi");
}) => Promise<unknown>;

const handlers = { onError: vi.fn() };

const CALLS: [string, Call][] = [
  ["the chat stream", ({ stream }) => stream.streamChat({ provider: "p", messages: [] }, handlers)],
  [
    "the agent's start",
    ({ stream }) => stream.streamAgentSession({ flow_id: 1, prompt: "x", provider: "p" }, handlers),
  ],
  [
    "the agent's follow-up",
    ({ stream }) => stream.streamAgentFollowup("s1", { action: "user_message" }, handlers),
  ],
  ["the agent's resume", ({ stream }) => stream.resumeAgentSessionStream("s1", handlers)],
  ["the router", ({ stream }) => stream.routeMessage({ message: "x", provider: "p" })],
  ["the diff stage", ({ diff }) => diff.stageDiff({} as never)],
  ["the on-device install", ({ local }) => local.streamLocalModelInstall(handlers)],
];

const load = async (renewal: Answer) => {
  const server = fakeServer((config) =>
    config.url === "/auth/refresh" ? renewal : { status: 200, data: {} },
  );
  const authService = await loadSession(server.adapter);
  const clients = {
    stream: await import("./aiStreamClient"),
    diff: await import("./aiDiffClient"),
    local: await import("../views/AiSettingsView/localModelApi"),
  };
  return { authService, server, clients };
};

beforeAll(
  () =>
    warmSession(
      () => import("./aiStreamClient"),
      () => import("./aiDiffClient"),
      () => import("../views/AiSettingsView/localModelApi"),
    ),
  WARM_UP_MS,
);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  vi.stubGlobal("sessionStorage", fakeStorage());
  window.history.replaceState(null, "", "/ui/#/main/designer");
  reloads = watchReloads();
  fetched.length = 0;
  handlers.onError.mockReset();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string | URL, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      fetched.push({ url: String(url), authorization: headers.get("Authorization") });
      return new Response(JSON.stringify({ detail: "Could not validate credentials" }), {
        status: 401,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
});

afterEach(() => {
  reloads.restore();
  axios.defaults.adapter = original;
  vi.unstubAllGlobals();
  window.history.replaceState(null, "", "/");
});

describe("the AI clients on a refused session", () => {
  it.each(CALLS)(
    "a 401 on %s whose renewal is refused ends at the sign-in page",
    async (_c, call) => {
      storeSession(3_600_000);
      const { authService, server, clients } = await load(refused);

      await within(call(clients)).catch(() => undefined);

      expect(fetched).toHaveLength(1);
      expect(server.renewals()).toHaveLength(1);
      expect(reloads.reloads).toHaveLength(1);
      expect(reloads.reloads[0].pathname).toBe("/ui/");
      expect(reloads.reloads[0].hash.startsWith("#/login")).toBe(true);
      expect(authService.isAuthenticated()).toBe(false);
    },
  );

  it.each(CALLS)("a 401 on %s whose renewal is granted keeps the session", async (_c, call) => {
    storeSession(3_600_000);
    const { authService, server, clients } = await load(renewed("fresh-token"));

    await within(call(clients)).catch(() => undefined);

    expect(server.renewals()).toHaveLength(1);
    expect(reloads.reloads).toHaveLength(0);
    expect(authService.isAuthenticated()).toBe(true);
    expect(await within(authService.getToken())).toBe("fresh-token");
  });

  it.each(CALLS)(
    "no token for %s, its renewal refused, ends at the sign-in page",
    async (_c, call) => {
      storeSession(40);
      const { server, clients } = await load(refused);
      await new Promise((resolve) => setTimeout(resolve, 60));

      await within(call(clients)).catch(() => undefined);

      expect(fetched).toHaveLength(0);
      expect(server.renewals()).toHaveLength(1);
      expect(reloads.reloads).toHaveLength(1);
      expect(reloads.reloads[0].hash.startsWith("#/login")).toBe(true);
    },
  );
});
