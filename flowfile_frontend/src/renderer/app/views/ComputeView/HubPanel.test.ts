// @vitest-environment happy-dom
// The Hub card as an administrator sees it: the connect form, the connected desktop's facts, and on a server
// that joined its hub the line saying whose hub it belongs to, with nothing to type.
import { createApp, h, nextTick, type App, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { HubConnection } from "../../api/hubConnection.api";

const mocks = vi.hoisted(() => ({ get: vi.fn() }));

vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: vi.fn(),
  disconnectHub: vi.fn(),
}));
vi.mock("element-plus", () => ({ ElMessage: { success: vi.fn(), error: vi.fn() } }));

import HubPanel from "./HubPanel.vue";

const element = (tag: string): FunctionalComponent => {
  const stub: FunctionalComponent = (_, { attrs, slots }) => h(tag, attrs, slots.default?.());
  stub.inheritAttrs = false;
  return stub;
};

const stubs: Record<string, FunctionalComponent> = {
  "el-collapse-transition": (_, { slots }) => slots.default?.(),
  "el-button": element("button"),
  "el-input": element("input"),
  "router-link": element("a"),
};

const none: HubConnection = {
  connected: false,
  hubUrl: null,
  deviceName: "ana-laptop",
  expiresAt: null,
  orgName: null,
  hubVersion: null,
  reachable: false,
  error: null,
  account: null,
  joined: false,
  organisationStorage: null,
};
const desktop: HubConnection = {
  ...none,
  connected: true,
  hubUrl: "https://hub.acme.test:8443",
  expiresAt: "2026-11-08T12:00:00Z",
  orgName: "Acme Data",
  hubVersion: "0.3.0",
  reachable: true,
  account: "Ana Lima",
};
const server: HubConnection = { ...desktop, joined: true };

const fakeLocalStorage = () => {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  } as unknown as Storage;
};

const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
  await nextTick();
};

const text = (el: Element | null) => el?.textContent?.replace(/\s+/g, " ").trim() ?? "";

let app: App | null = null;
let host: HTMLElement;

const mount = async (connection: HubConnection) => {
  mocks.get.mockResolvedValue(connection);
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp(HubPanel);
  app.use(createPinia());
  for (const [name, stub] of Object.entries(stubs)) app.component(name, stub);
  app.mount(host);
  await settle();
};

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
  mocks.get.mockReset();
});

afterEach(() => {
  app?.unmount();
  app = null;
  host?.remove();
  vi.unstubAllGlobals();
});

describe("HubPanel", () => {
  it("offers the hub's address and an account to a desktop with no hub, and nothing else", async () => {
    await mount(none);

    expect(find("hub-url")).not.toBeNull();
    expect(find("hub-username")).not.toBeNull();
    expect(find("hub-password")).not.toBeNull();
    expect(find("hub-connect")).not.toBeNull();
    expect(find("hub-status")).toBeNull();
    expect(find("hub-joined")).toBeNull();
  });

  it("says which hub a desktop is connected to, as whom, with its version and when the session ends", async () => {
    await mount(desktop);

    const status = find("hub-status")!;
    expect(status.className).toContain("compute-card__status--success");
    expect(text(status)).toContain(
      "Connected to https://hub.acme.test:8443 as the device ana-laptop.",
    );
    const facts = [...host.querySelectorAll(".hub-card__facts dt, .hub-card__facts dd")].map(text);
    expect(facts.slice(0, 6)).toEqual([
      "Account",
      "Ana Lima",
      "Organisation",
      "Acme Data",
      "Hub version",
      "0.3.0",
    ]);
    expect(facts[6]).toBe("Session expires");
    expect(find("hub-disconnect")).not.toBeNull();
    expect(find("hub-url")).toBeNull();
  });

  it("says whose hub a joined server belongs to, and offers nothing to type or press", async () => {
    await mount(server);

    expect(text(find("hub-joined"))).toContain(
      "This server belongs to Acme Data's hub at https://hub.acme.test:8443.",
    );
    expect(text(find("hub-account"))).toBe("Signed in as Ana Lima.");
    expect(host.querySelector(".hub-card__facts")).toBeNull();
    expect(find("hub-url")).toBeNull();
    expect(find("hub-connect")).toBeNull();
    expect(find("hub-disconnect")).toBeNull();
    expect(find("hub-servers-link")).not.toBeNull();
  });

  it("names the hub by its address when it does not answer its name", async () => {
    await mount({ ...server, orgName: null });

    expect(text(find("hub-joined"))).toContain(
      "This server belongs to the hub at https://hub.acme.test:8443.",
    );
  });

  it("keeps saying so on a joined server whose hub session ended, with why, and still asks for no password", async () => {
    await mount({
      ...server,
      connected: false,
      reachable: false,
      account: null,
      error: "the hub ended this session",
    });

    const status = find("hub-joined")!;
    expect(status.className).toContain("compute-card__status--warning");
    expect(text(status)).toContain("This server belongs to Acme Data's hub");
    expect(text(status)).toContain("The hub is not answering: the hub ended this session");
    expect(find("hub-account")).toBeNull();
    expect(find("hub-url")).toBeNull();
    expect(find("hub-password")).toBeNull();
    expect(find("hub-connect")).toBeNull();
  });
});
