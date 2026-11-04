// @vitest-environment happy-dom
// The organisation's storage row: on a server that joined its hub, read-only and set by the hub; never on a desktop.
import { createApp, nextTick, type App } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { HubConnection } from "../../api/hubConnection.api";

const mocks = vi.hoisted(() => ({ get: vi.fn(), connections: vi.fn() }));

vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: vi.fn(),
  disconnectHub: vi.fn(),
}));
vi.mock("./api", () => ({
  fetchCloudStorageConnectionsInterfaces: mocks.connections,
  createCloudStorageConnectionApi: vi.fn(),
  updateCloudStorageConnectionApi: vi.fn(),
  deleteCloudStorageConnectionApi: vi.fn(),
}));
vi.mock("./CloudConnectionSettings.vue", () => ({ default: { render: () => null } }));
vi.mock("element-plus", async () => {
  const { h } = await import("vue");
  const passthrough = { inheritAttrs: false, render: () => null };
  return {
    ElDialog: passthrough,
    ElButton: { render: () => h("button") },
    ElMessage: { error: vi.fn() },
  };
});

import CloudConnectionView from "./CloudConnectionView.vue";

const desktop: HubConnection = {
  connected: true,
  hubUrl: "https://hub.acme.test:8443",
  deviceName: "ana-laptop",
  expiresAt: null,
  orgName: "Acme Data",
  hubVersion: null,
  reachable: true,
  error: null,
  account: "Ana Lima",
  joined: false,
  organisationStorage: "s3://acme/flowfile",
};
const server: HubConnection = { ...desktop, joined: true };

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
  app = createApp(CloudConnectionView);
  app.use(createPinia());
  app.mount(host);
  await settle();
};

const row = () => host.querySelector('[data-testid="hub-organisation-storage"]');

beforeEach(() => {
  mocks.get.mockReset();
  mocks.connections.mockReset().mockResolvedValue([]);
});

afterEach(() => {
  app?.unmount();
  app = null;
  host?.remove();
});

describe("the organisation's storage row", () => {
  it("shows the storage a joined server's hub names, read-only and set by the hub", async () => {
    await mount(server);

    expect(text(row())).toBe(
      "Set by your hub The organisation's storage at s3://acme/flowfile; each run gets access from the hub.",
    );
  });

  it("shows no row on a desktop, even one connected to a hub that names a storage", async () => {
    await mount(desktop);

    expect(row()).toBeNull();
  });

  it("shows no row when the hub does not say", async () => {
    await mount({ ...server, organisationStorage: null });

    expect(row()).toBeNull();
  });
});
