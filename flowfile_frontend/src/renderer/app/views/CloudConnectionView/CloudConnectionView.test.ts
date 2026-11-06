// @vitest-environment happy-dom
// The organisation's storage row: on a server that joined its hub, read-only and set by the hub; never on a desktop.
import { createPinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { desktop as connected } from "../../__fixtures__/hubConnection";
import { mountIn, text } from "../../__fixtures__/mount";
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

const desktop: HubConnection = { ...connected, organisationStorage: "s3://acme/flowfile" };
const server: HubConnection = { ...desktop, joined: true };

let host: HTMLElement;

const mount = async (connection: HubConnection) => {
  mocks.get.mockResolvedValue(connection);
  host = await mountIn(CloudConnectionView, { pinia: createPinia() });
};

const row = () => host.querySelector('[data-testid="hub-organisation-storage"]');

beforeEach(() => {
  mocks.get.mockReset();
  mocks.connections.mockReset().mockResolvedValue([]);
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
