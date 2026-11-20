import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { none, server } from "../__fixtures__/hubConnection";

const mocks = vi.hoisted(() => ({ connection: vi.fn(), tree: vi.fn() }));

vi.mock("../api/hubConnection.api", () => ({
  getHubConnection: mocks.connection,
  connectHub: vi.fn(),
  disconnectHub: vi.fn(),
}));
vi.mock("../api", () => ({ CatalogApi: { getNamespaceTree: mocks.tree } }));

import { useOwnCatalog } from "./useOwnCatalog";

describe("useOwnCatalog", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    mocks.connection.mockReset();
    mocks.tree.mockReset();
  });

  it("offers the own catalog without reading the tree where no hub was joined", async () => {
    mocks.connection.mockResolvedValue(none);

    expect(await useOwnCatalog().refreshOwnCatalog()).toBe(true);
    expect(mocks.tree).not.toHaveBeenCalled();
  });

  it("asks the tree on a server that joined a hub", async () => {
    mocks.connection.mockResolvedValue(server);
    mocks.tree.mockResolvedValue([{ source: "local", offered: false }]);

    expect(await useOwnCatalog().refreshOwnCatalog()).toBe(false);
  });

  it("asks the tree when the connection could not be read", async () => {
    mocks.connection.mockRejectedValue(new Error("down"));
    mocks.tree.mockResolvedValue([{ source: "local" }]);

    expect(await useOwnCatalog().refreshOwnCatalog()).toBe(true);
    expect(mocks.tree).toHaveBeenCalledOnce();
  });
});
