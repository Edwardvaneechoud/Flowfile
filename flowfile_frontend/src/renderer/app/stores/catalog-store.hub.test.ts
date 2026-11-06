import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogTable, NamespaceTree } from "../types";

const { getTablePreviewMock, getTableHistoryMock, addTableFavoriteMock } = vi.hoisted(() => ({
  getTablePreviewMock: vi.fn(),
  getTableHistoryMock: vi.fn(),
  addTableFavoriteMock: vi.fn(),
}));

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getTablePreview: getTablePreviewMock,
    getTableHistory: getTableHistoryMock,
    addTableFavorite: addTableFavoriteMock,
  },
}));

import { useCatalogStore } from "./catalog-store";

const table = (overrides: Partial<CatalogTable> = {}): CatalogTable =>
  ({
    id: 5,
    name: "orders",
    table_type: "physical",
    is_remote_storage: false,
    is_favorite: false,
    ...overrides,
  }) as unknown as CatalogTable;

const schema = (id: number, tables: CatalogTable[], source?: "hub"): NamespaceTree =>
  ({ id, name: "raw", level: 1, children: [], tables, source }) as unknown as NamespaceTree;

const catalog = (id: number, children: NamespaceTree[], source?: "hub"): NamespaceTree =>
  ({ id, name: "sales", level: 0, children, tables: [], source }) as unknown as NamespaceTree;

const localTable = table();
const hubTable = table({ id: -5, source: "hub", hub_id: 5, hub_namespace_id: 3 });

describe("catalog-store with hub rows in the tree", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    sessionStorage.clear();
    getTablePreviewMock.mockReset().mockResolvedValue({ rows: [] });
    getTableHistoryMock.mockReset().mockResolvedValue(null);
    addTableFavoriteMock.mockReset().mockResolvedValue(undefined);
  });

  const seed = () => {
    const store = useCatalogStore();
    store.tree = [
      catalog(1, [schema(2, [localTable])]),
      catalog(-1, [schema(-3, [hubTable], "hub")], "hub"),
    ];
    return store;
  };

  it("selects a hub table without asking for its preview or history", () => {
    const store = seed();
    store.selectTable(-5);
    expect(store.selectedTableId).toBe(-5);
    expect(store.selectedTable?.hub_id).toBe(5);
    expect(getTablePreviewMock).not.toHaveBeenCalled();
    expect(getTableHistoryMock).not.toHaveBeenCalled();
  });

  it("loads a local table's preview and history as before", () => {
    const store = seed();
    store.selectTable(5);
    expect(store.selectedTable?.name).toBe("orders");
    expect(getTablePreviewMock).toHaveBeenCalledWith(5, 100, null);
    expect(getTableHistoryMock).toHaveBeenCalledWith(5);
  });

  it("does not favourite a hub table, and favourites a local one", async () => {
    const store = seed();
    await store.toggleTableFavorite(-5);
    expect(addTableFavoriteMock).not.toHaveBeenCalled();
    expect(hubTable.is_favorite).toBe(false);

    await store.toggleTableFavorite(5);
    expect(addTableFavoriteMock).toHaveBeenCalledWith(5);
  });

  it("finds a hub catalog by its negative id", () => {
    const store = seed();
    store.selectNamespace(-1);
    expect(store.selectedNamespace?.source).toBe("hub");
  });
});
