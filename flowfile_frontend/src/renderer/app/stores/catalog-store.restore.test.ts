import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { CatalogTable, DeltaTableHistory } from "../types";

vi.mock("../api/catalog.api", () => ({
  CatalogApi: {
    getTablePreview: vi.fn(),
    getTableHistory: vi.fn(),
    getTables: vi.fn(),
    restoreTableVersion: vi.fn(),
  },
}));

import { CatalogApi } from "../api/catalog.api";
import { useCatalogStore } from "./catalog-store";

const history = (version: number): DeltaTableHistory => ({ current_version: version, history: [] });
const table = (id: number, rowCount = 3): CatalogTable =>
  ({ id, table_type: "physical", is_remote_storage: true, row_count: rowCount }) as CatalogTable;

describe("restoreTableVersion", () => {
  afterEach(() => vi.unstubAllGlobals());
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.resetAllMocks();
    const entries = new Map<string, string>();
    vi.stubGlobal("sessionStorage", {
      getItem: (key: string) => entries.get(key) ?? null,
      setItem: (key: string, value: string) => entries.set(key, value),
    });
    vi.mocked(CatalogApi.getTables).mockResolvedValue([]);
    vi.mocked(CatalogApi.getTableHistory).mockResolvedValue(history(3));
  });

  it("sends the version this window last saw and adopts the restored table", async () => {
    const store = useCatalogStore();
    store.selectedTableId = 7;
    store.selectedTable = table(7, 9);
    store.tableHistory = history(2);
    store.selectedVersion = 0;
    vi.mocked(CatalogApi.restoreTableVersion).mockResolvedValue({
      table: table(7, 3),
      restored_version: 0,
      new_version: 3,
    });

    const result = await store.restoreTableVersion(7, 0);

    expect(CatalogApi.restoreTableVersion).toHaveBeenCalledWith(7, {
      version: 0,
      expected_version: 2,
    });
    expect(result.new_version).toBe(3);
    expect(store.selectedTable?.row_count).toBe(3);
    expect(store.selectedVersion).toBeNull();
    expect(store.tableHistory?.current_version).toBe(3);
    expect(CatalogApi.getTables).toHaveBeenCalled();
  });

  it("sends no expectation for a table that is not selected", async () => {
    const store = useCatalogStore();
    store.selectedTableId = 1;
    store.tableHistory = history(5);
    vi.mocked(CatalogApi.restoreTableVersion).mockResolvedValue({
      table: table(7),
      restored_version: 0,
      new_version: 3,
    });

    await store.restoreTableVersion(7, 0);

    expect(CatalogApi.restoreTableVersion).toHaveBeenCalledWith(7, {
      version: 0,
      expected_version: null,
    });
  });

  it("leaves the store alone when the restore is refused", async () => {
    const store = useCatalogStore();
    store.selectedTableId = 7;
    store.selectedTable = table(7, 9);
    store.tableHistory = history(2);
    store.selectedVersion = 0;
    vi.mocked(CatalogApi.restoreTableVersion).mockRejectedValue({ response: { status: 409 } });

    await expect(store.restoreTableVersion(7, 0)).rejects.toEqual({ response: { status: 409 } });
    expect(store.selectedTable?.row_count).toBe(9);
    expect(store.selectedVersion).toBe(0);
  });
});
