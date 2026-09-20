import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@/api/catalog.api", () => ({
  CatalogApi: { resolveTableStrict: vi.fn() },
}));

const treeRef: { value: unknown[] } = { value: [] };
vi.mock("@/stores/catalog-store", () => ({
  useCatalogStore: () => ({
    get tree() {
      return treeRef.value;
    },
  }),
}));

import { CatalogApi } from "@/api/catalog.api";
import { catalogRefKey } from "../nodes/node-types/elements/pythonScript/dataframeSchemaTypes";
import {
  getCatalogRefColumns,
  resetCatalogRefCache,
  resolveCatalogRefs,
} from "./catalogRefResolver";

const resolveStrict = CatalogApi.resolveTableStrict as unknown as ReturnType<typeof vi.fn>;

const tableWith = (columns: { name: string; dtype: string }[]) => ({ schema_columns: columns });

beforeEach(() => {
  vi.clearAllMocks();
  resetCatalogRefCache();
  treeRef.value = [];
  resolveStrict.mockResolvedValue(null);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("catalogRefKey", () => {
  it("keys by namespace id when present", () => {
    expect(catalogRefKey({ table: "orders", namespaceId: 7 })).toBe("ns:7:orders");
  });

  it("keys a 3-part reference by catalog.schema.table", () => {
    expect(catalogRefKey({ table: "orders", schema: "sales", catalog: "warehouse" })).toBe(
      "warehouse.sales.orders",
    );
  });

  it("keys a 2-part reference by schema.table and a bare one by table", () => {
    expect(catalogRefKey({ table: "orders", schema: "sales" })).toBe("sales.orders");
    expect(catalogRefKey({ table: "orders" })).toBe("orders");
  });

  it("ignores a lone catalog without a schema", () => {
    expect(catalogRefKey({ table: "orders", catalog: "warehouse" })).toBe("orders");
  });
});

describe("resolveCatalogRefs", () => {
  it("returns undefined for a reference never asked about", () => {
    expect(getCatalogRefColumns("orders")).toBeUndefined();
  });

  it("resolves a bare reference with strict=true and stores its columns", async () => {
    resolveStrict.mockResolvedValue(tableWith([{ name: "id", dtype: "Int64" }]));
    await resolveCatalogRefs([{ table: "orders" }]);
    expect(resolveStrict).toHaveBeenCalledWith("orders");
    expect(getCatalogRefColumns("orders")).toEqual([{ name: "id", dtype: "Int64" }]);
  });

  it("queries by namespace id when the reference carries one", async () => {
    resolveStrict.mockResolvedValue(tableWith([{ name: "amount", dtype: "Float64" }]));
    await resolveCatalogRefs([{ table: "orders", namespaceId: 12 }]);
    expect(resolveStrict).toHaveBeenCalledWith("orders", 12);
    expect(getCatalogRefColumns("ns:12:orders")).toEqual([{ name: "amount", dtype: "Float64" }]);
  });

  it("queries the 2-part form for a schema-only reference", async () => {
    await resolveCatalogRefs([{ table: "orders", schema: "sales" }]);
    expect(resolveStrict).toHaveBeenCalledWith("sales.orders");
  });

  it("walks the catalog tree to a namespace id for a 3-part reference", async () => {
    treeRef.value = [
      { id: 1, name: "other", children: [{ id: 2, name: "sales" }] },
      { id: 3, name: "warehouse", children: [{ id: 4, name: "sales" }] },
    ];
    await resolveCatalogRefs([{ table: "orders", schema: "sales", catalog: "warehouse" }]);
    expect(resolveStrict).toHaveBeenCalledWith("orders", 4);
  });

  it("falls back to the 2-part form when the tree has no such catalog/schema", async () => {
    treeRef.value = [{ id: 1, name: "warehouse", children: [{ id: 2, name: "finance" }] }];
    await resolveCatalogRefs([{ table: "orders", schema: "sales", catalog: "warehouse" }]);
    expect(resolveStrict).toHaveBeenCalledWith("sales.orders");
  });

  it("falls back to the 2-part form when the tree is empty", async () => {
    await resolveCatalogRefs([{ table: "orders", schema: "sales", catalog: "warehouse" }]);
    expect(resolveStrict).toHaveBeenCalledWith("sales.orders");
  });

  it("dedupes concurrent calls for the same reference", async () => {
    let release: (v: unknown) => void = () => undefined;
    resolveStrict.mockReturnValue(
      new Promise((resolve) => {
        release = resolve;
      }),
    );
    const a = resolveCatalogRefs([{ table: "orders" }]);
    const b = resolveCatalogRefs([{ table: "orders" }]);
    expect(resolveStrict).toHaveBeenCalledTimes(1);
    release(tableWith([{ name: "id", dtype: "Int64" }]));
    await Promise.all([a, b]);
    expect(resolveStrict).toHaveBeenCalledTimes(1);
    expect(getCatalogRefColumns("orders")).toEqual([{ name: "id", dtype: "Int64" }]);
  });

  it("dedupes repeated references inside one call", async () => {
    await resolveCatalogRefs([{ table: "orders" }, { table: "orders" }]);
    expect(resolveStrict).toHaveBeenCalledTimes(1);
  });

  it("does not re-query a resolved reference", async () => {
    resolveStrict.mockResolvedValue(tableWith([{ name: "id", dtype: "Int64" }]));
    await resolveCatalogRefs([{ table: "orders" }]);
    await resolveCatalogRefs([{ table: "orders" }]);
    expect(resolveStrict).toHaveBeenCalledTimes(1);
  });

  it("caches a negative and re-asks only after it expires", async () => {
    vi.useFakeTimers();
    resolveStrict.mockResolvedValue(null);
    await resolveCatalogRefs([{ table: "orders" }]);
    expect(getCatalogRefColumns("orders")).toBeNull();

    await resolveCatalogRefs([{ table: "orders" }]);
    expect(resolveStrict).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(30_000);
    expect(getCatalogRefColumns("orders")).toBeUndefined();
    resolveStrict.mockResolvedValue(tableWith([{ name: "id", dtype: "Int64" }]));
    await resolveCatalogRefs([{ table: "orders" }]);
    expect(resolveStrict).toHaveBeenCalledTimes(2);
    expect(getCatalogRefColumns("orders")).toEqual([{ name: "id", dtype: "Int64" }]);
  });

  it("treats a table with no schema_columns as an empty column list", async () => {
    resolveStrict.mockResolvedValue({ schema_columns: undefined });
    await resolveCatalogRefs([{ table: "orders" }]);
    expect(getCatalogRefColumns("orders")).toEqual([]);
  });
});
