import { describe, it, expect } from "vitest";
import {
  catalogStorageError,
  deleteBlockedReason,
  isEmptySchema,
  isSeededSchema,
  storageLockedReason,
  storedTableCount,
} from "./catalogStorageEdit";
import type { CatalogTable, NamespaceTree } from "../../types";

const table = (id: number, table_type: "physical" | "virtual") =>
  ({ id, table_type }) as CatalogTable;

const node = (over: Partial<NamespaceTree> & { id: number; level: number }) =>
  ({
    name: `ns${over.id}`,
    parent_id: null,
    children: [],
    flows: [],
    artifacts: [],
    tables: [],
    visualizations: [],
    notebooks: [],
    ...over,
  }) as NamespaceTree;

describe("catalogStorageError", () => {
  it("accepts a URI with a connection, and neither (local storage)", () => {
    expect(catalogStorageError("s3://flowfile-catalog", "hetzner")).toBeNull();
    expect(catalogStorageError("  ", null)).toBeNull();
  });

  it("refuses a URI that is not object storage", () => {
    expect(catalogStorageError("/data/catalog", "hetzner")).toMatch(/must start with s3:\/\//);
  });

  it("refuses one half of the pair without the other", () => {
    expect(catalogStorageError("s3://flowfile-catalog", null)).toMatch(
      /Select a storage connection/,
    );
    expect(catalogStorageError("", "hetzner")).toMatch(/Enter a storage URI/);
  });
});

describe("storage lock", () => {
  it("counts the stored tables of the catalog and its schemas, not the virtual ones", () => {
    const catalog = node({
      id: 1,
      level: 0,
      tables: [table(1, "physical")],
      children: [node({ id: 2, level: 1, tables: [table(2, "physical"), table(3, "virtual")] })],
    });

    expect(storedTableCount(catalog)).toBe(2);
    expect(storageLockedReason(catalog)).toMatch(/holds 2 tables/);
  });

  it("leaves the storage open while only virtual tables exist", () => {
    const catalog = node({
      id: 1,
      level: 0,
      children: [node({ id: 2, level: 1, tables: [table(3, "virtual")] })],
    });

    expect(storageLockedReason(catalog)).toBeNull();
  });
});

describe("deleteBlockedReason", () => {
  it("allows an empty catalog, and one whose schemas are all empty", () => {
    expect(deleteBlockedReason(node({ id: 1, level: 0 }))).toBeNull();
    expect(
      deleteBlockedReason(node({ id: 1, level: 0, children: [node({ id: 2, level: 1 })] })),
    ).toBeNull();
  });

  it("names what the catalog and its schemas still hold", () => {
    const catalog = node({
      id: 1,
      level: 0,
      children: [node({ id: 2, level: 1, tables: [table(1, "physical"), table(2, "virtual")] })],
      flows: [
        { id: 1 } as NamespaceTree["flows"][number],
        { id: 2 } as NamespaceTree["flows"][number],
      ],
    });

    expect(deleteBlockedReason(catalog)).toBe(
      "Only an empty catalog can be deleted; this one holds 2 flows, 2 tables.",
    );
  });
});

describe("isEmptySchema", () => {
  it("is true only for a schema that holds nothing", () => {
    expect(isEmptySchema(node({ id: 2, level: 1 }))).toBe(true);
    expect(isEmptySchema(node({ id: 2, level: 1, tables: [table(1, "virtual")] }))).toBe(false);
    const withChart = node({
      id: 2,
      level: 1,
      visualizations: [{ id: 1 } as NamespaceTree["visualizations"][number]],
    });
    expect(isEmptySchema(withChart)).toBe(false);
  });
});

describe("seeded namespaces", () => {
  const general = node({ id: 1, level: 0, name: "General" });
  const mine = node({ id: 5, level: 0, name: "Mine" });

  it("blocks deleting the General root catalog, empty or not", () => {
    expect(deleteBlockedReason(general)).toMatch(/created by Flowfile/);
    expect(
      deleteBlockedReason(node({ id: 3, level: 1, name: "General", parent_id: 5 })),
    ).toBeNull();
  });

  it("marks General's default and app-managed schemas, not a user catalog's", () => {
    const roots = [general, mine];
    expect(isSeededSchema(node({ id: 2, level: 1, name: "default", parent_id: 1 }), roots)).toBe(
      true,
    );
    expect(
      isSeededSchema(node({ id: 4, level: 1, name: "Local Flows", parent_id: 1 }), roots),
    ).toBe(true);
    expect(isSeededSchema(node({ id: 6, level: 1, name: "default", parent_id: 5 }), roots)).toBe(
      false,
    );
    expect(isSeededSchema(node({ id: 7, level: 1, name: "reports", parent_id: 1 }), roots)).toBe(
      false,
    );
  });
});
