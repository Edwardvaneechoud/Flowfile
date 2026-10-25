import { describe, it, expect } from "vitest";
import {
  catalogStorageError,
  deleteBlockedReason,
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
  it("allows an empty catalog", () => {
    expect(deleteBlockedReason(node({ id: 1, level: 0 }))).toBeNull();
  });

  it("names what a catalog still holds", () => {
    const catalog = node({
      id: 1,
      level: 0,
      children: [node({ id: 2, level: 1 })],
      flows: [
        { id: 1 } as NamespaceTree["flows"][number],
        { id: 2 } as NamespaceTree["flows"][number],
      ],
    });

    expect(deleteBlockedReason(catalog)).toBe(
      "Only an empty catalog can be deleted; this one holds 1 schema, 2 flows.",
    );
  });
});
