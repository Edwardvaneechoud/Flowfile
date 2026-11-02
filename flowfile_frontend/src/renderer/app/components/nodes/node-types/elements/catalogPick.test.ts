import { describe, expect, it } from "vitest";

import type { CatalogTable, NamespaceTree } from "../../../../types/catalog.types";
import type { CatalogWriteSettings, NodeCatalogReader } from "../../../../types/node.types";
import {
  isHubTarget,
  namespaceChoice,
  namespaceOptions,
  pickerRowId,
  readerNamespaceChoice,
  selectedNamespaceRow,
  selectedTableRow,
  tableChoice,
} from "./catalogPick";

const table = (fields: Partial<CatalogTable>): CatalogTable =>
  ({ name: "t", full_table_name: null, ...fields }) as CatalogTable;

const ns = (fields: Partial<NamespaceTree>): NamespaceTree =>
  ({ children: [], tables: [], parent_id: null, ...fields }) as NamespaceTree;

const localOrders = table({
  id: 4,
  name: "orders",
  namespace_id: 2,
  full_table_name: "General.default.orders",
});
const hubSales = table({
  id: -30,
  name: "sales",
  namespace_id: -20,
  full_table_name: "Finance.raw.sales",
  source: "hub",
  hub_id: 30,
  hub_namespace_id: 20,
});

// The hub's catalog comes first on purpose: the options still list the local ones first.
const tree: NamespaceTree[] = [
  ns({
    id: -10,
    name: "Finance",
    source: "hub",
    hub_id: 10,
    children: [
      ns({ id: -20, name: "raw", parent_id: -10, source: "hub", hub_id: 20, tables: [hubSales] }),
      ns({ id: -21, name: "Local Flows", parent_id: -10, source: "hub", hub_id: 21 }),
    ],
  }),
  ns({
    id: 1,
    name: "General",
    children: [
      ns({ id: 2, name: "default", parent_id: 1, tables: [localOrders] }),
      ns({ id: 3, name: "Local Flows", parent_id: 1 }),
    ],
  }),
];

const reader = (fields: Partial<NodeCatalogReader> = {}): NodeCatalogReader =>
  ({
    catalog_table_id: null,
    catalog_namespace_id: null,
    catalog_table_name: null,
    catalog_full_table_name: null,
    hub_table_id: null,
    hub_namespace_id: null,
    ...fields,
  }) as NodeCatalogReader;

const writer = (fields: Partial<CatalogWriteSettings> = {}): CatalogWriteSettings =>
  ({
    table_name: "out",
    namespace_id: null,
    namespace_full_name: null,
    hub_namespace_id: null,
    ...fields,
  }) as CatalogWriteSettings;

describe("namespace options", () => {
  it("lists the local schemas first, then the hub's, marked by source", () => {
    expect(namespaceOptions(tree)).toEqual([
      { id: 2, label: "General / default", full: "General.default", source: "local", hubId: null },
      { id: -20, label: "Finance / raw", full: "Finance.raw", source: "hub", hubId: 20 },
      {
        id: -21,
        label: "Finance / Local Flows",
        full: "Finance.Local Flows",
        source: "hub",
        hubId: 21,
      },
    ]);
  });

  it("keeps the local system schemas when asked, as the reader does", () => {
    expect(namespaceOptions(tree, true).map((o) => o.label)).toEqual([
      "General / default",
      "General / Local Flows",
      "Finance / raw",
      "Finance / Local Flows",
    ]);
  });

  it("is the local list alone when no hub is connected", () => {
    const local = tree.filter((c) => c.source !== "hub");
    expect(namespaceOptions(local).map((o) => o.source)).toEqual(["local"]);
  });
});

describe("a reader's table choice", () => {
  it("writes a local table's fields as before and clears the hub's", () => {
    expect(tableChoice(localOrders)).toEqual({
      catalog_table_id: 4,
      catalog_namespace_id: 2,
      catalog_table_name: "orders",
      catalog_full_table_name: "General.default.orders",
      hub_table_id: null,
      hub_namespace_id: null,
    });
  });

  it("names a hub table by the hub's ids only, never a negative or a local id", () => {
    const writes = tableChoice(hubSales);
    expect(writes).toEqual({
      hub_table_id: 30,
      hub_namespace_id: 20,
      catalog_table_name: "sales",
      catalog_full_table_name: "Finance.raw.sales",
      catalog_table_id: null,
      catalog_namespace_id: null,
    });
    const saved = { ...reader({ catalog_table_id: 4, catalog_namespace_id: 2 }), ...writes };
    expect(Object.values(saved).filter((v) => typeof v === "number" && v < 0)).toEqual([]);
  });

  it("clears the table, local or hub, and keeps the namespace", () => {
    expect(tableChoice(null)).toEqual({
      catalog_table_id: null,
      catalog_table_name: null,
      catalog_full_table_name: null,
      hub_table_id: null,
    });
  });

  it("writes a picked schema as its local or its hub id", () => {
    const [local, hub] = namespaceOptions(tree);
    expect(readerNamespaceChoice(local)).toEqual({
      catalog_namespace_id: 2,
      hub_namespace_id: null,
    });
    expect(readerNamespaceChoice(hub)).toEqual({
      catalog_namespace_id: null,
      hub_namespace_id: 20,
    });
    expect(readerNamespaceChoice(null)).toEqual({
      catalog_namespace_id: null,
      hub_namespace_id: null,
    });
  });
});

describe("a writer's namespace choice", () => {
  const [local, hub] = namespaceOptions(tree);

  it("writes a local schema's id and full name as before and clears the hub's", () => {
    expect(namespaceChoice(local)).toEqual({
      namespace_id: 2,
      namespace_full_name: "General.default",
      hub_namespace_id: null,
    });
  });

  it("names a hub schema by the hub's id only", () => {
    expect(namespaceChoice(hub)).toEqual({
      namespace_id: null,
      namespace_full_name: null,
      hub_namespace_id: 20,
    });
  });

  it("clears all three when the select is cleared", () => {
    expect(namespaceChoice(null)).toEqual({
      namespace_id: null,
      namespace_full_name: null,
      hub_namespace_id: null,
    });
  });
});

describe("restoring a saved choice", () => {
  const options = namespaceOptions(tree, true);
  const tables = [localOrders, hubSales];

  it("finds a saved hub reader's schema and table by the hub's ids", () => {
    const saved = reader(tableChoice(hubSales));
    expect(selectedNamespaceRow(saved, options)?.id).toBe(-20);
    expect(selectedTableRow(saved, tables)).toBe(hubSales);
    expect(pickerRowId(selectedTableRow(saved, tables)!)).toBe(-30);
  });

  it("finds a saved local reader by its local ids", () => {
    const saved = reader(tableChoice(localOrders));
    expect(selectedNamespaceRow(saved, options)?.id).toBe(2);
    expect(selectedTableRow(saved, tables)).toBe(localOrders);
  });

  it("never matches a local id against a hub row's negative id, or the reverse", () => {
    expect(selectedTableRow(reader({ catalog_table_id: -30 }), tables)).toBeNull();
    expect(selectedTableRow(reader({ hub_table_id: 4, hub_namespace_id: 2 }), tables)).toBeNull();
    expect(selectedNamespaceRow(writer({ namespace_id: -20 }), options)).toBeNull();
  });

  it("finds a saved hub writer's schema by the hub's id", () => {
    expect(selectedNamespaceRow(writer({ hub_namespace_id: 20 }), options)?.id).toBe(-20);
  });

  it("finds a local writer by its id, else by its full name", () => {
    expect(selectedNamespaceRow(writer({ namespace_id: 2 }), options)?.id).toBe(2);
    expect(
      selectedNamespaceRow(writer({ namespace_full_name: "General.default" }), options)?.id,
    ).toBe(2);
    expect(
      selectedNamespaceRow(writer({ namespace_full_name: "Finance.raw" }), options),
    ).toBeNull();
  });

  it("finds nothing for a hub target the tree no longer lists", () => {
    const local = namespaceOptions(tree.filter((c) => c.source !== "hub"));
    expect(selectedNamespaceRow(writer({ hub_namespace_id: 20 }), local)).toBeNull();
    expect(
      selectedTableRow(reader({ hub_table_id: 30, hub_namespace_id: 20 }), [localOrders]),
    ).toBeNull();
  });
});

describe("hub targets", () => {
  it("is a reader of a hub table", () => {
    expect(isHubTarget(reader(tableChoice(hubSales)))).toBe(true);
    expect(isHubTarget(reader(tableChoice(localOrders)))).toBe(false);
    expect(isHubTarget(reader())).toBe(false);
  });

  it("is a writer into a hub schema", () => {
    expect(isHubTarget(writer({ hub_namespace_id: 20 }))).toBe(true);
    expect(isHubTarget(writer({ namespace_id: 2 }))).toBe(false);
    expect(isHubTarget(writer())).toBe(false);
  });
});
