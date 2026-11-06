import { describe, expect, it } from "vitest";

import type { CatalogTable, NamespaceTree } from "../../../../types/catalog.types";
import type { CatalogWriteSettings, NodeCatalogReader } from "../../../../types/node.types";
import {
  READ_ONLY,
  hubDefaultSchema,
  isHubTarget,
  namespaceChoice,
  namespaceOptions,
  optionGroups,
  pickable,
  pickerPlace,
  readerNamespaceChoice,
  selectedNamespaceRow,
  selectedTableRow,
  tableChoice,
  tablePlace,
  unusableNamespaceIds,
  writeBlock,
  writerDefault,
} from "./catalogPick";
import { hubPresence } from "../../../../views/CatalogView/hubRows";

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
    const unplaced = { place: null, unavailableReason: null, readOnly: false };
    expect(namespaceOptions(tree)).toEqual([
      {
        id: 2,
        label: "General / default",
        full: "General.default",
        source: "local",
        hubId: null,
        ...unplaced,
        writable: true,
      },
      {
        id: -20,
        label: "Finance / raw",
        full: "Finance.raw",
        source: "hub",
        hubId: 20,
        ...unplaced,
        writable: false,
      },
      {
        id: -21,
        label: "Finance / Local Flows",
        full: "Finance.Local Flows",
        source: "hub",
        hubId: 21,
        ...unplaced,
        writable: false,
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

describe("pickers by place and reach", () => {
  const ON_ITS_OWN_DISK =
    "The hub keeps the tables of scratch.notes on its own disk; this Flowfile cannot read them. " +
    "Pick a catalog on the organisation's storage, or ask the hub's admin to put this one there.";
  const USE = { is_owner: false, access_level: "use" as const, shared_by: "bob" };
  const MANAGE = { is_owner: false, access_level: "manage" as const, shared_by: "bob" };
  const OWNER = { is_owner: true, access_level: "owner" as const, shared_by: null };
  const hubCatalog = (id: number, name: string, schemas: Partial<NamespaceTree>[], fields = {}) =>
    ns({
      id: -id,
      name,
      source: "hub",
      hub_id: id,
      place: "Acme Data",
      ...fields,
      children: schemas.map((schema) =>
        ns({ parent_id: -id, source: "hub", place: "Acme Data", ...schema }),
      ),
    });
  const desktop: NamespaceTree[] = [
    ns({
      id: 1,
      name: "General",
      place: "On this computer",
      children: [ns({ id: 2, name: "default", parent_id: 1, place: "On this computer" })],
    }),
    hubCatalog(10, "main", [
      {
        id: -11,
        name: "default",
        hub_id: 11,
        access: MANAGE,
        tables: [table({ id: -40, source: "hub" })],
      },
    ]),
    hubCatalog(
      20,
      "scratch",
      [{ id: -21, name: "notes", hub_id: 21, unavailable_reason: ON_ITS_OWN_DISK }],
      { unavailable_reason: "The hub keeps the tables of scratch on its own disk" },
    ),
  ];
  const joined: NamespaceTree[] = desktop.map((catalog) =>
    catalog.source === "hub" ? catalog : { ...catalog, offered: false, place: "On this server" },
  );

  it("heads the hub's schemas with one Hub group beside this machine's, which keep their place", () => {
    const options = namespaceOptions(desktop);
    const groups = optionGroups(pickable(options, null), pickerPlace);
    expect(groups.map((g) => [g.label, g.rows.map((o) => o.label)])).toEqual([
      ["On this computer", ["General / default"]],
      ["Hub", ["main / default"]],
    ]);
  });

  it("offers a joined server's people the hub's catalogs only, as one group with no Hub heading", () => {
    const options = namespaceOptions(joined);
    expect(options.map((o) => o.label)).toEqual(["main / default", "scratch / notes"]);
    const groups = optionGroups(pickable(options, null), pickerPlace);
    expect(groups.map((g) => [g.label, g.rows.map((o) => o.label)])).toEqual([
      ["", ["main / default"]],
    ]);
  });

  it("lists a desktop without a hub as one group with no heading, as before", () => {
    const options = namespaceOptions(tree.filter((c) => c.source !== "hub"));
    const groups = optionGroups(pickable(options, null), pickerPlace);
    expect(groups.map((g) => [g.label, g.rows.map((o) => o.label)])).toEqual([
      ["", ["General / default"]],
    ]);
  });

  it("lists rows core names no place for as one group with no heading", () => {
    const groups = optionGroups([localOrders, hubSales], () => null);
    expect(groups).toEqual([{ label: "", rows: [localOrders, hubSales] }]);
  });

  it("keeps a schema this machine cannot use out of the list unless the node points at it", () => {
    const options = namespaceOptions(desktop);
    const notes = options.find((o) => o.full === "scratch.notes")!;
    expect(notes.unavailableReason).toBe(ON_ITS_OWN_DISK);
    expect(pickable(options, null).map((o) => o.full)).toEqual(["General.default", "main.default"]);
    expect(pickable(options, notes.id).map((o) => o.full)).toEqual([
      "General.default",
      "main.default",
      "scratch.notes",
    ]);
  });

  it("takes a schema's reason from its catalog when the schema has none of its own", () => {
    const tree = [
      hubCatalog(30, "lake", [{ id: -31, name: "landing" }], { unavailable_reason: "az" }),
    ];
    expect(namespaceOptions(tree)[0].unavailableReason).toBe("az");
    expect([...unusableNamespaceIds(tree)].sort((a, b) => a - b)).toEqual([-31, -30]);
  });

  it("says from the options whether the hub's schemas show alone or beside this machine's", () => {
    expect(hubPresence(namespaceOptions(desktop))).toBe("beside");
    expect(hubPresence(namespaceOptions(joined))).toBe("only");
    expect(hubPresence(namespaceOptions(tree.filter((c) => c.source !== "hub")))).toBe("none");
  });

  it("names each table's place by its source", () => {
    const options = namespaceOptions(desktop);
    expect(tablePlace(table({ id: 4 }), options)).toBe("On this computer");
    expect(tablePlace(table({ id: -40, source: "hub" }), options)).toBe("Hub");
  });

  describe("the writer's default", () => {
    it("is nothing on a desktop, whose own catalogs are offered", () => {
      expect(writerDefault(namespaceOptions(desktop))).toBeNull();
    });

    it("is the hub's main.default on a joined server", () => {
      expect(writerDefault(namespaceOptions(joined))?.full).toBe("main.default");
    });

    it("never preselects a schema this machine cannot use", () => {
      const unreachable = joined.map((catalog) =>
        catalog.name === "main" ? { ...catalog, unavailable_reason: "on its own disk" } : catalog,
      );
      expect(writerDefault(namespaceOptions(unreachable))).toBeNull();
    });

    it("is the first usable hub schema the person may write when main.default is not one", () => {
      const tree = [
        hubCatalog(10, "main", [{ id: -11, name: "default", hub_id: 11, access: USE }]),
        hubCatalog(50, "team", [
          { id: -51, name: "raw", hub_id: 51, access: USE },
          { id: -52, name: "curated", hub_id: 52, access: MANAGE },
        ]),
      ];
      expect(hubDefaultSchema(namespaceOptions(tree))?.full).toBe("team.curated");
    });

    it("greys a schema the person may only read, says why, and lists it", () => {
      const tree = [
        hubCatalog(10, "main", [
          { id: -11, name: "default", hub_id: 11, access: USE },
          { id: -12, name: "team", hub_id: 12, access: MANAGE },
        ]),
      ];
      const options = namespaceOptions(tree);
      expect(options.map((o) => [o.full, o.readOnly, writeBlock(o)])).toEqual([
        ["main.default", true, READ_ONLY],
        ["main.team", false, null],
      ]);
      expect(pickable(options, null).map((o) => o.full)).toEqual(["main.default", "main.team"]);
      expect(writerDefault(options)?.full).toBe("main.team");
    });

    it("never greys a schema of this machine's own, whatever grant it carries", () => {
      const use = { is_owner: false, access_level: "use" as const, shared_by: "bob" };
      const tree = [
        ns({
          id: 1,
          name: "Sales",
          access: use,
          children: [ns({ id: 2, name: "shared", parent_id: 1, access: use })],
        }),
      ];
      const [option] = namespaceOptions(tree);
      expect([option.source, option.readOnly, writeBlock(option)]).toEqual(["local", false, null]);
    });

    it("names this machine's reach before read-only when both hold", () => {
      const tree = [
        hubCatalog(20, "scratch", [{ id: -21, name: "notes", hub_id: 21, access: USE }], {
          unavailable_reason: "on its own disk",
        }),
      ];
      expect(writeBlock(namespaceOptions(tree)[0])).toBe("on its own disk");
    });

    it("takes a catalog's grant for its schemas, and is nothing when no hub schema may be written", () => {
      const tree = [
        hubCatalog(10, "main", [{ id: -11, name: "default", hub_id: 11 }], { access: USE }),
      ];
      expect(namespaceOptions(tree)[0].writable).toBe(false);
      expect(hubDefaultSchema(namespaceOptions(tree))).toBeNull();
    });

    it("never counts a hub schema the hub stamped nothing on, nor preselects it", () => {
      const tree = [hubCatalog(10, "main", [{ id: -11, name: "default", hub_id: 11 }])];
      const [option] = namespaceOptions(tree);
      expect([option.writable, option.readOnly]).toEqual([false, false]);
      expect(writerDefault(namespaceOptions(tree))).toBeNull();
    });

    it("counts a hub schema the person owns as theirs to write", () => {
      const tree = [
        hubCatalog(10, "main", [{ id: -11, name: "default", hub_id: 11 }], { access: OWNER }),
      ];
      expect(writerDefault(namespaceOptions(tree))?.full).toBe("main.default");
    });

    it("still counts a local schema with no access as writable, as an unrestricted desktop has it", () => {
      expect(namespaceOptions(tree)[0]).toMatchObject({ source: "local", writable: true });
    });

    it("writes the hub's id only when chosen", () => {
      const preset = writerDefault(namespaceOptions(joined));
      expect(namespaceChoice(preset)).toEqual({
        namespace_id: null,
        namespace_full_name: null,
        hub_namespace_id: 11,
      });
    });
  });
});
