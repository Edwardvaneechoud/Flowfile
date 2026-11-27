import { describe, expect, it } from "vitest";

import { ns, table } from "../../../../__fixtures__/catalogTree";
import type { NamespaceTree } from "../../../../types/catalog.types";
import type { CatalogWriteSettings, NodeCatalogReader } from "../../../../types/node.types";
import {
  READ_ONLY,
  outsideNoChanges,
  outsideNoVersion,
  hubDefaultSchema,
  isHubTarget,
  namespaceChoice,
  namespaceOptions,
  optionGroups,
  outsideTableChoice,
  pickable,
  readerNamespaceChoice,
  selectedNamespaceRow,
  selectedOutsideTable,
  selectedTableRow,
  tableChoice,
  tablePlace,
  unusableNamespaceIds,
  writeBlock,
  writerDefault,
} from "./catalogPick";
import * as catalogPick from "./catalogPick";

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
  full_table_name: "raw.sales",
  qualified_name: "Finance.raw.sales",
  source: "hub",
});

// The hub's catalog comes first on purpose: the options still list the local ones first.
const tree: NamespaceTree[] = [
  ns({
    id: -10,
    name: "Finance",
    source: "hub",
    children: [
      ns({ id: -20, name: "raw", parent_id: -10, source: "hub", tables: [hubSales] }),
      ns({ id: -21, name: "Local Flows", parent_id: -10, source: "hub" }),
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

// A catalog kept elsewhere, with one schema it still lists and one it no longer does.
const polaris = ns({
  id: 7,
  name: "polaris",
  kept_elsewhere: true,
  children: [
    ns({ id: 8, name: "sales", parent_id: 7, level: 1, kept_elsewhere: true }),
    ns({
      id: 9,
      name: "old",
      parent_id: 7,
      level: 1,
      kept_elsewhere: true,
      gone_since: "2026-11-22T10:00:00",
      unavailable_reason: "No longer in polaris's own catalog.",
    }),
  ],
});

const reader = (fields: Partial<NodeCatalogReader> = {}): NodeCatalogReader =>
  ({
    source: "local",
    catalog_table_id: null,
    catalog_namespace_id: null,
    catalog_table_name: null,
    catalog_full_table_name: null,
    ...fields,
  }) as NodeCatalogReader;

const writer = (fields: Partial<CatalogWriteSettings> = {}): CatalogWriteSettings =>
  ({
    table_name: "out",
    source: "local",
    namespace_id: null,
    namespace_full_name: null,
    ...fields,
  }) as CatalogWriteSettings;

describe("namespace options", () => {
  it("lists the local schemas first, then the hub's, marked by source", () => {
    expect(namespaceOptions(tree).map((o) => [o.id, o.source, o.hubId])).toEqual([
      [2, "local", null],
      [-20, "hub", 20],
      [-21, "hub", 21],
    ]);
  });

  it("offers a catalog kept elsewhere among this machine's, its schemas flagged", () => {
    const options = namespaceOptions([...tree, polaris], true);
    expect(options.map((o) => [o.label, o.source, o.keptElsewhere])).toEqual([
      ["General / default", "local", false],
      ["General / Local Flows", "local", false],
      ["polaris / sales", "local", true],
      ["polaris / old", "local", true],
      ["Finance / raw", "hub", false],
      ["Finance / Local Flows", "hub", false],
    ]);
    expect(options.find((o) => o.id === 8)).toMatchObject({ catalogId: 7, full: "polaris.sales" });
  });

  it("keeps a schema the other catalog no longer lists out of the picker", () => {
    const options = namespaceOptions([polaris]);
    expect(pickable(options, null).map((o) => o.full)).toEqual(["polaris.sales"]);
    expect(pickable(options, 9).map((o) => o.full)).toEqual(["polaris.sales", "polaris.old"]);
  });

  it("keeps the local system schemas when asked, as the reader does", () => {
    expect(namespaceOptions(tree, true).map((o) => o.label)).toEqual([
      "General / default",
      "General / Local Flows",
      "Finance / raw",
      "Finance / Local Flows",
    ]);
  });
});

describe("a reader's table choice", () => {
  it("writes a local table's fields as before, as this machine's", () => {
    expect(tableChoice(localOrders)).toEqual({
      source: "local",
      catalog_table_id: 4,
      catalog_namespace_id: 2,
      catalog_table_name: "orders",
      catalog_full_table_name: "General.default.orders",
    });
  });

  it("names a hub table by its qualified name only, never an id of the hub's or a negative one", () => {
    const writes = tableChoice(hubSales);
    expect(writes).toEqual({
      source: "hub",
      catalog_table_name: "sales",
      catalog_full_table_name: "Finance.raw.sales",
      catalog_table_id: null,
      catalog_namespace_id: null,
    });
    const saved = { ...reader({ catalog_table_id: 4, catalog_namespace_id: 2 }), ...writes };
    expect(Object.values(saved).filter((v) => typeof v === "number")).toEqual([]);
  });

  it("clears the table, local or hub, and keeps where it lives and the namespace", () => {
    expect(tableChoice(null)).toEqual({
      catalog_table_id: null,
      catalog_table_name: null,
      catalog_full_table_name: null,
    });
  });

  it("writes a picked schema as where it lives and, for this machine's, its id", () => {
    const [local, hub] = namespaceOptions(tree);
    expect(readerNamespaceChoice(local)).toEqual({ source: "local", catalog_namespace_id: 2 });
    expect(readerNamespaceChoice(hub)).toEqual({ source: "hub", catalog_namespace_id: null });
    expect(readerNamespaceChoice(null)).toEqual({ source: "local", catalog_namespace_id: null });
  });
});

describe("a writer's namespace choice", () => {
  const [local, hub] = namespaceOptions(tree);

  it("writes a local schema's id and full name, a hub schema's full name only, and clears both", () => {
    expect(namespaceChoice(local)).toEqual({
      source: "local",
      namespace_id: 2,
      namespace_full_name: "General.default",
    });
    expect(namespaceChoice(hub)).toEqual({
      source: "hub",
      namespace_id: null,
      namespace_full_name: "Finance.raw",
    });
    expect(namespaceChoice(null)).toEqual({
      source: "local",
      namespace_id: null,
      namespace_full_name: null,
    });
  });
});

describe("restoring a saved choice", () => {
  const options = namespaceOptions(tree, true);
  const tables = [localOrders, hubSales];

  it("finds a saved hub reader's schema and table, and a hub writer's schema, by their names", () => {
    const saved = reader(tableChoice(hubSales));
    expect(selectedNamespaceRow(saved, options)?.id).toBe(-20);
    expect(selectedTableRow(saved, tables)).toBe(hubSales);
    const hubWriter = writer({ source: "hub", namespace_full_name: "Finance.raw" });
    expect(selectedNamespaceRow(hubWriter, options)?.id).toBe(-20);
  });

  it("finds nothing for a hub target that names no table or schema yet", () => {
    expect(selectedNamespaceRow(reader({ source: "hub" }), options)).toBeNull();
    expect(selectedTableRow(reader({ source: "hub" }), tables)).toBeNull();
    expect(selectedNamespaceRow(writer({ source: "hub" }), options)).toBeNull();
  });

  it("finds a saved local reader by its local ids", () => {
    const saved = reader(tableChoice(localOrders));
    expect(selectedNamespaceRow(saved, options)?.id).toBe(2);
    expect(selectedTableRow(saved, tables)).toBe(localOrders);
  });

  it("never matches a local id against a hub row's negative id, or a hub name against a local row", () => {
    expect(selectedTableRow(reader({ catalog_table_id: -30 }), tables)).toBeNull();
    const localName = reader({ source: "hub", catalog_full_table_name: "General.default.orders" });
    expect(selectedTableRow(localName, tables)).toBeNull();
    expect(selectedNamespaceRow(writer({ namespace_id: -20 }), options)).toBeNull();
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
    const hubWriter = writer({ source: "hub", namespace_full_name: "Finance.raw" });
    expect(selectedNamespaceRow(hubWriter, local)).toBeNull();
    expect(selectedTableRow(reader(tableChoice(hubSales)), [localOrders])).toBeNull();
  });
});

describe("hub targets", () => {
  it("is a reader of a hub table or a writer into a hub schema", () => {
    expect(isHubTarget(reader(tableChoice(hubSales)))).toBe(true);
    expect(isHubTarget(reader(tableChoice(localOrders)))).toBe(false);
    expect(isHubTarget(reader())).toBe(false);
    expect(isHubTarget(writer({ source: "hub", namespace_full_name: "Finance.raw" }))).toBe(true);
    expect(isHubTarget(writer({ namespace_id: 2 }))).toBe(false);
    expect(isHubTarget(writer())).toBe(false);
  });
});

describe("a table kept by another catalog", () => {
  const options = namespaceOptions([...tree, polaris], true);
  const sales = options.find((o) => o.full === "polaris.sales")!;

  it("is taken by its name only: no table id and no schema id of this install's", () => {
    const writes = outsideTableChoice(sales, "orders");
    expect(writes).toEqual({
      source: "local",
      catalog_table_name: "orders",
      catalog_full_table_name: "polaris.sales.orders",
      catalog_table_id: null,
      catalog_namespace_id: null,
    });
    const saved = { ...reader({ catalog_table_id: 4, catalog_namespace_id: 2 }), ...writes };
    expect(Object.values(saved).filter((v) => typeof v === "number")).toEqual([]);
    expect(readerNamespaceChoice(sales)).toEqual({ source: "local", catalog_namespace_id: null });
  });

  it("finds a saved reader's schema and table again by that name", () => {
    const saved = reader(outsideTableChoice(sales, "orders"));
    expect(selectedOutsideTable(saved, options)).toEqual({ option: sales, name: "orders" });
    expect(selectedNamespaceRow(saved, options)).toBe(sales);
    expect(selectedTableRow(saved, [localOrders])).toBeNull();
    const byNameOnly = reader({ catalog_full_table_name: "polaris.sales.orders" });
    expect(selectedOutsideTable(byNameOnly, options)?.name).toBe("orders");
  });

  it("finds a reader that names the table by its schema's id and the table's name, as core does", () => {
    const byId = reader({ catalog_namespace_id: sales.id, catalog_table_name: "orders" });
    expect(selectedOutsideTable(byId, options)).toEqual({ option: sales, name: "orders" });
    const byIdAndShortName = reader({
      catalog_namespace_id: sales.id,
      catalog_full_table_name: "orders",
    });
    expect(selectedOutsideTable(byIdAndShortName, options)?.name).toBe("orders");
    expect(selectedOutsideTable(reader({ catalog_namespace_id: sales.id }), options)).toBeNull();
  });

  it("never takes a local table's name for one kept elsewhere", () => {
    expect(selectedOutsideTable(reader(tableChoice(localOrders)), options)).toBeNull();
    const localName = reader({ catalog_full_table_name: "General.default.orders" });
    expect(selectedOutsideTable(localName, options)).toBeNull();
    expect(selectedOutsideTable(reader(tableChoice(hubSales)), options)).toBeNull();
  });

  it("gives the writer the schema by its name only, and finds it again by that name", () => {
    const writes = namespaceChoice(sales);
    expect(writes).toEqual({
      source: "local",
      namespace_id: null,
      namespace_full_name: "polaris.sales",
    });
    expect(selectedNamespaceRow(writer(writes), options)).toBe(sales);
    expect(isHubTarget(writer(writes))).toBe(false);
  });

  it("warns a saved reader that asks for changes or a version in core's own words", () => {
    // flowfile_core external_catalogs.outside_read_refusal words a refused run the same.
    expect(outsideNoChanges("polaris")).toBe(
      "polaris keeps this table. Flowfile can read it only as it is now; reading its changes is not available there.",
    );
    expect(outsideNoVersion("polaris")).toBe(
      "polaris keeps this table. Flowfile can read it only as it is now; reading it at a version is not available there.",
    );
  });

  it("exports no sentence about SQL mode", () => {
    expect(Object.keys(catalogPick)).not.toContain("OUTSIDE_NO_SQL");
  });
});

describe("pickers by place and reach", () => {
  const ON_ITS_OWN_DISK = "The hub keeps the tables of scratch.notes on its own disk";
  const USE = { is_owner: false, access_level: "use" as const, shared_by: "bob" };
  const MANAGE = { is_owner: false, access_level: "manage" as const, shared_by: "bob" };
  const OWNER = { is_owner: true, access_level: "owner" as const, shared_by: null };
  const hubCatalog = (id: number, name: string, schemas: Partial<NamespaceTree>[], fields = {}) =>
    ns({
      id: -id,
      name,
      source: "hub",
      place: "Acme Data",
      ...fields,
      children: schemas.map((schema) => ns({ parent_id: -id, source: "hub", ...schema })),
    });
  const desktop: NamespaceTree[] = [
    ns({
      id: 1,
      name: "General",
      place: "On this computer",
      children: [ns({ id: 2, name: "default", parent_id: 1 })],
    }),
    hubCatalog(10, "main", [{ id: -11, name: "default", access: MANAGE }]),
    hubCatalog(20, "scratch", [{ id: -21, name: "notes", unavailable_reason: ON_ITS_OWN_DISK }]),
  ];
  const joined: NamespaceTree[] = desktop.map((catalog) =>
    catalog.source === "hub" ? catalog : { ...catalog, offered: false },
  );

  it("heads the hub's schemas and this machine's each with the place core names", () => {
    const options = namespaceOptions(desktop);
    const groups = optionGroups(pickable(options, null), (o) => o.place);
    expect(groups.map((g) => [g.label, g.rows.map((o) => o.label)])).toEqual([
      ["On this computer", ["General / default"]],
      ["Acme Data", ["main / default"]],
    ]);
  });

  it("offers a joined server's people the hub's catalogs only, as one group with no heading", () => {
    const options = namespaceOptions(joined);
    expect(options.map((o) => o.label)).toEqual(["main / default", "scratch / notes"]);
    const groups = optionGroups(pickable(options, null), (o) => o.place);
    expect(groups.map((g) => [g.label, g.rows.map((o) => o.label)])).toEqual([
      ["", ["main / default"]],
    ]);
  });

  it("lists a desktop without a hub as one group with no heading, as before", () => {
    const options = namespaceOptions(tree.filter((c) => c.source !== "hub"));
    const groups = optionGroups(pickable(options, null), (o) => o.place);
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

  it("names each table's place by its schema's catalog", () => {
    const options = namespaceOptions(desktop);
    expect(tablePlace(table({ id: 4 }), options)).toBe("On this computer");
    expect(tablePlace(table({ id: -40, source: "hub", namespace_id: -11 }), options)).toBe(
      "Acme Data",
    );
    expect(tablePlace(table({ id: 5, namespace_id: 99 }), options)).toBeNull();
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
        hubCatalog(10, "main", [{ id: -11, name: "default", access: USE }]),
        hubCatalog(50, "team", [
          { id: -51, name: "raw", access: USE },
          { id: -52, name: "curated", access: MANAGE },
        ]),
      ];
      expect(hubDefaultSchema(namespaceOptions(tree))?.full).toBe("team.curated");
    });

    it("greys a schema the person may only read, says why, and lists it", () => {
      const tree = [
        hubCatalog(10, "main", [
          { id: -11, name: "default", access: USE },
          { id: -12, name: "team", access: MANAGE },
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
      const tree = [
        ns({
          id: 1,
          name: "Sales",
          access: USE,
          children: [ns({ id: 2, name: "shared", parent_id: 1, access: USE })],
        }),
      ];
      const [option] = namespaceOptions(tree);
      expect([option.source, option.readOnly, writeBlock(option)]).toEqual(["local", false, null]);
    });

    it("names this machine's reach before read-only when both hold", () => {
      const tree = [
        hubCatalog(20, "scratch", [{ id: -21, name: "notes", access: USE }], {
          unavailable_reason: "on its own disk",
        }),
      ];
      expect(writeBlock(namespaceOptions(tree)[0])).toBe("on its own disk");
    });

    it("takes a catalog's grant for its schemas, and is nothing when no hub schema may be written", () => {
      const tree = [hubCatalog(10, "main", [{ id: -11, name: "default" }], { access: USE })];
      expect(namespaceOptions(tree)[0].writable).toBe(false);
      expect(hubDefaultSchema(namespaceOptions(tree))).toBeNull();
    });

    it("never counts a hub schema the hub stamped nothing on, nor preselects it", () => {
      const tree = [hubCatalog(10, "main", [{ id: -11, name: "default" }])];
      const [option] = namespaceOptions(tree);
      expect([option.writable, option.readOnly]).toEqual([false, false]);
      expect(writerDefault(namespaceOptions(tree))).toBeNull();
    });

    it("counts a hub schema the person owns as theirs to write", () => {
      const tree = [hubCatalog(10, "main", [{ id: -11, name: "default" }], { access: OWNER })];
      expect(writerDefault(namespaceOptions(tree))?.full).toBe("main.default");
    });

    it("still counts a local schema with no access as writable, as an unrestricted desktop has it", () => {
      expect(namespaceOptions(tree)[0]).toMatchObject({ source: "local", writable: true });
    });

    it("writes the hub schema's name only when chosen", () => {
      const preset = writerDefault(namespaceOptions(joined));
      expect(namespaceChoice(preset)).toEqual({
        source: "hub",
        namespace_id: null,
        namespace_full_name: "main.default",
      });
    });
  });
});
