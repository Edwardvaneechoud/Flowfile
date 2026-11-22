// Where a new chart is saved, and what it keeps as its source there.

import { describe, expect, it } from "vitest";
import type { SaveTargetOption } from "../../composables/useWritableNamespaces";
import {
  LOCAL_TABLE_STAYS_LOCAL,
  chartSourceForSave,
  chartTargets,
  pickableCatalogs,
  pickedSource,
  targetsInSameCatalog,
} from "./chartSaveTarget";
import { NO_HUB_SCHEMA_TO_SAVE_IN, chartSource, queryOver } from "./hubRows";

const own: SaveTargetOption = { id: 2, label: "General / default", hub: false, place: "Here" };
const curated: SaveTargetOption = { id: -11, label: "sales / curated", hub: true, place: "Acme" };
const mine: SaveTargetOption = { id: -13, label: "sales / mine", hub: true, place: "Acme" };

const orders = {
  id: -101,
  name: "orders",
  namespace_id: -12,
  source: "hub" as const,
  qualified_name: "sales.raw.orders",
};
const local = { id: 7, name: "stock", namespace_id: 2, source: "local" as const };

const hubTable = chartSource(orders);
const localTable = chartSource(local);
const query = { source_type: "sql" as const, sql_query: "SELECT 1" };

describe("chartSource", () => {
  it("names a table by its id, a hub table by the negative id it has here", () => {
    expect(hubTable).toEqual({ source_type: "table", table_id: -101 });
    expect(localTable).toEqual({ source_type: "table", table_id: 7 });
  });
});

describe("chartTargets", () => {
  it("asks nothing and adds no note where no hub is shown", () => {
    const targets = chartTargets(localTable, [own], { ownOffered: true, hubShown: false });
    expect(targets).toEqual({
      asks: false,
      options: [],
      defaultId: null,
      note: null,
      blocked: null,
    });
    expect(chartTargets(query, [own], { ownOffered: true, hubShown: false }).asks).toBe(false);
  });

  it("offers a chart of a table on this machine no hub schema, and says why", () => {
    const targets = chartTargets(localTable, [own, curated], { ownOffered: true, hubShown: true });
    expect(targets.asks).toBe(false);
    expect(targets.options).toEqual([]);
    expect(targets.note).toBe(LOCAL_TABLE_STAYS_LOCAL);
  });

  it("offers a chart of a hub table this machine's schemas first, the hub's after, and starts on the hub", () => {
    const targets = chartTargets(hubTable, [own, curated, mine], {
      ownOffered: true,
      hubShown: true,
    });
    expect(targets.asks).toBe(true);
    expect(targets.options.map((o) => o.id)).toEqual([2, -11, -13]);
    expect(targets.defaultId).toBe(-11);
    expect(targets.note).toBeNull();
  });

  it("starts a chart of a hub table in that table's own schema when the person may write it", () => {
    const targets = chartTargets(
      hubTable,
      [own, curated, mine],
      { ownOffered: true, hubShown: true },
      -13,
    );
    expect(targets.defaultId).toBe(-13);
  });

  it("starts a query chart on this machine on a desktop, with no schema as before", () => {
    const targets = chartTargets(query, [own, curated], { ownOffered: true, hubShown: true });
    expect(targets.asks).toBe(true);
    expect(targets.defaultId).toBeNull();
    expect(targets.blocked).toBeNull();
  });

  it("starts every chart in a hub schema on a server that joined a hub", () => {
    const joined = { ownOffered: false, hubShown: true };
    expect(chartTargets(query, [curated, mine], joined).defaultId).toBe(-11);
    expect(chartTargets(hubTable, [curated, mine], joined).defaultId).toBe(-11);
  });

  it("blocks the save on a joined server when the person may write no hub schema", () => {
    const targets = chartTargets(hubTable, [], { ownOffered: false, hubShown: true });
    expect(targets.asks).toBe(true);
    expect(targets.blocked).toBe(NO_HUB_SCHEMA_TO_SAVE_IN);
  });
});

describe("chartSourceForSave", () => {
  it("keeps a hub table's id when the chart is saved in a hub schema", () => {
    expect(chartSourceForSave(hubTable, orders, true)).toEqual(hubTable);
  });

  it("keeps a query over a hub table when the chart is saved on this machine", () => {
    expect(chartSourceForSave(hubTable, orders, false)).toEqual({
      source_type: "sql",
      sql_query: "SELECT * FROM sales.raw.orders",
    });
    expect(queryOver({ ...orders, qualified_name: "sales.raw.count-rows" })).toBe(
      'SELECT * FROM sales.raw."count-rows"',
    );
  });

  it("leaves a table of this machine and a query as they are", () => {
    expect(chartSourceForSave(localTable, local, false)).toEqual(localTable);
    expect(chartSourceForSave(query, null, true)).toEqual(query);
  });
});

describe("targetsInSameCatalog", () => {
  it("offers a saved chart only the schemas of the catalog that keeps it", () => {
    expect(targetsInSameCatalog([own, curated, mine], { id: -301, source: "hub" })).toEqual([
      curated,
      mine,
    ]);
    expect(targetsInSameCatalog([own, curated], { id: 5 })).toEqual([own]);
  });
});

describe("the tables a new chart is offered", () => {
  const catalogs = [
    { id: 1, source: "local" as const },
    { id: 3, source: "local" as const, offered: false },
    { id: -10, source: "hub" as const },
  ];

  it("leaves out a catalog core marks not offered", () => {
    expect(pickableCatalogs(catalogs).map((c) => c.id)).toEqual([1, -10]);
  });

  it("offers a chart of a hub dashboard the hub's tables only", () => {
    expect(pickableCatalogs(catalogs, "hub").map((c) => c.id)).toEqual([-10]);
  });

  it("offers a chart of this machine's dashboard every offered table, a hub's as a query over it", () => {
    expect(pickableCatalogs(catalogs, "local").map((c) => c.id)).toEqual([1, -10]);
    expect(pickedSource(orders, "local")).toEqual({
      source_type: "sql",
      sql_query: "SELECT * FROM sales.raw.orders",
    });
    expect(pickedSource(local, "local")).toEqual(localTable);
  });

  it("names a hub table by its id for a hub dashboard and where the editor asks at save", () => {
    expect(pickedSource(orders, "hub")).toEqual(hubTable);
    expect(pickedSource(orders)).toEqual(hubTable);
  });
});
