// Which catalog keeps a dashboard, where a new one is saved, and the charts and tables it may then use.

import { describe, expect, it } from "vitest";
import type { SaveTargetOption } from "../../composables/useWritableNamespaces";
import { NO_HUB_SCHEMA_TO_SAVE_IN } from "../CatalogView/hubRows";
import {
  chartsOfDashboardCatalog,
  dashboardOnHub,
  dashboardTargets,
  tablesOfDashboardCatalog,
} from "./dashboardCatalog";

const own: SaveTargetOption = { id: 2, label: "General / default", hub: false, place: "Here" };
const curated: SaveTargetOption = { id: -11, label: "sales / curated", hub: true, place: "Acme" };
const mine: SaveTargetOption = { id: -13, label: "sales / mine", hub: true, place: "Acme" };

const desktop = { ownOffered: true, hubShown: true };
const joined = { ownOffered: false, hubShown: true };

describe("dashboardOnHub", () => {
  it("knows a saved hub dashboard by its negative id and a new one by the schema picked for it", () => {
    expect(dashboardOnHub({ id: -401, namespace_id: -11 })).toBe(true);
    expect(dashboardOnHub({ id: 0, namespace_id: -11 })).toBe(true);
    expect(dashboardOnHub({ id: 0, namespace_id: null })).toBe(false);
    expect(dashboardOnHub({ id: 12, namespace_id: null })).toBe(false);
    expect(dashboardOnHub(null)).toBe(false);
  });
});

describe("dashboardTargets", () => {
  it("asks nothing where no hub is shown: a dashboard is saved as it always was", () => {
    const targets = dashboardTargets([own], { ownOffered: true, hubShown: false });
    expect(targets).toEqual({ asks: false, options: [], defaultId: null, blocked: null });
  });

  it("starts on this machine on a desktop and offers the hub's schemas beside it", () => {
    const targets = dashboardTargets([own, curated, mine], desktop);
    expect(targets.asks).toBe(true);
    expect(targets.options.map((o) => o.id)).toEqual([-11, -13]);
    expect(targets.defaultId).toBeNull();
    expect(targets.blocked).toBeNull();
  });

  it("starts in the first hub schema the person may write on a server that joined a hub", () => {
    expect(dashboardTargets([curated, mine], joined).defaultId).toBe(-11);
  });

  it("blocks a new dashboard on a joined server when the person may write no hub schema", () => {
    const targets = dashboardTargets([], joined);
    expect(targets.blocked).toBe(NO_HUB_SCHEMA_TO_SAVE_IN);
    expect(targets.defaultId).toBeNull();
  });
});

describe("what a dashboard may use", () => {
  const charts = [
    { id: 5, source: "local" as const },
    { id: -301, source: "hub" as const },
    { id: -302 },
  ];
  const localTables = [{ id: 7, name: "stock" }];
  const tree = [
    {
      id: 1,
      source: "local" as const,
      tables: [],
      children: [{ id: 2, tables: localTables, children: [] }],
    },
    {
      id: -10,
      source: "hub" as const,
      tables: [],
      children: [
        { id: -12, source: "hub" as const, tables: [{ id: -101, name: "orders" }], children: [] },
      ],
    },
    {
      id: -40,
      source: "hub" as const,
      unavailable_reason: "on the hub's own disk",
      tables: [],
      children: [{ id: -41, tables: [{ id: -102, name: "old" }], children: [] }],
    },
  ];

  it("offers a hub dashboard the hub's charts only", () => {
    const offered = chartsOfDashboardCatalog(charts, { id: -401, namespace_id: -11 });
    expect(offered.map((c) => c.id)).toEqual([-301, -302]);
  });

  it("offers a dashboard of this machine this machine's charts only", () => {
    expect(chartsOfDashboardCatalog(charts, { id: 12 }).map((c) => c.id)).toEqual([5]);
    expect(
      chartsOfDashboardCatalog(charts, { id: 0, namespace_id: null }).map((c) => c.id),
    ).toEqual([5]);
  });

  it("offers a hub dashboard the tables of the hub's catalogs this Flowfile can use", () => {
    const offered = tablesOfDashboardCatalog(localTables, tree, { id: 0, namespace_id: -11 });
    expect(offered.map((t) => t.id)).toEqual([-101]);
  });

  it("leaves a dashboard of this machine the tables it always listed", () => {
    expect(tablesOfDashboardCatalog(localTables, tree, { id: 12 })).toEqual(localTables);
  });
});
