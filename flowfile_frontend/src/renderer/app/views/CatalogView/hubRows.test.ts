// Guards which catalog rows count as the hub's and the filter local actions use.

import { describe, it, expect } from "vitest";
import {
  byPlace,
  hubFlowVersionTitle,
  hubPresence,
  offersNew,
  offersOwnCatalog,
  rootOf,
  showsPlaces,
  withoutHubRows,
} from "./hubRows";

describe("withoutHubRows", () => {
  it("drops the hub's rows and keeps the local ones in order", () => {
    const rows = [
      { id: 1, source: "local" as const },
      { id: -2, source: "hub" as const },
      { id: 3 },
      { id: -4, source: "hub" as const },
    ];
    expect(withoutHubRows(rows).map((r) => r.id)).toEqual([1, 3]);
  });
});

describe("hubPresence", () => {
  const local = { source: "local" };
  const hub = { source: "hub" };

  it("is none without hub rows, as a desktop with no hub lists them", () => {
    expect(hubPresence([local, {}])).toBe("none");
    expect(hubPresence([])).toBe("none");
  });

  it("is only where every offered row is the hub's, this machine's not offered", () => {
    expect(hubPresence([{ ...local, offered: false }, hub])).toBe("only");
    expect(hubPresence([hub])).toBe("only");
  });

  it("is beside where the hub's rows show next to this machine's", () => {
    expect(hubPresence([local, hub])).toBe("beside");
  });
});

describe("offersOwnCatalog", () => {
  it("is true on a desktop, with or without a hub's rows beside its own", () => {
    expect(offersOwnCatalog([{ source: "local" }, {}])).toBe(true);
    expect(offersOwnCatalog([{ source: "local", offered: true }, { source: "hub" }])).toBe(true);
    expect(offersOwnCatalog([])).toBe(true);
  });

  it("is false where core marks this machine's rows not offered, a server that joined a hub", () => {
    expect(offersOwnCatalog([{ source: "local", offered: false }, { source: "hub" }])).toBe(false);
    expect(offersOwnCatalog([{ source: "local", offered: false }])).toBe(false);
  });
});

describe("place", () => {
  const rows = [
    { id: 1, place: "On this computer" },
    { id: 2, place: "On this computer" },
    { id: -3, place: "Acme Data" },
    { id: -4, place: "Acme Data" },
  ];

  it("groups the rows of one place together, this machine's first as core lists them", () => {
    const groups = byPlace(rows);
    expect(groups.map((g) => [g.place, g.rows.map((r) => r.id)])).toEqual([
      ["On this computer", [1, 2]],
      ["Acme Data", [-3, -4]],
    ]);
    expect(showsPlaces(groups)).toBe(true);
  });

  it("shows no headings where one place shows, or where core names no place", () => {
    expect(showsPlaces(byPlace(rows.slice(2)))).toBe(false);
    expect(
      showsPlaces(
        byPlace([
          { id: 1, place: null },
          { id: 2, place: null },
        ]),
      ),
    ).toBe(false);
    expect(byPlace([])).toEqual([]);
  });
});

describe("offersNew", () => {
  it("offers New where this machine's catalogs show, and hides it where all are the hub's or one is selected", () => {
    expect(offersNew("none", null)).toBe(true);
    expect(offersNew("none", { source: "local" })).toBe(true);
    expect(offersNew("beside", null)).toBe(true);
    expect(offersNew("beside", { source: "local" })).toBe(true);
    expect(offersNew("only", null)).toBe(false);
    expect(offersNew("only", { source: "hub" })).toBe(false);
    expect(offersNew("beside", { source: "hub" })).toBe(false);
  });
});

describe("hubFlowVersionTitle", () => {
  it("names the newest version and who published it", () => {
    expect(hubFlowVersionTitle({ latest_version: 3, author_name: "ana" })).toBe(
      "Version 3 on the hub, published by ana",
    );
  });

  it("names the version alone when the hub does not say who published it", () => {
    expect(hubFlowVersionTitle({ latest_version: 1, author_name: null })).toBe(
      "Version 1 on the hub",
    );
  });
});

describe("rootOf", () => {
  const tree = [
    {
      id: 1,
      flows: [{ id: 4 }],
      children: [{ id: 2, flows: [{ id: 5 }], children: [] }],
    },
    {
      id: -10,
      flows: [],
      children: [{ id: -11, flows: [{ id: -20 }, { id: -21 }], children: [] }],
    },
  ];

  it("gives the catalog that holds a schema, or the catalog itself", () => {
    expect(rootOf(tree, -11)?.id).toBe(-10);
    expect(rootOf(tree, 2)?.id).toBe(1);
    expect(rootOf(tree, 1)?.id).toBe(1);
    expect(rootOf(tree, 7)).toBeNull();
  });
});
