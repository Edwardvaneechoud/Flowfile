import { describe, expect, it, vi } from "vitest";
import type { NamespaceTree } from "../types/catalog.types";
import type { AccessInfo } from "../types/sharing.types";

// Mock the composable's Pinia deps so importing it doesn't pull the axios/auth
// chain (auth.service touches localStorage at load) into the node test env.
vi.mock("../stores/catalog-store", () => ({ useCatalogStore: () => ({ tree: [] }) }));
vi.mock("./useResourceSharing", () => ({
  useResourceSharing: () => ({ canManage: () => true }),
}));

import {
  collectSaveTargetOptions,
  collectWritableSchemaOptions,
  groupSaveTargets,
  pruneNonWritableSchemas,
} from "./useWritableNamespaces";

// canManage semantics from useResourceSharing, without Pinia (no admin override).
const canManage = (r: { access?: AccessInfo | null }) =>
  !r.access || r.access.is_owner || r.access.access_level === "manage";

const ns = (
  id: number,
  name: string,
  overrides: Partial<NamespaceTree> = {},
  children: NamespaceTree[] = [],
): NamespaceTree => ({
  id,
  name,
  parent_id: null,
  level: 0,
  description: null,
  owner_id: 1,
  created_at: "",
  updated_at: "",
  children,
  flows: [],
  artifacts: [],
  tables: [],
  visualizations: [],
  notebooks: [],
  ...overrides,
});

const access = (level: AccessInfo["access_level"], isOwner = false): AccessInfo => ({
  is_owner: isOwner,
  access_level: level,
});

const tree = (): NamespaceTree[] => [
  ns(1, "General", {}, [
    ns(2, "default", { parent_id: 1, level: 1 }),
    ns(3, "reports", { parent_id: 1, level: 1 }),
  ]),
  ns(10, "demat", { access: access("use") }, [
    ns(11, "main", { parent_id: 10, level: 1, access: access("use") }),
  ]),
  ns(20, "team", { access: access("manage") }, [
    ns(21, "shared", { parent_id: 20, level: 1, access: access("manage") }),
    ns(22, "restricted", { parent_id: 20, level: 1, access: access("use") }),
  ]),
  ns(30, "mine", { access: access("owner", true) }, [
    ns(31, "personal", { parent_id: 30, level: 1, access: access("owner", true) }),
  ]),
];

describe("collectWritableSchemaOptions", () => {
  it("excludes use-level (read-only) schemas", () => {
    const options = collectWritableSchemaOptions(tree(), canManage);
    const ids = options.map((o) => o.id);
    expect(ids).not.toContain(11);
    expect(ids).not.toContain(22);
  });

  it("keeps owner, manage, and unrestricted (null access) schemas", () => {
    const options = collectWritableSchemaOptions(tree(), canManage);
    expect(options.map((o) => o.id)).toEqual([2, 3, 21, 31]);
  });

  it("labels options as 'catalog / schema'", () => {
    const options = collectWritableSchemaOptions(tree(), canManage);
    expect(options.find((o) => o.id === 21)?.label).toBe("team / shared");
  });

  it("still strips system schemas under General", () => {
    const withSystem = [
      ns(1, "General", {}, [
        ns(2, "default", { parent_id: 1, level: 1 }),
        ns(4, "Python Editor", { parent_id: 1, level: 1 }),
        ns(5, "sync", { parent_id: 1, level: 1 }),
      ]),
    ];
    const options = collectWritableSchemaOptions(withSystem, canManage);
    expect(options.map((o) => o.id)).toEqual([2]);
  });

  it("is unchanged in unrestricted mode (all access null)", () => {
    const unrestricted = [
      ns(1, "cat", {}, [
        ns(2, "a", { parent_id: 1, level: 1 }),
        ns(3, "b", { parent_id: 1, level: 1 }),
      ]),
    ];
    expect(collectWritableSchemaOptions(unrestricted, canManage)).toHaveLength(2);
  });

  it("inherits the catalog-level grant for unstamped schemas (backend stamps direct grants only)", () => {
    const inherited = [
      ns(40, "shared-use", { access: access("use") }, [
        ns(41, "unstamped", { parent_id: 40, level: 1 }),
      ]),
      ns(50, "shared-manage", { access: access("manage") }, [
        ns(51, "unstamped", { parent_id: 50, level: 1 }),
        ns(52, "direct-use", { parent_id: 50, level: 1, access: access("use") }),
      ]),
    ];
    const ids = collectWritableSchemaOptions(inherited, canManage).map((o) => o.id);
    expect(ids).not.toContain(41);
    expect(ids).toContain(51);
    // A direct stamp on the schema wins over the parent's.
    expect(ids).not.toContain(52);
  });
});

describe("pruneNonWritableSchemas", () => {
  it("removes read-only schemas but keeps catalog containers", () => {
    const pruned = pruneNonWritableSchemas(tree(), canManage);
    expect(pruned).toHaveLength(4);
    const demat = pruned.find((n) => n.id === 10);
    expect(demat?.children).toHaveLength(0);
    const team = pruned.find((n) => n.id === 20);
    expect(team?.children.map((c) => c.id)).toEqual([21]);
  });

  it("does not mutate the input tree", () => {
    const input = tree();
    pruneNonWritableSchemas(input, canManage);
    expect(input.find((n) => n.id === 20)?.children).toHaveLength(2);
  });
});

// A hub's catalogs as core lists them after this machine's own: negative ids, the hub's name as their place.
const hubCatalog = (
  id: number,
  name: string,
  schemas: [number, string, AccessInfo | null][],
  overrides: Partial<NamespaceTree> = {},
): NamespaceTree =>
  ns(
    id,
    name,
    { source: "hub", place: "Acme Data", ...overrides },
    schemas.map(([schemaId, schemaName, schemaAccess]) =>
      ns(schemaId, schemaName, {
        parent_id: id,
        level: 1,
        source: "hub",
        place: "Acme Data",
        access: schemaAccess,
      }),
    ),
  );

const own = (offered: boolean): NamespaceTree =>
  ns(1, "General", { place: "On this server", offered }, [
    ns(2, "default", { parent_id: 1, level: 1, place: "On this server", offered }),
  ]);

const sales = () =>
  hubCatalog(-10, "sales", [
    [-11, "curated", access("manage")],
    [-12, "raw", access("use")],
    [-13, "mine", access("owner", true)],
  ]);

describe("collectWritableSchemaOptions on a server that joined a hub", () => {
  it("does not offer this server's own schemas, which core marks not offered", () => {
    expect(collectWritableSchemaOptions([own(false), sales()], canManage)).toEqual([]);
  });

  it("never offers a hub schema: flows, tables and query views are not saved there", () => {
    const ids = collectWritableSchemaOptions([own(true), sales()], canManage).map((o) => o.id);
    expect(ids).toEqual([2]);
  });
});

describe("collectSaveTargetOptions", () => {
  it("offers a hub schema the person may write, and none they may only use", () => {
    const options = collectSaveTargetOptions([own(false), sales()], canManage);
    expect(options.map((o) => o.id)).toEqual([-11, -13]);
    expect(options.every((o) => o.hub && o.place === "Acme Data")).toBe(true);
    expect(options[0].label).toBe("sales / curated");
  });

  it("offers this computer's own schemas first and the hub's after them", () => {
    const options = collectSaveTargetOptions([own(true), sales()], canManage);
    expect(options.map((o) => [o.id, o.hub])).toEqual([
      [2, false],
      [-11, true],
      [-13, true],
    ]);
  });

  it("does not offer a schema marked not offered", () => {
    const options = collectSaveTargetOptions([own(false), sales()], canManage);
    expect(options.some((o) => o.id === 2)).toBe(false);
  });

  it("inherits the hub catalog's grant for a schema the hub did not stamp", () => {
    const granted = hubCatalog(-20, "finance", [[-21, "ledger", null]], { access: access("use") });
    const managed = hubCatalog(-30, "ops", [[-31, "runs", null]], { access: access("manage") });
    const ids = collectSaveTargetOptions([granted, managed], canManage).map((o) => o.id);
    expect(ids).toEqual([-31]);
  });

  it("does not take this install's admin for the hub's: a use-only hub schema stays out", () => {
    // useResourceSharing.canManage for an admin of this install: admin of local rows only.
    const asLocalAdmin = (r: { access?: AccessInfo | null; source?: string | null }) =>
      canManage(r) || !r.source || r.source === "local";
    const ids = collectSaveTargetOptions([own(true), sales()], asLocalAdmin).map((o) => o.id);
    expect(ids).toEqual([2, -11, -13]);
  });

  it("leaves out a hub catalog this Flowfile cannot use", () => {
    const onItsDisk = hubCatalog(-40, "legacy", [[-41, "old", access("manage")]], {
      unavailable_reason: "The hub keeps the tables of legacy on its own disk",
    });
    expect(collectSaveTargetOptions([onItsDisk], canManage)).toEqual([]);
  });

  it("is the local list when no hub is connected", () => {
    expect(collectSaveTargetOptions(tree(), canManage).map((o) => o.id)).toEqual(
      collectWritableSchemaOptions(tree(), canManage).map((o) => o.id),
    );
  });
});

describe("groupSaveTargets", () => {
  it("heads the hub's schemas with the hub's name and this machine's with its place", () => {
    const groups = groupSaveTargets(collectSaveTargetOptions([own(true), sales()], canManage));
    expect(groups.map((g) => [g.label, g.options.map((o) => o.id)])).toEqual([
      ["On this server", [2]],
      ["Acme Data (hub)", [-11, -13]],
    ]);
  });

  it("marks the hub's schemas as the hub's where they are the only ones", () => {
    const groups = groupSaveTargets(collectSaveTargetOptions([own(false), sales()], canManage));
    expect(groups.map((g) => g.label)).toEqual(["Acme Data (hub)"]);
  });

  it("uses no heading without a hub", () => {
    const groups = groupSaveTargets(collectSaveTargetOptions(tree(), canManage));
    expect(groups.map((g) => g.label)).toEqual([null]);
  });
});
