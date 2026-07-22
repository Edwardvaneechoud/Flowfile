import { describe, expect, it, vi } from "vitest";
import type { NamespaceTree } from "../types/catalog.types";
import type { AccessInfo } from "../types/sharing.types";

// Mock the composable's Pinia deps so importing it doesn't pull the axios/auth
// chain (auth.service touches localStorage at load) into the node test env.
vi.mock("../stores/catalog-store", () => ({ useCatalogStore: () => ({ tree: [] }) }));
vi.mock("./useResourceSharing", () => ({
  useResourceSharing: () => ({ canManage: () => true }),
}));

import { collectWritableSchemaOptions, pruneNonWritableSchemas } from "./useWritableNamespaces";

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
