import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import type { ExternalCatalogInfo, NamespaceTree } from "../../types";
import * as externalCatalog from "./externalCatalog";
import {
  answerStillShown,
  catalogAfterSave,
  changedLines,
  emptyExternalForm,
  externalFormError,
  externalPayload,
  formFromCatalog,
  goneNote,
  isKeptElsewhere,
  lastCommitLine,
  localStamp,
  needsSignInAgain,
  notShownNote,
  OUTSIDE_CATALOG_DOCS_URL,
  OUTSIDE_CATALOG_LIMITS,
  outsideCatalogExplainer,
  sameInstant,
  outsideTableTarget,
  utcTime,
  readSnippet,
  snapshotsLine,
  signInLabel,
  SIGN_IN_AGAIN,
} from "./externalCatalog";

const stored: ExternalCatalogInfo = {
  kind: "iceberg_rest",
  uri: "http://127.0.0.1:18191/api/catalog",
  warehouse: "signed",
  sign_in: "client_credentials",
  client_id: "flowfile",
  scope: "PRINCIPAL_ROLE:ALL",
  token_endpoint: null,
  synced_at: "2026-11-22T10:00:00",
  sync_error: null,
  not_shown: 0,
};

const filled = () => ({
  ...emptyExternalForm(),
  name: "polaris",
  uri: stored.uri,
  warehouse: "signed",
  client_id: "flowfile",
  client_secret: "s3cr3t",
  scope: "PRINCIPAL_ROLE:ALL",
});

describe("externalFormError", () => {
  it("passes a complete client id and secret", () => {
    expect(externalFormError(filled())).toBeNull();
  });

  it.each([
    [{ name: "" }, "Give the catalog a name."],
    [{ name: "a.b" }, "A catalog's name holds no dot."],
    [{ uri: "ftp://x" }, "The address starts with http:// or https://."],
    [{ client_id: "" }, "A client id and secret sign-in needs the client id."],
    [{ client_id: "realm:id" }, "A client id cannot hold ':'."],
    [{ token_endpoint: "token" }, "The token endpoint starts with http:// or https://."],
    [{ client_secret: "" }, "Enter the client secret."],
    [{ sign_in: "token" as const }, "Enter the token."],
  ])("refuses %o with core's sentence", (change, said) => {
    expect(externalFormError({ ...filled(), ...change })).toBe(said);
  });

  it("keeps the stored secret on an edit unless the target moves", () => {
    const edit = { ...formFromCatalog(catalogNode()), scope: "PRINCIPAL_ROLE:other" };
    expect(externalFormError(edit, stored)).toBeNull();
    expect(externalFormError({ ...edit, uri: "https://elsewhere/api/catalog" }, stored)).toBe(
      SIGN_IN_AGAIN,
    );
    expect(needsSignInAgain({ ...edit, sign_in: "token" }, stored)).toBe(true);
    expect(needsSignInAgain({ ...edit, sign_in: "none" }, stored)).toBe(false);
  });
});

describe("the catalog's panel after Edit then Save", () => {
  const moved: ExternalCatalogInfo = {
    ...stored,
    uri: "http://127.0.0.1:18181/catalog",
    warehouse: "open",
    sign_in: "none",
    client_id: null,
    scope: null,
  };
  const saved = catalogNode({ description: "moved", external: moved, children: [] });

  it("holds what was saved, even when the reloaded tree still holds the old row", () => {
    const shown = catalogNode({ children: [catalogNode({ id: 8, level: 1, parent_id: 7 })] });
    const after = catalogAfterSave(shown, shown, saved);
    expect(after.external).toEqual(moved);
    expect(after.description).toBe("moved");
    expect(after.children.map((s) => s.id)).toEqual([8]);
    expect(catalogAfterSave(null, shown, saved).external).toEqual(moved);
  });

  it("opens the next edit on what was saved, so a second save without changes changes nothing", () => {
    const shown = catalogNode();
    const body = externalPayload(formFromCatalog(catalogAfterSave(shown, shown, saved)), true);
    expect(body).toEqual({
      uri: moved.uri,
      warehouse: "open",
      sign_in: "none",
      description: "moved",
      client_id: null,
      scope: null,
      token_endpoint: null,
    });
  });
});

describe("an answer the panel asked for", () => {
  it("lands only while the panel still shows the catalog it was asked for", () => {
    expect(answerStillShown(7, 7, 7)).toBe(true);
    // The person picked a table meanwhile, which clears the catalog's selection.
    expect(answerStillShown(7, null, 7)).toBe(false);
    // The panel now shows another catalog.
    expect(answerStillShown(7, 9, 9)).toBe(false);
    expect(answerStillShown(7, 7, 9)).toBe(false);
  });

  it("is what the panel's sync, Test and save check before they show an answer", () => {
    const panel = readFileSync(new URL("./NamespaceDetailPanel.vue", import.meta.url), "utf8");
    const body = (name: string) => {
      const from = panel.slice(panel.indexOf(`function ${name}(`));
      return from.slice(0, from.indexOf("\n}\n"));
    };
    expect(panel).toMatch(/const stillShown = \(id: number\) =>\s+answerStillShown\(/);
    for (const name of ["refresh", "testStored", "onExternalSaved"]) {
      expect(body(name)).toContain("stillShown(");
    }
    expect(panel).toContain('@saved="onExternalSaved"');
  });
});

describe("externalPayload", () => {
  it("sends the chosen sign-in's fields only", () => {
    const body = externalPayload({ ...filled(), token: "left-over" });
    expect(body).toEqual({
      name: "polaris",
      description: null,
      uri: stored.uri,
      warehouse: "signed",
      sign_in: "client_credentials",
      client_id: "flowfile",
      scope: "PRINCIPAL_ROLE:ALL",
      token_endpoint: null,
      client_secret: "s3cr3t",
    });
  });

  it("leaves the secret key out on an edit where it was left empty", () => {
    const body = externalPayload({ ...formFromCatalog(catalogNode()) }, true);
    expect("client_secret" in body).toBe(false);
    expect("name" in body).toBe(false);
  });

  it("sends no sign-in fields for an open catalog", () => {
    const body = externalPayload({ ...filled(), sign_in: "none" });
    expect(Object.keys(body).sort()).toEqual([
      "description",
      "name",
      "sign_in",
      "uri",
      "warehouse",
    ]);
  });
});

function catalogNode(overrides: Partial<NamespaceTree> = {}): NamespaceTree {
  return {
    id: 7,
    name: "polaris",
    parent_id: null,
    level: 0,
    description: null,
    owner_id: 1,
    created_at: "",
    updated_at: "",
    kept_elsewhere: true,
    external: stored,
    children: [],
    flows: [],
    artifacts: [],
    tables: [],
    visualizations: [],
    notebooks: [],
    ...overrides,
  };
}

describe("the words the screen shows", () => {
  it("names each kind of sign-in", () => {
    expect([signInLabel("none"), signInLabel("token"), signInLabel("client_credentials")]).toEqual([
      "No sign-in",
      "A token",
      "A client id and secret",
    ]);
  });

  it("counts the namespaces it does not show", () => {
    expect(notShownNote(0)).toBeNull();
    expect(notShownNote(1)).toBe(
      "1 namespace of this catalog is not shown: Flowfile shows one level of schemas, and names without a dot.",
    );
    expect(notShownNote(2)).toMatch(/^2 namespaces of this catalog are not shown/);
  });

  it("says why a schema is gone", () => {
    const gone = catalogNode({
      level: 1,
      gone_since: "2026-11-22T10:00:00",
      unavailable_reason: "No longer in polaris's own catalog since 2026-11-22 10:00 UTC.",
    });
    expect(goneNote(gone)).toBe("No longer in polaris's own catalog since 2026-11-22 10:00 UTC.");
    expect(goneNote(catalogNode())).toBeNull();
  });

  it("tells a catalog kept elsewhere from one of Flowfile's own", () => {
    expect(isKeptElsewhere(catalogNode())).toBe(true);
    expect(isKeptElsewhere(catalogNode({ kept_elsewhere: false }))).toBe(false);
    expect(isKeptElsewhere(null)).toBe(false);
  });

  it("writes the two Python lines that import flowfile and read the table, by name", () => {
    expect(readSnippet("polaris", "sales", "orders").split("\n")).toEqual([
      "import flowfile as ff",
      'frame = ff.read_catalog_table("orders", namespace_full_name="polaris.sales")',
    ]);
  });

  it("lists nothing such a table does not have, neither as a constant nor on its panel", () => {
    expect(Object.keys(externalCatalog)).not.toContain("OUTSIDE_TABLE_NOTES");
    const panel = readFileSync(new URL("./ExternalTableDetailPanel.vue", import.meta.url), "utf8");
    expect(panel).not.toMatch(/external-notes|does not have here/);
  });

  it("tells one instant from another whether core sent a zone or not, so the time card shows it once", () => {
    expect(sameInstant("2026-11-24T14:03:22.120", "2026-11-24T14:03:22.120Z")).toBe(true);
    expect(sameInstant("2026-11-24T14:03:22.120", "2026-11-24T14:03:23.000")).toBe(true);
    expect(sameInstant("2026-11-24T14:03:22.120", "2026-11-24T14:03:25.000")).toBe(false);
    expect(sameInstant(null, "2026-11-24T14:03:22")).toBe(false);
    expect(sameInstant(undefined, undefined)).toBe(false);
  });

  it("shows a commit and its metadata stamped milliseconds apart as one line", () => {
    const updated = "2026-11-24T15:36:56.757";
    const committed = "2026-11-24T15:36:56.735";
    expect(sameInstant(updated, committed)).toBe(true);
    const lines = changedLines(updated, committed, 1, utcTime(updated));
    expect(lines.changed).toMatch(/^\d\d:\d\d:\d\d$/);
    expect(lines.commit).toBeNull();
    expect(changedLines(null, committed, 1).changed).toBe("--");
  });

  it("shows the last commit apart from a later metadata change, in the same format", () => {
    const updated = "2026-11-24T15:40:10.000";
    const committed = "2026-11-24T15:36:56.735";
    const sameDay = utcTime(updated);
    const today = changedLines(updated, committed, 2, sameDay);
    expect(today.changed).toMatch(/^\d\d:\d\d:\d\d$/);
    expect(today.commit).toBe(`Last commit ${localStamp(committed, sameDay)}`);
    expect(today.commit).toMatch(/^Last commit \d\d:\d\d:\d\d$/);

    const nextWeek = new Date(sameDay.getTime() + 7 * 86_400_000);
    const later = changedLines(updated, committed, 2, nextWeek);
    expect(later.changed).toMatch(/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/);
    expect(later.commit).toMatch(/^Last commit \d{4}-\d\d-\d\d \d\d:\d\d:\d\d$/);
    expect(later.commit).toBe(`Last commit ${localStamp(committed, nextWeek)}`);
  });

  it("shows nothing for a time that does not parse", () => {
    expect(localStamp("not a time")).toBe("");
    expect(lastCommitLine("not a time", 3)).toBeNull();
  });

  it("says when the table was last committed to, in local time, and never how", () => {
    const at = new Date(2026, 10, 24, 14, 3, 22);
    const sameDay = new Date(2026, 10, 24, 18, 0, 0);
    expect(lastCommitLine(at.toISOString(), 3, sameDay)).toBe("Last commit 14:03:22");
    expect(lastCommitLine(at.toISOString().replace("Z", ""), 3, sameDay)).toBe(
      "Last commit 14:03:22",
    );
    expect(lastCommitLine(at.toISOString(), 3, new Date(2026, 10, 25, 9, 0, 0))).toBe(
      "Last commit 2026-11-24 14:03:22",
    );
    expect(lastCommitLine(at.toISOString(), undefined, sameDay)).toBe("Last commit 14:03:22");
    expect(lastCommitLine(at.toISOString(), 0, sameDay)).toBeNull();
    expect(lastCommitLine(null, 3)).toBeNull();
    expect(lastCommitLine(undefined, undefined)).toBeNull();
  });

  it("counts the snapshots the table keeps, never its rows", () => {
    expect(snapshotsLine(1)).toBe("1 snapshot kept");
    expect(snapshotsLine(4)).toBe("4 snapshots kept");
    expect(snapshotsLine(0)).toBe("0 snapshots kept");
    expect(snapshotsLine(undefined)).toBeNull();
  });
});

describe("a flow's table kept elsewhere", () => {
  const tree = [
    catalogNode({
      children: [catalogNode({ id: 8, name: "sales", level: 1, parent_id: 7 })],
    }),
    catalogNode({
      id: 1,
      name: "General",
      kept_elsewhere: false,
      external: null,
      children: [
        catalogNode({ id: 2, name: "default", level: 1, parent_id: 1, kept_elsewhere: false }),
      ],
    }),
  ];

  it("is told by its schema being kept elsewhere, and opens by its catalog, schema and name", () => {
    expect(outsideTableTarget({ id: 30, name: "orders", namespace_id: 8 }, tree)).toEqual({
      catalogId: 7,
      schemaId: 8,
      name: "orders",
    });
  });

  it("is nothing for a table of Flowfile's own or one with no schema", () => {
    expect(outsideTableTarget({ id: 4, name: "orders", namespace_id: 2 }, tree)).toBeNull();
    expect(outsideTableTarget({ id: 5, name: "x", namespace_id: null }, tree)).toBeNull();
    expect(outsideTableTarget({ id: 6, name: "x", namespace_id: 99 }, tree)).toBeNull();
  });
});

describe("the badge's pop-over", () => {
  const sectionText = (sections: ReturnType<typeof outsideCatalogExplainer>, heading: string) =>
    sections.find((s) => s.heading === heading)?.text ?? "";

  it("names the catalog's address and warehouse", () => {
    const sections = outsideCatalogExplainer(stored);
    expect(sections.map((s) => s.heading)).toEqual([
      "Kept by another catalog",
      "In a flow",
      "Limits",
    ]);
    expect(sectionText(sections, "Kept by another catalog")).toBe(
      "These tables live in an Iceberg REST catalog at http://127.0.0.1:18191/api/catalog " +
        "(warehouse signed). Flowfile lists them from that catalog and reads and writes them with temporary " +
        "keys it hands out. Nothing is copied here and no storage key is stored.",
    );
    expect(sectionText(sections, "In a flow")).toBe(
      "Read from Catalog reads a table as it is now. Write to Catalog appends to a table or overwrites it, " +
        "and creates it when it is missing.",
    );
  });

  it("leaves the warehouse out when there is none", () => {
    const text = sectionText(
      outsideCatalogExplainer({ uri: "http://lake:8181/catalog", warehouse: null }),
      "Kept by another catalog",
    );
    expect(text).toMatch(
      /^These tables live in an Iceberg REST catalog at http:\/\/lake:8181\/catalog\. /,
    );
    expect(text).not.toContain("warehouse");
  });

  it("lists the four limits", () => {
    const limits = outsideCatalogExplainer(stored).find((s) => s.heading === "Limits")?.items;
    expect(limits).toBe(OUTSIDE_CATALOG_LIMITS);
    expect(limits).toHaveLength(4);
    expect(limits?.[0]).toMatch(
      /^Writing: Append and Overwrite only\. No Upsert, Update, Delete, SCD2/,
    );
    expect(limits?.[1]).toBe("Reading: no change feed, no versions, no SQL mode.");
    expect(limits?.[2]).toBe(
      "No optimise, vacuum, table trigger, preview outside a flow or charts.",
    );
    expect(limits?.[3]).toMatch(
      /^Iceberg format versions 2 and 3 are read, version 2 is written\./,
    );
  });

  it("links to the docs' section on a catalog from elsewhere", () => {
    expect(OUTSIDE_CATALOG_DOCS_URL).toMatch(
      /\/users\/visual-editor\/catalog\/index\.html#a-catalog-from-elsewhere$/,
    );
  });
});
