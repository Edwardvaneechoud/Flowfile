import { describe, expect, it } from "vitest";
import type { ExternalCatalogInfo, NamespaceTree } from "../../types";
import {
  emptyExternalForm,
  externalFormError,
  externalPayload,
  formFromCatalog,
  goneNote,
  isKeptElsewhere,
  needsSignInAgain,
  notShownNote,
  readSnippet,
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

  it("writes the two Python lines that name the catalog", () => {
    expect(readSnippet("polaris", "sales", "orders").split("\n")).toEqual([
      'frame = ff.read_from_cloud_storage("sales.orders", file_format="iceberg", catalog="polaris")',
      'ff.write_to_cloud_storage(frame, "sales.orders_copy", file_format="iceberg", catalog="polaris")',
    ]);
  });
});
