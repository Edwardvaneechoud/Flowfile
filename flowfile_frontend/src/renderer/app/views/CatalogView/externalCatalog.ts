// A catalog kept elsewhere (an Iceberg REST catalog): the add and edit form's rules, in core's own words, and the
// sentences the catalog screen shows about it. Pure, so the form and the panels share one source.
import { docsUrl } from "../../lib/docsLinks";
import type {
  CatalogTableSummary,
  ExternalCatalogForm,
  ExternalCatalogInfo,
  ExternalCatalogPayload,
  ExternalSignIn,
  NamespaceTree,
} from "../../types";

export const POLARIS_SCOPE = "PRINCIPAL_ROLE:ALL";

export const NO_KEY_TYPED =
  "Flowfile asks this catalog for a table's storage keys each time a flow runs; none is typed or stored here.";

export const SIGN_IN_AGAIN = "Enter the sign-in again to change where it is sent.";

const HTTP = /^https?:\/\//;

export function emptyExternalForm(): ExternalCatalogForm {
  return {
    name: "",
    description: "",
    uri: "",
    warehouse: "",
    sign_in: "client_credentials",
    token: "",
    client_id: "",
    client_secret: "",
    scope: "",
    token_endpoint: "",
  };
}

/** The form a stored catalog opens with: its settings, the secret fields empty (they mean "keep the stored one"). */
export function formFromCatalog(node: NamespaceTree): ExternalCatalogForm {
  const external = node.external as ExternalCatalogInfo;
  return {
    ...emptyExternalForm(),
    name: node.name,
    description: node.description ?? "",
    uri: external.uri,
    warehouse: external.warehouse ?? "",
    sign_in: external.sign_in,
    client_id: external.client_id ?? "",
    scope: external.scope ?? "",
    token_endpoint: external.token_endpoint ?? "",
  };
}

const blank = (value: string) => value.trim() === "";

/** Whether a change of the form would send the stored sign-in to another place or as another kind. */
export function needsSignInAgain(
  form: ExternalCatalogForm,
  original: ExternalCatalogInfo | null,
): boolean {
  if (!original || form.sign_in === "none") return false;
  return (
    form.uri.trim() !== original.uri ||
    (form.token_endpoint.trim() || null) !== (original.token_endpoint || null) ||
    form.sign_in !== original.sign_in
  );
}

/**
 * The first rule the form breaks, in the words core answers with; null when it may be sent. On an edit a blank
 * secret keeps the stored one, unless the address, the token endpoint or the kind of sign-in changes.
 */
export function externalFormError(
  form: ExternalCatalogForm,
  editing: ExternalCatalogInfo | null = null,
): string | null {
  if (!editing && blank(form.name)) return "Give the catalog a name.";
  if (!editing && form.name.includes(".")) return "A catalog's name holds no dot.";
  if (!HTTP.test(form.uri.trim())) return "The address starts with http:// or https://.";
  if (form.sign_in === "client_credentials") {
    if (blank(form.client_id)) return "A client id and secret sign-in needs the client id.";
    if (form.client_id.includes(":")) return "A client id cannot hold ':'.";
    if (!blank(form.token_endpoint) && !HTTP.test(form.token_endpoint.trim()))
      return "The token endpoint starts with http:// or https://.";
  }
  const secretBlank = form.sign_in === "token" ? blank(form.token) : blank(form.client_secret);
  if (form.sign_in !== "none" && secretBlank) {
    if (!editing) return form.sign_in === "token" ? "Enter the token." : "Enter the client secret.";
    if (needsSignInAgain(form, editing)) return SIGN_IN_AGAIN;
  }
  return null;
}

const orNull = (value: string) => (blank(value) ? null : value.trim());

/** The body the routes take: only the fields of the chosen sign-in, and no secret key when it was left empty. */
export function externalPayload(
  form: ExternalCatalogForm,
  editing = false,
): ExternalCatalogPayload {
  const body: ExternalCatalogPayload = {
    uri: form.uri.trim(),
    warehouse: orNull(form.warehouse),
    sign_in: form.sign_in,
  };
  if (!editing) body.name = form.name.trim();
  if (!editing || !blank(form.description)) body.description = orNull(form.description);
  if (form.sign_in === "token" && !blank(form.token)) body.token = form.token;
  if (form.sign_in === "client_credentials") {
    body.client_id = form.client_id.trim();
    body.scope = orNull(form.scope);
    body.token_endpoint = orNull(form.token_endpoint);
    if (!blank(form.client_secret)) body.client_secret = form.client_secret;
  }
  if (editing && form.sign_in !== "client_credentials") {
    body.client_id = null;
    body.scope = null;
    body.token_endpoint = null;
  }
  return body;
}

const SIGN_IN_LABELS: Record<ExternalSignIn, string> = {
  none: "No sign-in",
  token: "A token",
  client_credentials: "A client id and secret",
};

export function signInLabel(kind: ExternalSignIn): string {
  return SIGN_IN_LABELS[kind];
}

export function notShownNote(count: number): string | null {
  if (count <= 0) return null;
  const subject =
    count === 1 ? "1 namespace of this catalog is" : `${count} namespaces of this catalog are`;
  return `${subject} not shown: Flowfile shows one level of schemas, and names without a dot.`;
}

/** Why a schema shows only under "Show unavailable": the other catalog no longer lists it. */
export function goneNote(schema: NamespaceTree): string | null {
  return schema.gone_since
    ? (schema.unavailable_reason ?? "No longer in the other catalog.")
    : null;
}

export function isKeptElsewhere(node: { kept_elsewhere?: boolean } | null | undefined): boolean {
  return !!node?.kept_elsewhere;
}

/** The Python lines that import flowfile and read a table of the catalog, by name. */
export function readSnippet(catalog: string, schema: string, table: string): string {
  const where = JSON.stringify(`${catalog}.${schema}`);
  return [
    "import flowfile as ff",
    `frame = ff.read_catalog_table(${JSON.stringify(table)}, namespace_full_name=${where})`,
  ].join("\n");
}

/** A time core answers without a zone, read as UTC. */
export function utcTime(value: string): Date {
  return new Date(/(Z|[+-]\d\d:?\d\d)$/.test(value) ? value : `${value}Z`);
}

const twoDigits = (n: number) => String(n).padStart(2, "0");

/**
 * A time core answers as ``HH:MM:SS`` in local time, with ``YYYY-MM-DD`` before it when that is not today; an empty
 * string for a time that does not parse.
 */
export function localStamp(at: string, now: Date = new Date()): string {
  const time = utcTime(at);
  if (Number.isNaN(time.getTime())) return "";
  const clock = [time.getHours(), time.getMinutes(), time.getSeconds()].map(twoDigits).join(":");
  if (time.toDateString() === now.toDateString()) return clock;
  const day = [time.getFullYear(), time.getMonth() + 1, time.getDate()].map(twoDigits).join("-");
  return `${day} ${clock}`;
}

/**
 * ``Last commit HH:MM:SS`` in local time, with the date before it when that is not today; null when the table keeps
 * no snapshot. No operation is named: Flowfile's own overwrite commits a delete and then an append.
 */
export function lastCommitLine(
  at: string | null | undefined,
  snapshotCount: number | null | undefined,
  now: Date = new Date(),
): string | null {
  if (!at || snapshotCount === 0) return null;
  const stamp = localStamp(at, now);
  return stamp ? `Last commit ${stamp}` : null;
}

/** Two times this close are one moment: the catalog stamps a commit and its metadata a few milliseconds apart. */
export const SAME_INSTANT_MS = 2_000;

/** Whether two times core answers name the same moment (within two seconds); false when either is missing. */
export function sameInstant(a: string | null | undefined, b: string | null | undefined): boolean {
  return !!a && !!b && Math.abs(utcTime(a).getTime() - utcTime(b).getTime()) <= SAME_INSTANT_MS;
}

/**
 * The time card's lines: when the table last changed, and its last commit only when that is another moment
 * (a metadata-only change since), both in ``localStamp``'s format.
 */
export function changedLines(
  updatedAt: string | null | undefined,
  lastCommitAt: string | null | undefined,
  snapshotCount: number | null | undefined,
  now: Date = new Date(),
): { changed: string; commit: string | null } {
  return {
    changed: updatedAt ? localStamp(updatedAt, now) : "--",
    commit: sameInstant(updatedAt, lastCommitAt)
      ? null
      : lastCommitLine(lastCommitAt, snapshotCount, now),
  };
}

/** How many snapshots the table keeps; null when the description does not say. */
export function snapshotsLine(count: number | null | undefined): string | null {
  if (count == null) return null;
  return count === 1 ? "1 snapshot kept" : `${count} snapshots kept`;
}

/** Where a flow's table lives when its schema is kept by another catalog: what its panel opens on; else null. */
export function outsideTableTarget(
  table: CatalogTableSummary,
  tree: NamespaceTree[],
): { catalogId: number; schemaId: number; name: string } | null {
  if (table.namespace_id == null) return null;
  for (const catalog of tree) {
    if (!catalog.kept_elsewhere) continue;
    const schema = (catalog.children ?? []).find((child) => child.id === table.namespace_id);
    if (schema) return { catalogId: catalog.id, schemaId: schema.id, name: table.name };
  }
  return null;
}

/** The docs' section on a catalog from elsewhere, which the badge's pop-over links to. */
export const OUTSIDE_CATALOG_DOCS_URL = docsUrl(
  "users/visual-editor/catalog/index.html#a-catalog-from-elsewhere",
);

/** What a catalog kept elsewhere cannot do in Flowfile, as core refuses it and the user docs list it. */
export const OUTSIDE_CATALOG_LIMITS: readonly string[] = [
  "Writing: Append and Overwrite only. No Upsert, Update, Delete, SCD2, virtual tables, change tracking or partitioning.",
  "Reading: no change feed, no versions, no SQL mode.",
  "No optimise, vacuum, table trigger, preview outside a flow or charts.",
  "Iceberg format versions 2 and 3 are read, version 2 is written. Tables with equality deletes are not read.",
];

export interface ExplainerSection {
  heading: string;
  text?: string;
  items?: readonly string[];
}

/** The badge's pop-over: what a catalog kept elsewhere is, how a flow uses it, and its limits. */
export function outsideCatalogExplainer(
  external: Pick<ExternalCatalogInfo, "uri" | "warehouse">,
): ExplainerSection[] {
  const where = external.warehouse
    ? `${external.uri} (warehouse ${external.warehouse})`
    : external.uri;
  return [
    {
      heading: "Kept by another catalog",
      text:
        `These tables live in an Iceberg REST catalog at ${where}. Flowfile lists them from that catalog and reads ` +
        "and writes them with temporary keys it hands out. Nothing is copied here and no storage key is stored.",
    },
    {
      heading: "In a flow",
      text:
        "Read from Catalog reads a table as it is now. Write to Catalog appends to a table or overwrites it, and " +
        "creates it when it is missing.",
    },
    { heading: "Limits", items: OUTSIDE_CATALOG_LIMITS },
  ];
}

/** How long ago the schemas were last brought in line, as the panel says it. */
export function syncedLine(external: ExternalCatalogInfo, now: Date = new Date()): string {
  if (!external.synced_at) return "Not yet";
  const at = new Date(
    external.synced_at.endsWith("Z") ? external.synced_at : `${external.synced_at}Z`,
  );
  const seconds = Math.max(0, Math.round((now.getTime() - at.getTime()) / 1000));
  if (seconds < 60) return "Just now";
  if (seconds < 3600) return `${Math.round(seconds / 60)} minutes ago`;
  return at.toLocaleString();
}
