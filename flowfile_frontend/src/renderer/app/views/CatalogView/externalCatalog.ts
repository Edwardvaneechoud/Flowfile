// A catalog kept elsewhere (an Iceberg REST catalog): the add and edit form's rules, in core's own words, and the
// sentences the catalog screen shows about it. Pure, so the form and the panels share one source.
import type {
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

/** The two Python lines that read a table of the catalog and write a filtered copy of it. */
export function readSnippet(catalog: string, schema: string, table: string): string {
  const name = JSON.stringify(`${schema}.${table}`);
  const copy = JSON.stringify(`${schema}.${table}_copy`);
  const named = JSON.stringify(catalog);
  return [
    `frame = ff.read_from_cloud_storage(${name}, file_format="iceberg", catalog=${named})`,
    `ff.write_to_cloud_storage(frame, ${copy}, file_format="iceberg", catalog=${named})`,
  ].join("\n");
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
