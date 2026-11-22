// Framework-free rules for the connection form's Snowflake section, unit-tested without mounting it.

import type {
  DatabaseConnectionTestResult,
  FullDatabaseConnection,
} from "./databaseConnectionTypes";

export const SNOWFLAKE = "snowflake";

export type SnowflakeAuthMethod = "oauth" | "access_token" | "key_pair";

const HOST_SUFFIXES = [".snowflakecomputing.com", ".snowflakecomputing.cn"];
const DEFAULT_SUFFIX = HOST_SUFFIXES[0];
const SNOWSIGHT_SUFFIXES = new Map([
  ["app.snowflake.com", HOST_SUFFIXES[0]],
  ["app.snowflake.cn", HOST_SUFFIXES[1]],
]);
const HOST_LABELS = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/;

export const MAX_KEY_FILE_BYTES = 64 * 1024;
export const SIGN_IN_POLL_INTERVAL_MS = 1500;
export const SIGN_IN_TIMEOUT_MS = 5 * 60 * 1000;

export type AccountPreview = { host: string } | { error: string };

/**
 * The hostname and path of a URL as Python's `urllib.parse.urlsplit` reads them, which is what the
 * server parses. The browser's `URL` differs (it validates the port, decodes and IDNA-encodes the
 * host), so it would accept or refuse other inputs than the server does. null where `urlsplit`
 * raises (unbalanced or bracketed hosts, which never name a Snowflake account).
 */
function splitUrl(value: string): { host: string; path: string } | null {
  let rest = (value.includes("://") ? value : `https://${value}`).replace(/[\t\r\n]/g, "");
  const colon = rest.indexOf(":");
  if (colon > 0 && /^[A-Za-z][A-Za-z0-9+.-]*$/.test(rest.slice(0, colon))) {
    rest = rest.slice(colon + 1);
  }
  let netloc = "";
  if (rest.startsWith("//")) {
    const end = rest.slice(2).search(/[/?#]/);
    netloc = end < 0 ? rest.slice(2) : rest.slice(2, end + 2);
    rest = end < 0 ? "" : rest.slice(end + 2);
  }
  if (netloc.includes("[") || netloc.includes("]")) return null;
  const path = rest.split("#")[0].split("?")[0];
  const host = netloc
    .slice(netloc.lastIndexOf("@") + 1)
    .split(":")[0]
    .toLowerCase();
  return { host, path };
}

const notAnAccount = (text: string): AccountPreview => ({
  error:
    `'${text}' is not a Snowflake account. Use the form myorg-myaccount, ` +
    "or paste the link of your account.",
});

function stripSuffix(host: string): string {
  const lowered = host.toLowerCase();
  const suffix = HOST_SUFFIXES.find((s) => lowered.endsWith(s));
  return suffix ? lowered.slice(0, -suffix.length) : lowered;
}

/**
 * The hostname the server will derive from what the user typed as their account.
 *
 * Mirrors `shared/snowflake_auth.py::normalize_account` for the live hint under the
 * Account field; the server stays authoritative on save.
 */
export function previewAccount(text: string): AccountPreview {
  const original = (text || "").trim();
  if (!original) {
    return { error: "Enter your Snowflake account, for example myorg-myaccount." };
  }
  let value = original;
  let suffix = DEFAULT_SUFFIX;
  if (value.includes("@")) return notAnAccount(original);
  if (value.includes("/") || value.includes(":")) {
    const parts = splitUrl(value);
    if (!parts) return notAnAccount(original);
    const { host, path } = parts;
    const snowsightSuffix = SNOWSIGHT_SUFFIXES.get(host);
    if (snowsightSuffix) {
      const segments = path.split("/").filter((segment) => segment);
      if (segments.length < 2) {
        return {
          error:
            "That Snowflake link does not name an account. Open your account in Snowflake and copy the link.",
        };
      }
      const [first, second] = segments;
      // An organization name holds no hyphen or dot, so either marks the older region/locator link.
      value =
        first.includes("-") || first.includes(".") ? `${second}.${first}` : `${first}-${second}`;
      suffix = snowsightSuffix;
    } else if (HOST_SUFFIXES.some((s) => host.endsWith(s)) || (host && !host.includes("."))) {
      value = host;
    } else {
      return notAnAccount(original);
    }
  }
  let host = value.toLowerCase().replace(/\.+$/, "").replace(/_/g, "-");
  if (!HOST_SUFFIXES.some((s) => host.endsWith(s))) {
    host += suffix;
  }
  if (!stripSuffix(host) || !HOST_LABELS.test(host)) return notAnAccount(original);
  return { host };
}

/** The account part of a Snowflake hostname, for the connection list. */
export function accountLabel(host?: string | null): string {
  return host ? stripSuffix(host) : "";
}

const METHOD_LABELS: Record<SnowflakeAuthMethod, string> = {
  oauth: "Browser sign-in",
  access_token: "Access token",
  key_pair: "Key-pair",
};

const METHOD_DESCRIPTIONS: Record<SnowflakeAuthMethod, string> = {
  oauth: "Sign in through your browser. Your Snowflake password never passes through Flowfile.",
  access_token: "A programmatic access token generated in Snowflake for your user.",
  key_pair: "A private key whose public key is registered on your Snowflake user.",
};

export function methodLabel(method?: string | null): string {
  return METHOD_LABELS[method as SnowflakeAuthMethod] ?? method ?? "";
}

export function methodDescription(method?: string | null): string {
  return METHOD_DESCRIPTIONS[method as SnowflakeAuthMethod] ?? "";
}

export interface SnowflakeFormContext {
  isEditing: boolean;
  storedAuthMethod?: string | null;
  storedHost?: string | null;
  storedRole?: string | null;
}

const blank = (value?: string | null) => !(value ?? "").trim();

function methodUnchanged(connection: FullDatabaseConnection, ctx: SnowflakeFormContext): boolean {
  return ctx.isEditing && !!ctx.storedAuthMethod && ctx.storedAuthMethod === connection.authMethod;
}

/** Whether an edited browser-signed-in connection can keep its stored sign-in. */
export function storedSignInStillValid(
  connection: FullDatabaseConnection,
  ctx: SnowflakeFormContext,
): boolean {
  if (connection.authMethod !== "oauth" || !methodUnchanged(connection, ctx)) return false;
  const preview = previewAccount(connection.host ?? "");
  if ("error" in preview) return false;
  if (ctx.storedHost && preview.host !== ctx.storedHost.toLowerCase()) return false;
  // A Snowflake OAuth token is bound to the role it was granted for.
  return (connection.role ?? "").trim() === (ctx.storedRole ?? "").trim();
}

function hasValidAccount(connection: FullDatabaseConnection): boolean {
  return "host" in previewAccount(connection.host ?? "");
}

/** Whether the credentials entered are enough to ask Snowflake who the user is (Connect). */
export function canDiscover(
  connection: FullDatabaseConnection,
  ctx: SnowflakeFormContext,
): boolean {
  if (connection.authMethod !== "access_token" && connection.authMethod !== "key_pair")
    return false;
  if (!hasValidAccount(connection) || blank(connection.username)) return false;
  return !blank(connection.password) || methodUnchanged(connection, ctx);
}

/**
 * Whether the form can be saved: a name, a valid account and what the chosen method needs (a username
 * and a secret, a secret kept from the stored connection, or a sign-in).
 */
export function isSnowflakeFormValid(
  connection: FullDatabaseConnection,
  ctx: SnowflakeFormContext,
): boolean {
  if (blank(connection.connectionName) || !hasValidAccount(connection)) return false;
  switch (connection.authMethod) {
    case "access_token":
    case "key_pair":
      return (
        !blank(connection.username) &&
        (!blank(connection.password) || methodUnchanged(connection, ctx))
      );
    case "oauth":
      return !!connection.oauthFlowId || storedSignInStillValid(connection, ctx);
    default:
      return false;
  }
}

export const signInDeadline = (now: number): number => now + SIGN_IN_TIMEOUT_MS;

/** Delay before the next sign-in status poll, or null once the deadline has passed. */
export function nextPollDelayMs(now: number, deadline: number): number | null {
  const remaining = deadline - now;
  if (remaining <= 0) return null;
  return Math.min(SIGN_IN_POLL_INTERVAL_MS, remaining);
}

export type SignInPhase = "idle" | "starting" | "waiting" | "ok" | "error" | "timeout";

export function identityText(prefix: string, username?: string | null, role?: string | null) {
  const who = username ? `${prefix} ${username}` : prefix;
  return role ? `${who} (role ${role})` : who;
}

/** The line under the sign-in button for a browser sign-in phase. */
export function signInStatusText(
  phase: SignInPhase,
  info: { username?: string | null; role?: string | null; message?: string | null } = {},
): string {
  switch (phase) {
    case "starting":
      return "Opening Snowflake…";
    case "waiting":
      return "Waiting for you to sign in in your browser…";
    case "ok":
      return identityText("Signed in as", info.username, info.role);
    case "error":
      return info.message || "Signing in to Snowflake did not work. Try again.";
    case "timeout":
      return "The sign-in was not completed within 5 minutes. Try again.";
    default:
      return "";
  }
}

const trimmedOrNull = (value?: string | null): string | null => (value ?? "").trim() || null;

/** The Snowflake-specific part of the connection sent on save. */
export function buildPayloadFields(
  connection: FullDatabaseConnection,
): Partial<FullDatabaseConnection> {
  const method = connection.authMethod;
  const secret = connection.password ?? "";
  return {
    host: (connection.host ?? "").trim(),
    port: undefined,
    sslEnabled: false,
    url: "",
    username: (connection.username ?? "").trim(),
    database: (connection.database ?? "").trim(),
    authMethod: method ?? null,
    role: trimmedOrNull(connection.role),
    warehouse: trimmedOrNull(connection.warehouse),
    password: method === "oauth" ? "" : method === "key_pair" ? secret : secret.trim(),
    privateKeyPassphrase: method === "key_pair" ? connection.privateKeyPassphrase || null : null,
    oauthFlowId: method === "oauth" ? connection.oauthFlowId || null : null,
  };
}

/** The fields reset when the database type changes to or from Snowflake. */
export function snowflakeResetFields(): Partial<FullDatabaseConnection> {
  return {
    password: "",
    authMethod: null,
    role: null,
    warehouse: null,
    privateKeyPassphrase: null,
    oauthFlowId: null,
  };
}

/**
 * What a sign-in or Connect result fills in on the form: the session's warehouse when none is
 * chosen. The database is never filled in: it is only the default a node starts on, and a
 * connection reaches every database the role may use.
 */
export function sessionDefaults(
  connection: Pick<FullDatabaseConnection, "warehouse">,
  session: { warehouse: string | null },
): Partial<FullDatabaseConnection> {
  if (!connection.warehouse && session.warehouse) return { warehouse: session.warehouse };
  return {};
}

/** The database shown for a Snowflake connection in the list: its default, else "all databases". */
export function snowflakeDatabaseLabel(database?: string | null): string {
  return (database ?? "").trim() || "all databases";
}

/** Options for a warehouse/database select: the session's list plus the current value. */
export function selectOptions(list: string[], current?: string | null): string[] {
  const options = [...list];
  if (current && !options.includes(current)) options.unshift(current);
  return options;
}

/** The toast after a successful connection test. */
export function testSuccessText(result: DatabaseConnectionTestResult): string {
  const details = result.details;
  if (details && (details.user || details.role || details.warehouse)) {
    const parts = [
      details.user ? `Connected as ${details.user}` : "Connected",
      details.role ? `role ${details.role}` : null,
      details.warehouse ? `warehouse ${details.warehouse}` : null,
    ].filter(Boolean);
    return parts.join(", ");
  }
  return result.message || "Connection succeeded";
}
