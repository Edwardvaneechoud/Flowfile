// Framework-free helpers for the Snowflake section of the database connection form,
// kept apart so the rules can be unit-tested without mounting the component.

import type {
  DatabaseConnectionTestResult,
  FullDatabaseConnection,
} from "./databaseConnectionTypes";

export const SNOWFLAKE = "snowflake";

export type SnowflakeAuthMethod = "oauth" | "access_token" | "key_pair";

const HOST_SUFFIXES = [".snowflakecomputing.com", ".snowflakecomputing.cn"];
const DEFAULT_SUFFIX = HOST_SUFFIXES[0];
const SNOWSIGHT_HOSTS = new Set(["app.snowflake.com", "app.snowflake.cn"]);
const HOST_LABELS = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/;

export const MAX_KEY_FILE_BYTES = 64 * 1024;
export const SIGN_IN_POLL_INTERVAL_MS = 1500;
export const SIGN_IN_TIMEOUT_MS = 5 * 60 * 1000;

export type AccountPreview = { host: string } | { error: string };

function hostnameOf(value: string): string {
  try {
    return new URL(value.includes("://") ? value : `https://${value}`).hostname.toLowerCase();
  } catch {
    return "";
  }
}

function pathSegmentsOf(value: string): string[] {
  try {
    const url = new URL(value.includes("://") ? value : `https://${value}`);
    return url.pathname.split("/").filter((segment) => segment);
  } catch {
    return [];
  }
}

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
  let value = (text || "").trim();
  if (!value) {
    return { error: "Enter your Snowflake account, for example myorg-myaccount." };
  }
  if (value.includes("://") || value.includes("/")) {
    const host = hostnameOf(value);
    if (SNOWSIGHT_HOSTS.has(host)) {
      const segments = pathSegmentsOf(value);
      if (segments.length < 2) {
        return {
          error:
            "That Snowflake link does not name an account. Open your account in Snowflake and copy the link.",
        };
      }
      const [first, second] = segments;
      // An organization name holds no hyphen, so a hyphen marks the older region/locator link.
      value = first.includes("-") ? `${second}.${first}` : `${first}-${second}`;
    } else {
      value = host;
    }
  }
  let host = value.toLowerCase().replace(/\.+$/, "").replace(/_/g, "-");
  if (!HOST_SUFFIXES.some((suffix) => host.endsWith(suffix))) {
    host += DEFAULT_SUFFIX;
  }
  if (!stripSuffix(host) || !HOST_LABELS.test(host)) {
    return {
      error:
        `'${text.trim()}' is not a Snowflake account. Use the form myorg-myaccount, ` +
        "or paste the link of your Snowflake account.",
    };
  }
  return { host };
}

/** The account part of a Snowflake hostname, for the connection list. */
export function accountLabel(host?: string | null): string {
  return host ? stripSuffix(host) : "";
}

const METHOD_LABELS: Record<SnowflakeAuthMethod, string> = {
  oauth: "Sign in with Snowflake",
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
