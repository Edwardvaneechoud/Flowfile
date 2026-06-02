// Framework-free helpers for GoogleAnalyticsSetupWizard.vue, kept separate so the
// step model and gating can be unit-tested without mounting the component.

import type { GoogleAnalyticsAuthMethod } from "./GoogleAnalyticsConnectionTypes";

export type GaSetupMethod = GoogleAnalyticsAuthMethod;

export type WizardStepKind = "method" | "confirm" | "input" | "action" | "verify";

export interface WizardStep {
  id: string;
  title: string;
  kind: WizardStepKind;
}

export interface ServiceAccountKeyValidation {
  valid: boolean;
  clientEmail: string | null;
  error: string | null;
}

const REQUIRED_SA_FIELDS = ["client_email", "private_key", "token_uri"] as const;

// Mirrors the server-side check in routes/ga_connections.py so the user gets
// instant feedback; the backend stays authoritative on save.
export function validateServiceAccountKey(raw: string): ServiceAccountKeyValidation {
  const text = raw.trim();
  if (!text) {
    return { valid: false, clientEmail: null, error: "Paste the service account JSON key." };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return {
      valid: false,
      clientEmail: null,
      error: "That doesn't look like valid JSON — paste the whole .json file.",
    };
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return { valid: false, clientEmail: null, error: "The key should be a JSON object." };
  }
  const obj = parsed as Record<string, unknown>;
  if (obj.type !== "service_account") {
    return {
      valid: false,
      clientEmail: null,
      error: 'This isn\'t a service account key (its "type" should be "service_account").',
    };
  }
  for (const field of REQUIRED_SA_FIELDS) {
    const value = obj[field];
    if (typeof value !== "string" || !value.trim()) {
      return { valid: false, clientEmail: null, error: `The key is missing "${field}".` };
    }
  }
  return { valid: true, clientEmail: obj.client_email as string, error: null };
}

export const METHOD_STEP: WizardStep = { id: "method", title: "How you'll use it", kind: "method" };

export const SERVICE_ACCOUNT_STEPS: WizardStep[] = [
  METHOD_STEP,
  { id: "sa-project", title: "Pick a Google Cloud project", kind: "confirm" },
  { id: "sa-create", title: "Create the service account", kind: "confirm" },
  { id: "sa-enable", title: "Turn on the Analytics API", kind: "confirm" },
  { id: "sa-key", title: "Download & paste the key", kind: "input" },
  { id: "sa-grant", title: "Give it Viewer access", kind: "confirm" },
  { id: "sa-name", title: "Name the connection", kind: "input" },
  { id: "sa-save", title: "Save", kind: "action" },
  { id: "sa-verify", title: "Test & finish", kind: "verify" },
];

export const OAUTH_STEPS: WizardStep[] = [
  METHOD_STEP,
  { id: "oauth-project", title: "Pick a Google Cloud project", kind: "confirm" },
  { id: "oauth-enable", title: "Turn on the Analytics API", kind: "confirm" },
  { id: "oauth-consent", title: "Configure the consent screen", kind: "confirm" },
  { id: "oauth-client", title: "Create the OAuth client", kind: "confirm" },
  { id: "oauth-redirect", title: "Add the redirect URI", kind: "confirm" },
  { id: "oauth-config", title: "Save your Client ID & Secret", kind: "action" },
  { id: "oauth-connect", title: "Connect your Google account", kind: "action" },
  { id: "oauth-verify", title: "Test & finish", kind: "verify" },
];

// Console-heavy OAuth steps that are skipped when a client is already configured.
export const OAUTH_CONSOLE_STEP_IDS = [
  "oauth-project",
  "oauth-enable",
  "oauth-consent",
  "oauth-client",
  "oauth-redirect",
  "oauth-config",
];

export interface VisibleStepsInput {
  method: GaSetupMethod;
  showOauthConsoleSteps: boolean;
}

export function computeVisibleSteps({
  method,
  showOauthConsoleSteps,
}: VisibleStepsInput): WizardStep[] {
  if (method === "service_account") return SERVICE_ACCOUNT_STEPS;
  if (showOauthConsoleSteps) return OAUTH_STEPS;
  return OAUTH_STEPS.filter((step) => !OAUTH_CONSOLE_STEP_IDS.includes(step.id));
}

export function isConnectionNameValid(name: string, existingNames: string[]): boolean {
  const trimmed = name.trim();
  return trimmed.length > 0 && !existingNames.includes(trimmed);
}

// Scope an in-project Google Cloud console URL to a specific project so the link
// lands there directly. No-op when the id is blank.
export function withProject(url: string, projectId: string): string {
  const id = projectId.trim();
  if (!id) return url;
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}project=${encodeURIComponent(id)}`;
}

// Google rejects OAuth redirect URIs unless they are http(s) on localhost or an
// https domain with a public TLD — a bare LAN IP (e.g. 192.168.x.x) is refused.
// Returns a warning when the current redirect URI won't be accepted, else null.
export function redirectUriWarning(uri: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(uri);
  } catch {
    return null;
  }
  const host = parsed.hostname.replace(/^\[|\]$/g, "");
  if (host === "localhost" || host === "127.0.0.1" || host === "::1") return null;
  const isIp = /^\d{1,3}(\.\d{1,3}){3}$/.test(host) || host.includes(":");
  if (isIp) {
    return "Google won't accept a redirect URI that points at an IP address. Open Flowfile at http://localhost (same machine), or put it behind a domain with HTTPS.";
  }
  if (parsed.protocol !== "https:") {
    return "Google requires HTTPS for non-localhost redirect URIs. Put Flowfile behind a domain with HTTPS.";
  }
  return null;
}

export interface WizardGateState {
  method: GaSetupMethod;
  confirmed: Record<string, boolean>;
  serviceAccountKey: string;
  connectionName: string;
  clientId: string;
  clientSecret: string;
  oauthConfigured: boolean;
  existingNames: string[];
}

// Whether the active step's primary button should be enabled.
export function canAdvanceStep(step: WizardStep | undefined, state: WizardGateState): boolean {
  if (!step) return false;
  switch (step.id) {
    case "method":
      return true;
    case "sa-key":
      return validateServiceAccountKey(state.serviceAccountKey).valid;
    case "sa-name":
    case "oauth-connect":
      return isConnectionNameValid(state.connectionName, state.existingNames);
    case "sa-save":
      return (
        validateServiceAccountKey(state.serviceAccountKey).valid &&
        isConnectionNameValid(state.connectionName, state.existingNames)
      );
    case "oauth-config":
      return (
        state.clientId.trim().length > 0 &&
        (state.clientSecret.trim().length > 0 || state.oauthConfigured)
      );
    default:
      if (step.kind === "confirm") return state.confirmed[step.id] === true;
      return true;
  }
}
