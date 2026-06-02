// Unit tests for the GA setup wizard's pure logic: service-account key
// validation, which steps are visible per method, and per-step gating.

import { describe, it, expect } from "vitest";
import {
  canAdvanceStep,
  computeVisibleSteps,
  OAUTH_CONSOLE_STEP_IDS,
  OAUTH_STEPS,
  redirectUriWarning,
  SERVICE_ACCOUNT_STEPS,
  validateServiceAccountKey,
  withProject,
  type WizardGateState,
} from "./wizardLogic";

const validKey = JSON.stringify({
  type: "service_account",
  client_email: "robot@my-project.iam.gserviceaccount.com",
  private_key: "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
  token_uri: "https://oauth2.googleapis.com/token",
});

const baseState = (overrides: Partial<WizardGateState> = {}): WizardGateState => ({
  method: "service_account",
  confirmed: {},
  serviceAccountKey: "",
  connectionName: "",
  clientId: "",
  clientSecret: "",
  oauthConfigured: false,
  existingNames: [],
  ...overrides,
});

const step = (steps: typeof SERVICE_ACCOUNT_STEPS, id: string) => {
  const found = steps.find((s) => s.id === id);
  if (!found) throw new Error(`missing step ${id}`);
  return found;
};

describe("validateServiceAccountKey", () => {
  it("rejects empty input", () => {
    expect(validateServiceAccountKey("   ").valid).toBe(false);
  });

  it("rejects non-JSON", () => {
    const result = validateServiceAccountKey("not json {");
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/valid JSON/);
  });

  it("rejects a JSON array", () => {
    expect(validateServiceAccountKey("[]").valid).toBe(false);
  });

  it("rejects the wrong type", () => {
    const result = validateServiceAccountKey(JSON.stringify({ type: "authorized_user" }));
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/service_account/);
  });

  it("rejects a key missing a required field", () => {
    const result = validateServiceAccountKey(
      JSON.stringify({ type: "service_account", client_email: "a@b.com", private_key: "x" }),
    );
    expect(result.valid).toBe(false);
    expect(result.error).toMatch(/token_uri/);
  });

  it("accepts a complete key and extracts client_email", () => {
    const result = validateServiceAccountKey(validKey);
    expect(result.valid).toBe(true);
    expect(result.clientEmail).toBe("robot@my-project.iam.gserviceaccount.com");
    expect(result.error).toBeNull();
  });
});

describe("computeVisibleSteps", () => {
  it("returns the service-account steps", () => {
    const steps = computeVisibleSteps({ method: "service_account", showOauthConsoleSteps: true });
    expect(steps).toEqual(SERVICE_ACCOUNT_STEPS);
  });

  it("returns the full OAuth steps when console steps are shown", () => {
    const steps = computeVisibleSteps({ method: "oauth", showOauthConsoleSteps: true });
    expect(steps).toEqual(OAUTH_STEPS);
  });

  it("drops the console steps when already configured", () => {
    const steps = computeVisibleSteps({ method: "oauth", showOauthConsoleSteps: false });
    const ids = steps.map((s) => s.id);
    OAUTH_CONSOLE_STEP_IDS.forEach((id) => expect(ids).not.toContain(id));
    expect(ids).toEqual(["method", "oauth-connect", "oauth-verify"]);
  });
});

describe("withProject", () => {
  it("returns the url unchanged when no project id", () => {
    expect(withProject("https://console.cloud.google.com/apis/credentials", "  ")).toBe(
      "https://console.cloud.google.com/apis/credentials",
    );
  });

  it("appends ?project when the url has no query", () => {
    expect(withProject("https://console.cloud.google.com/apis/credentials", "my-proj")).toBe(
      "https://console.cloud.google.com/apis/credentials?project=my-proj",
    );
  });

  it("appends &project when the url already has a query", () => {
    expect(withProject("https://console.cloud.google.com/x?a=1", "my proj")).toBe(
      "https://console.cloud.google.com/x?a=1&project=my%20proj",
    );
  });
});

describe("redirectUriWarning", () => {
  it("warns on a LAN IP", () => {
    expect(
      redirectUriWarning("http://192.168.50.180:8080/api/ga_connections/oauth/callback"),
    ).toMatch(/IP address/);
  });

  it("allows localhost web origin", () => {
    expect(
      redirectUriWarning("http://localhost:8080/api/ga_connections/oauth/callback"),
    ).toBeNull();
  });

  it("allows the desktop localhost/loopback callback", () => {
    expect(redirectUriWarning("http://localhost:63578/ga_connections/oauth/callback")).toBeNull();
    expect(redirectUriWarning("http://127.0.0.1:63578/ga_connections/oauth/callback")).toBeNull();
  });

  it("allows an https domain", () => {
    expect(
      redirectUriWarning("https://ga.example.com/api/ga_connections/oauth/callback"),
    ).toBeNull();
  });

  it("warns on a non-localhost http domain", () => {
    expect(
      redirectUriWarning("http://flowfile.internal/api/ga_connections/oauth/callback"),
    ).toMatch(/HTTPS/);
  });
});

describe("canAdvanceStep", () => {
  it("always allows the method step", () => {
    expect(canAdvanceStep(step(SERVICE_ACCOUNT_STEPS, "method"), baseState())).toBe(true);
  });

  it("gates confirm steps on their checkbox", () => {
    const s = step(SERVICE_ACCOUNT_STEPS, "sa-create");
    expect(canAdvanceStep(s, baseState())).toBe(false);
    expect(canAdvanceStep(s, baseState({ confirmed: { "sa-create": true } }))).toBe(true);
  });

  it("gates the key step on a valid key", () => {
    const s = step(SERVICE_ACCOUNT_STEPS, "sa-key");
    expect(canAdvanceStep(s, baseState({ serviceAccountKey: "{}" }))).toBe(false);
    expect(canAdvanceStep(s, baseState({ serviceAccountKey: validKey }))).toBe(true);
  });

  it("gates the name step on a non-empty, unique name", () => {
    const s = step(SERVICE_ACCOUNT_STEPS, "sa-name");
    expect(canAdvanceStep(s, baseState({ connectionName: "  " }))).toBe(false);
    expect(canAdvanceStep(s, baseState({ connectionName: "ga4", existingNames: ["ga4"] }))).toBe(
      false,
    );
    expect(canAdvanceStep(s, baseState({ connectionName: "ga4" }))).toBe(true);
  });

  it("requires id + secret on first OAuth config, secret optional once configured", () => {
    const s = step(OAUTH_STEPS, "oauth-config");
    expect(canAdvanceStep(s, baseState({ clientId: "id" }))).toBe(false);
    expect(canAdvanceStep(s, baseState({ clientId: "id", clientSecret: "secret" }))).toBe(true);
    expect(canAdvanceStep(s, baseState({ clientId: "id", oauthConfigured: true }))).toBe(true);
  });

  it("always allows finishing the verify step", () => {
    expect(canAdvanceStep(step(SERVICE_ACCOUNT_STEPS, "sa-verify"), baseState())).toBe(true);
  });
});
