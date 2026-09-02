import { describe, expect, it } from "vitest";

import { docsUrl } from "../../lib/docsLinks";
import {
  CONSENT_COPY,
  EXAMPLE_EVENT,
  TELEMETRY_DOCS_URL,
  decideConsentClose,
  shouldShowConsentModal,
  type ConsentModalGates,
} from "./telemetryConsent";

const openGates = (): ConsentModalGates => ({
  loaded: true,
  available: true,
  consent: null,
  canManage: true,
  routeName: "designer",
  tutorialActive: false,
});

describe("shouldShowConsentModal", () => {
  it("is true only when every gate passes", () => {
    expect(shouldShowConsentModal(openGates())).toBe(true);
  });

  it("stays hidden until status has loaded", () => {
    expect(shouldShowConsentModal({ ...openGates(), loaded: false })).toBe(false);
  });

  it("stays hidden when telemetry is unavailable (kill switch / no endpoint)", () => {
    expect(shouldShowConsentModal({ ...openGates(), available: false })).toBe(false);
  });

  it("never re-prompts once consent was granted or declined", () => {
    expect(shouldShowConsentModal({ ...openGates(), consent: true })).toBe(false);
    expect(shouldShowConsentModal({ ...openGates(), consent: false })).toBe(false);
  });

  it("stays hidden for users who cannot manage the setting", () => {
    expect(shouldShowConsentModal({ ...openGates(), canManage: false })).toBe(false);
  });

  it("shows only on the designer route", () => {
    expect(shouldShowConsentModal({ ...openGates(), routeName: "catalog" })).toBe(false);
    expect(shouldShowConsentModal({ ...openGates(), routeName: undefined })).toBe(false);
  });

  it("never appears over the tutorial overlay", () => {
    expect(shouldShowConsentModal({ ...openGates(), tutorialActive: true })).toBe(false);
  });
});

describe("decideConsentClose", () => {
  it("treats a gate-driven close as no answer at all", () => {
    expect(decideConsentClose("gate", false)).toEqual({
      consent: null,
      tombstone: "never",
      closeImmediately: true,
    });
  });

  it("confirms an accept against the backend before marking the ask answered", () => {
    expect(decideConsentClose("accept", false)).toEqual({
      consent: true,
      tombstone: "on-success",
      closeImmediately: false,
    });
  });

  it("takes a decline immediately and never re-asks", () => {
    expect(decideConsentClose("decline", false)).toEqual({
      consent: false,
      tombstone: "now",
      closeImmediately: true,
    });
  });

  it("posts at most one answer", () => {
    for (const reason of ["accept", "decline", "gate"] as const) {
      expect(decideConsentClose(reason, true).consent).toBeNull();
      expect(decideConsentClose(reason, true).tombstone).toBe("never");
    }
  });
});

describe("EXAMPLE_EVENT contract", () => {
  const ENVELOPE_KEYS = [
    "event",
    "event_id",
    "install_id",
    "app_version",
    "platform",
    "mode",
    "ts",
    "props",
  ];
  const FLOW_RUN_SUCCEEDED_PROPS = [
    "node_count_bucket",
    "node_types",
    "duration_bucket",
    "used_sample_data",
  ];

  it("carries exactly the 8 envelope keys", () => {
    expect(Object.keys(EXAMPLE_EVENT).sort()).toEqual([...ENVELOPE_KEYS].sort());
  });

  it("carries only allowlisted flow_run_succeeded props", () => {
    expect(EXAMPLE_EVENT.event).toBe("flow_run_succeeded");
    for (const key of Object.keys(EXAMPLE_EVENT.props)) {
      expect(FLOW_RUN_SUCCEEDED_PROPS).toContain(key);
    }
  });

  it("holds only contract-shaped values — no user content anywhere", () => {
    expect(EXAMPLE_EVENT.platform).toBe("darwin");
    expect(EXAMPLE_EVENT.mode).toBe("electron");
    expect(EXAMPLE_EVENT.install_id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
    );
    expect(EXAMPLE_EVENT.event_id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(EXAMPLE_EVENT.ts).toBe("2026-08-29T12:00:00Z");
    expect(EXAMPLE_EVENT.props.node_count_bucket).toBe("4-7");
    expect(EXAMPLE_EVENT.props.duration_bucket).toBe("1-10s");
    expect(EXAMPLE_EVENT.props.used_sample_data).toBe(false);
    for (const nodeType of EXAMPLE_EVENT.props.node_types) {
      expect(nodeType).toMatch(/^[a-z_]+$/);
    }
    // Real node-type names, sorted the way the client emits them.
    expect([...EXAMPLE_EVENT.props.node_types]).toEqual(["filter", "output", "read"]);
  });
});

describe("consent copy", () => {
  it("names the kill-switch env var in renderable parts", () => {
    expect(CONSENT_COPY.envVarLine.code).toBe("FLOWFILE_TELEMETRY=0");
    expect(CONSENT_COPY.envVarLine.prefix).toMatch(/hard-disable/i);
    expect(CONSENT_COPY.envVarLine.suffix).toBe(".");
  });

  it("links the docs page through the shared docs-link builder", () => {
    expect(TELEMETRY_DOCS_URL).toBe(docsUrl("users/telemetry.html"));
    expect(TELEMETRY_DOCS_URL).toBe(
      "https://edwardvaneechoud.github.io/Flowfile/users/telemetry.html",
    );
  });

  it("names the destination the events are sent to", () => {
    expect(CONSENT_COPY.body).toContain("events.flowfile.app");
  });

  it("says where the choice can be changed later", () => {
    expect(CONSENT_COPY.recoveryLine).toMatch(/Compute/);
    expect(CONSENT_COPY.recoveryLine).toMatch(/Privacy/);
  });

  it("warns multi-user deployments that the choice covers everyone", () => {
    expect(CONSENT_COPY.serverWideLine).toMatch(/everyone/i);
  });

  it("offers a recoverable failure path rather than a silent drop", () => {
    expect(CONSENT_COPY.saveErrorLine).toMatch(/n't save/i);
    expect(CONSENT_COPY.retryLabel).toBe("Try again");
  });

  it("keeps the decline and accept labels factual, with No as the plain option", () => {
    expect(CONSENT_COPY.declineLabel).toBe("No thanks");
    expect(CONSENT_COPY.acceptLabel).toBe("Share anonymous usage data");
    expect(CONSENT_COPY.body).toMatch(/turned off by default/i);
    expect(CONSENT_COPY.body).toMatch(/never your data/i);
  });
});
