// Consent-modal logic + copy: @vue/test-utils is not a dependency, so the .vue is a thin binding.

import { docsUrl } from "../../lib/docsLinks";

export const TELEMETRY_DOCS_URL = docsUrl("users/telemetry.html");

// Canonical example event from the telemetry contract, shown verbatim here and on the docs page.
export const EXAMPLE_EVENT = {
  event: "flow_run_succeeded",
  event_id: "b7a1d9c4-3e52-4f18-9a6b-0c5d2e7f8a13",
  install_id: "3f6b1c2e-8a94-4c50-9d0e-2f7a61b8c4d1",
  app_version: "0.12.7",
  platform: "darwin",
  mode: "electron",
  ts: "2026-08-29T12:00:00Z",
  props: {
    node_count_bucket: "4-7",
    node_types: ["filter", "output", "read"],
    duration_bucket: "1-10s",
    used_sample_data: false,
  },
} as const;

export const CONSENT_COPY = {
  headline: "Help improve Flowfile?",
  body:
    "Flowfile can send anonymous usage events to the project's own collector at " +
    "events.flowfile.app, to help prioritise development. " +
    "This is turned off by default. " +
    "Events describe features you use — never your data, file paths, column names, " +
    "formulas, or anything you type. " +
    "It would be very helpful to understand better how you use Flowfile!",
  // Split so the modal can wrap the env var in <code> without duplicating copy.
  envVarLine: {
    prefix: "Operators can hard-disable this with ",
    code: "FLOWFILE_TELEMETRY=0",
    suffix: ".",
  },
  exampleToggleLabel: "See an example event",
  docsLinkLabel: "Read exactly what is sent",
  acceptLabel: "Share anonymous usage data",
  declineLabel: "No thanks",
  recoveryLine: "You can change this later under Compute → Privacy.",
  // Rendered only on multi-user deployments.
  serverWideLine: "This server is shared: your choice applies to everyone using it.",
  saveErrorLine: "Couldn't save that choice — nothing was turned on. Try again?",
  retryLabel: "Try again",
} as const;

export interface ConsentModalGates {
  loaded: boolean;
  available: boolean;
  consent: boolean | null;
  canManage: boolean;
  routeName: unknown;
  tutorialActive: boolean;
}

/**
 * The dialog appears at most once ever: only after status has loaded, only
 * when telemetry is deliverable and undecided, only for users allowed to
 * decide, only on the Designer route, and never over the tutorial overlay.
 */
export function shouldShowConsentModal(gates: ConsentModalGates): boolean {
  return (
    gates.loaded &&
    gates.available &&
    gates.consent === null &&
    gates.canManage &&
    gates.routeName === "designer" &&
    !gates.tutorialActive
  );
}

/**
 * How the dialog closed. `gate` is every close the user did not ask for — the
 * visibility gates flipping (route change, logout, tutorial, a refetched
 * status), which el-dialog reports through the same close path as a real
 * dismissal.
 */
export type ConsentCloseReason = "accept" | "decline" | "gate";

/** When the never-ask-again marker is written, if ever. */
export type ConsentTombstone = "now" | "on-success" | "never";

export interface ConsentCloseDecision {
  /** Consent value to POST; `null` means the close carried no answer. */
  consent: boolean | null;
  tombstone: ConsentTombstone;
  /** `false` keeps the dialog up until the POST resolves, to surface failure. */
  closeImmediately: boolean;
}

const NO_ANSWER: ConsentCloseDecision = {
  consent: null,
  tombstone: "never",
  closeImmediately: true,
};

/**
 * Given how the dialog closed, decide what happens.
 *
 * Only a deliberate answer counts: a gate-driven close neither posts nor marks
 * the ask answered, so it returns later. An accept is confirmed by the backend
 * before the marker is written — a lost opt-in must stay recoverable. A decline
 * is best-effort: its failure direction is "no telemetry", which is what was
 * asked for, so it closes at once and still marks the ask answered rather than
 * nagging someone who already said no.
 */
export function decideConsentClose(
  reason: ConsentCloseReason,
  alreadyAnswered: boolean,
): ConsentCloseDecision {
  if (alreadyAnswered || reason === "gate") return NO_ANSWER;
  if (reason === "accept") {
    return { consent: true, tombstone: "on-success", closeImmediately: false };
  }
  return { consent: false, tombstone: "now", closeImmediately: true };
}
