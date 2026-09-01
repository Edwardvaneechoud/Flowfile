// Pure logic + copy for the telemetry consent modal. @vue/test-utils is not a
// dependency, so everything meaningful lives here (unit-tested in
// telemetryConsent.test.ts) and the .vue stays a thin binding.

import { docsUrl } from "../../lib/docsLinks";

export const TELEMETRY_DOCS_URL = docsUrl("users/telemetry.html");

// The canonical example event from the telemetry contract — shown verbatim in
// the modal's collapsed "See an example event" section and on the docs page.
export const EXAMPLE_EVENT = {
  event: "flow_run_succeeded",
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
    "Flowfile can send anonymous usage events to help prioritise development. " +
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
