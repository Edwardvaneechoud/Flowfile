// The words of the hub publish and open dialogs, kept out of the .vue files so they can be unit-tested
// (@vue/test-utils is not a dependency; the dialogs are thin bindings over these).
import type { HubFlowVersion, HubRefusal, LocalPathUse } from "../../api/hubFlows.api";

export const publishedLabel = (version: number): string => `Published as version ${version}`;

/** One warning line per node that reads or writes a path on this machine. */
export const warningLines = (uses: LocalPathUse[]): string[] =>
  uses.map((use) => `Uses a local file path: ${use.label} (${use.path})`);

export const OPENED_FROM_HUB = "Flow opened from hub";

/** The hub lists an author by id only. */
export const authorLabel = (version: HubFlowVersion): string => `user #${version.authorId}`;

/** Why a publish was refused as stale, from the versions the hub named. */
export function conflictLabel(base: HubFlowVersion | null, current: HubFlowVersion | null): string {
  if (!current) return "The hub has no version this publish can build on.";
  if (!base) {
    return `This flow already has v${current.version} on the hub; ${authorLabel(current)} published it`;
  }
  return `Version ${base.version} is no longer the latest; ${authorLabel(current)} published v${current.version}`;
}

export const publishOverLabel = (current: HubFlowVersion): string =>
  `Publish over v${current.version}`;

export const openInsteadLabel = (current: HubFlowVersion): string =>
  `Open v${current.version} instead`;

/** When a version was published, in the viewer's own zone and naming it; a time without a zone is UTC. */
export function publishedTime(iso: string): string {
  const utc = iso.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(iso) ? iso : `${iso}Z`;
  return new Date(utc).toLocaleString(undefined, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    timeZoneName: "short",
  });
}

export interface VersionFact {
  label: string;
  value: string;
}

/** The side-by-side facts of a version: version, author, published_at, message. */
export function versionFacts(version: HubFlowVersion | null): VersionFact[] {
  if (!version) return [{ label: "Version", value: "none" }];
  return [
    { label: "Version", value: `v${version.version}` },
    { label: "Author", value: authorLabel(version) },
    { label: "Published", value: publishedTime(version.publishedAt) },
    { label: "Message", value: version.message?.trim() || "—" },
  ];
}

/** A one-line account of a refusal, for the kinds that are not the conflict panel. */
export function refusalMessage(refusal: HubRefusal): string {
  switch (refusal.kind) {
    case "stale_base":
      return conflictLabel(refusal.base, refusal.current);
    case "refused":
      return `The hub refused the request (${refusal.status}): ${refusal.detail}`;
    case "other_hub":
      return `This flow came from another hub (${refusal.hubUrl}); connect to that hub to publish it.`;
    case "local_copy_exists":
      return `A local copy of this flow exists at ${refusal.flowPath}.`;
  }
}

export const latestVersionLabel = (latestVersion: number | null): string =>
  latestVersion === null ? "not published yet" : `v${latestVersion}`;
