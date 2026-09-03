// Update-prompt logic + copy: @vue/test-utils is not a dependency, so the .vue is a thin binding.

export const IGNORED_VERSION_KEY = "flowfile.update.ignoredVersion.v1";

export function loadIgnoredVersion(): string | null {
  try {
    return globalThis.localStorage?.getItem(IGNORED_VERSION_KEY) ?? null;
  } catch {
    return null;
  }
}

export function saveIgnoredVersion(version: string): void {
  try {
    globalThis.localStorage?.setItem(IGNORED_VERSION_KEY, version);
  } catch {
    /* localStorage unavailable */
  }
}

export interface UpdatePromptGates {
  availableVersion: string | null;
  ignoredVersion: string | null;
  /** A manual check re-offers even a skipped version. */
  force: boolean;
}

/**
 * Whether a release should be offered. Plain string inequality is enough: the
 * updater feed only ever hands back a version newer than the running build.
 */
export function shouldPromptForUpdate(gates: UpdatePromptGates): boolean {
  return (
    gates.availableVersion !== null &&
    (gates.force || gates.availableVersion !== gates.ignoredVersion)
  );
}

export const UPDATE_COPY = {
  headline: "Update available",
  body: (version: string, currentVersion: string) =>
    `Flowfile ${version} is available — you have ${currentVersion}.`,
  releaseNotesLabel: "View release notes",
  installLabel: "Install now",
  laterLabel: "Remind me later",
  skipLabel: "Skip this version",
  downloadingLine: "Downloading the update…",
  backingUpLine: "Backing up your catalog database…",
  backupPathLine: (path: string) => `Database backed up to ${path}`,
  installingLine: "Stopping background services and installing — this can take a minute.",
  downloadFailedLine: "The download didn't finish. Check your connection and try again.",
  backupFailedLine:
    "Couldn't back up the catalog database. You can install anyway, or cancel and try again later.",
  installFailedLine:
    "The install didn't finish. Background services were stopped, so restart Flowfile before using it again.",
  retryLabel: "Try again",
  cancelLabel: "Cancel",
  continueWithoutBackupLabel: "Continue without backup",
  restartLabel: "Restart Flowfile",
  platformNotes: {
    mac: "Flowfile restarts itself once the update is installed.",
    windows: "Windows asks permission to run the installer, which restarts Flowfile.",
    linux: "Your system may ask for your password to install the package.",
  },
} as const;

export function platformNote(platform: "mac" | "windows" | "linux" | null): string {
  return platform ? UPDATE_COPY.platformNotes[platform] : "";
}

export function releasePageUrl(version: string): string {
  return `https://github.com/edwardvaneechoud/Flowfile/releases/tag/v${version}`;
}
