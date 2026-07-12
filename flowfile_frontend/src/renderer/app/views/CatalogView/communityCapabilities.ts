// Plain-language mapping of a community node's risk_flags (produced by the
// backend AST security scanner) into the capability chips shown in the consent
// dialog. Extracted from the dialog component so it is unit-testable without a
// DOM mount (@vue/test-utils is not a dependency).

export interface CapabilityLine {
  flag: string;
  label: string;
  description: string;
}

// Keyed by the scanner's risk_flag ids. Unknown flags fall through to a generic
// line so a newly-added backend flag is still surfaced, never silently dropped.
const CAPABILITY_COPY: Record<string, { label: string; description: string }> = {
  network: {
    label: "Network access",
    description: "Can open network connections and send or receive data over the internet.",
  },
  fs_write: {
    label: "Writes files",
    description: "Can create or modify files on this machine's disk.",
  },
  fs_read: {
    label: "Reads files",
    description: "Can read files from this machine's disk.",
  },
  subprocess: {
    label: "Runs programs",
    description: "Can launch other programs or system commands.",
  },
  env_read: {
    label: "Reads environment",
    description: "Can read environment variables, which may hold configuration or secrets.",
  },
  dynamic_code: {
    label: "Dynamic code",
    description: "Builds and runs code at runtime, which the scanner cannot fully inspect.",
  },
  secrets: {
    label: "Uses secrets",
    description: "Requests a stored secret that is passed to the node when it runs.",
  },
  serialization: {
    label: "Loads serialized data",
    description: "Deserializes data (e.g. pickle), which can execute code while loading.",
  },
};

export function capabilityLines(riskFlags: string[]): CapabilityLine[] {
  return riskFlags.map((flag) => {
    const copy = CAPABILITY_COPY[flag];
    return {
      flag,
      label: copy?.label ?? flag,
      description:
        copy?.description ?? "Uses an elevated capability flagged by the security scanner.",
    };
  });
}

export const NO_CAPABILITIES_NOTE =
  "No elevated capabilities detected — this is not a safety guarantee.";

export function environmentNote(environment: "local" | "kernel"): string {
  return environment === "kernel"
    ? "This node runs in an isolated Docker kernel; your secrets are not available there."
    : "This node runs on this machine's worker with your data and any secrets you assign it.";
}

/** First 12 hex chars of the pinned node sha256, matching the receipt short form. */
export function shortSha(sha256: string): string {
  return (sha256 ?? "").slice(0, 12);
}
