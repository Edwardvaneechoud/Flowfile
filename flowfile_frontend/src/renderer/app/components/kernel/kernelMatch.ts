// Pure helpers for kernel dependency matching UI. No Vue/axios imports so the
// logic stays unit-testable in the node-env vitest setup.
import type { KernelInfo, KernelMatch, KernelSuggestion } from "@/types";

export interface MatchBadge {
  label: string;
  tone: "full" | "partial" | "none" | "unknown";
  title?: string;
}

export function describeMatch(match: KernelMatch | undefined, depCount: number): MatchBadge {
  if (!match) return { label: "", tone: "unknown" };
  if (match.level === "full") {
    if (match.unverified.length) {
      return {
        label: "✓ should have all packages",
        tone: "full",
        title: `Unverified: ${match.unverified.join(", ")}`,
      };
    }
    return { label: "✓ has all packages", tone: "full" };
  }
  // We couldn't look inside the image, so we know neither way. Saying "has all
  // packages" here is the failure this state exists to prevent.
  if (match.level === "unknown") {
    return {
      label: "? can't verify packages",
      tone: "unknown",
      title:
        match.image_flavour === "custom"
          ? `Custom image contents are unknown: ${match.unknown.join(", ")}`
          : `No package list for this image: ${match.unknown.join(", ")}`,
    };
  }
  if (match.level === "partial") {
    const shown = match.missing.slice(0, 2).join(", ");
    const more = match.missing.length - 2;
    return {
      label: `missing: ${shown}${more > 0 ? ` +${more}` : ""}`,
      tone: "partial",
      title: match.missing.join(", "),
    };
  }
  return {
    label: depCount > 0 ? `missing all ${depCount}` : "missing all",
    tone: "none",
    title: match.missing.join(", "),
  };
}

// Backend order is authoritative for matched kernels; kernels absent from
// `matches` keep their original relative order, appended last.
export function sortKernelsByMatch(kernels: KernelInfo[], matches: KernelMatch[]): KernelInfo[] {
  if (!matches.length) return [...kernels];
  const rank = new Map(matches.map((m, index) => [m.kernel_id, index]));
  const matched: KernelInfo[] = [];
  const unmatched: KernelInfo[] = [];
  for (const kernel of kernels) {
    (rank.has(kernel.id) ? matched : unmatched).push(kernel);
  }
  matched.sort((a, b) => (rank.get(a.id) ?? 0) - (rank.get(b.id) ?? 0));
  return [...matched, ...unmatched];
}

// Full matches only, never partial. Provably-complete (no unverified) wins
// over merely-plausible; running breaks ties.
export function pickAutoSelect(matches: KernelMatch[], kernels: KernelInfo[]): string | null {
  const knownIds = new Set(kernels.map((k) => k.id));
  const full = matches.filter((m) => m.level === "full" && knownIds.has(m.kernel_id));
  const isRunning = (m: KernelMatch) => m.state === "idle" || m.state === "executing";
  const isVerified = (m: KernelMatch) => m.unverified.length === 0;
  const pick =
    full.find((m) => isVerified(m) && isRunning(m)) ??
    full.find(isVerified) ??
    full.find(isRunning) ??
    full[0];
  return pick?.kernel_id ?? null;
}

// Client-side seed for when the match endpoint is unreachable; mirrors the
// backend KernelSuggestion shape. Pass known kernel ids so the generated id
// dodges collisions (kernel ids are globally unique on create).
export function fallbackSuggestion(
  nodeName: string,
  dependencies: string[],
  existingIds: string[] = [],
): KernelSuggestion {
  const slug =
    nodeName
      .replace(/[^A-Za-z0-9_-]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .toLowerCase() || "custom-node";
  const taken = new Set(existingIds);
  const baseId = `${slug}-kernel`;
  let id = baseId;
  for (let n = 2; taken.has(id); n++) id = `${baseId}-${n}`;
  return {
    config: {
      id,
      name: `${nodeName.trim() || "Custom node"} kernel`,
      packages: [...new Set(dependencies)],
      cpu_cores: 2.0,
      memory_gb: 4.0,
      gpu: false,
      image_flavour: "base",
      custom_image: null,
    },
    covered_by_flavour: [],
    flavour_image_available: null,
  };
}

function specName(spec: string): string {
  const match = spec.trim().match(/^[A-Za-z0-9_.-]+/);
  return (match ? match[0] : spec.trim()).toLowerCase().replace(/[-_.]+/g, "-");
}

// Same-name specs conflict inside one pip install — the incoming spec wins.
export function mergePackages(existing: string[], toAdd: string[]): string[] {
  const incomingNames = new Set(toAdd.map(specName));
  const kept = existing.filter((spec) => !incomingNames.has(specName(spec)));
  return [...new Set([...kept, ...toAdd])];
}
