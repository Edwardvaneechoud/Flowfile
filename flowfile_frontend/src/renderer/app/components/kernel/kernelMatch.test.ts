import { describe, expect, it } from "vitest";

import type { KernelInfo, KernelMatch } from "@/types";

import {
  describeMatch,
  fallbackSuggestion,
  mergePackages,
  pickAutoSelect,
  sortKernelsByMatch,
} from "./kernelMatch";

function kernel(id: string, state: KernelInfo["state"] = "stopped"): KernelInfo {
  return {
    id,
    name: id,
    state,
    container_id: null,
    port: null,
    packages: [],
    resolved_packages: [],
    memory_gb: 4,
    cpu_cores: 2,
    gpu: false,
    image_flavour: "base",
    custom_image: null,
    image: null,
    created_at: "2026-01-01T00:00:00Z",
    error_message: null,
    kernel_version: null,
  };
}

function match(
  kernelId: string,
  level: KernelMatch["level"],
  missing: string[] = [],
  state: KernelMatch["state"] = "stopped",
  unverified: string[] = [],
  unknown: string[] = [],
  imageFlavour: KernelMatch["image_flavour"] = "base",
): KernelMatch {
  return {
    kernel_id: kernelId,
    kernel_name: kernelId,
    state,
    image_flavour: imageFlavour,
    satisfied: [],
    missing,
    unverified,
    unknown,
    invalid: [],
    details: {},
    level,
    baseline: level === "unknown" ? "unknown" : "manifest",
  };
}

describe("describeMatch", () => {
  it("full match", () => {
    expect(describeMatch(match("k", "full"), 2)).toEqual({
      label: "✓ has all packages",
      tone: "full",
    });
  });

  it("partial truncates to two entries with the rest in +N and full list in title", () => {
    const badge = describeMatch(match("k", "partial", ["a", "b", "c"]), 3);
    expect(badge.label).toBe("missing: a, b +1");
    expect(badge.tone).toBe("partial");
    expect(badge.title).toBe("a, b, c");
  });

  it("partial without overflow has no +N", () => {
    expect(describeMatch(match("k", "partial", ["a"]), 2).label).toBe("missing: a");
  });

  it("none reports the dep count", () => {
    expect(describeMatch(match("k", "none", ["a", "b"]), 2).label).toBe("missing all 2");
  });

  it("undefined match is the unknown tone with no label", () => {
    expect(describeMatch(undefined, 2)).toEqual({ label: "", tone: "unknown" });
  });

  it("full with unverified deps gets the softer badge with details in title", () => {
    const badge = describeMatch(match("k", "full", [], "stopped", ["pkg>=1"]), 1);
    expect(badge.label).toBe("✓ should have all packages");
    expect(badge.tone).toBe("full");
    expect(badge.title).toBe("Unverified: pkg>=1");
  });

  it("unknown never claims the packages are there", () => {
    const badge = describeMatch(match("k", "unknown", [], "stopped", [], ["pkg>=1"]), 1);
    expect(badge.label).toBe("? can't verify packages");
    expect(badge.tone).toBe("unknown");
    expect(badge.label).not.toContain("✓");
    expect(badge.title).toContain("pkg>=1");
  });

  it("unknown on a custom image explains why", () => {
    const badge = describeMatch(
      match("k", "unknown", [], "stopped", [], ["pkg>=1"], "custom"),
      1,
    );
    expect(badge.title).toBe("Custom image contents are unknown: pkg>=1");
  });
});

describe("sortKernelsByMatch", () => {
  it("backend order wins for matched kernels", () => {
    const kernels = [kernel("b"), kernel("a")];
    const matches = [match("a", "full"), match("b", "partial")];
    expect(sortKernelsByMatch(kernels, matches).map((k) => k.id)).toEqual(["a", "b"]);
  });

  it("unmatched kernels keep original order, appended last", () => {
    const kernels = [kernel("x"), kernel("a"), kernel("y")];
    const matches = [match("a", "full")];
    expect(sortKernelsByMatch(kernels, matches).map((k) => k.id)).toEqual(["a", "x", "y"]);
  });

  it("empty matches is identity", () => {
    const kernels = [kernel("b"), kernel("a")];
    expect(sortKernelsByMatch(kernels, []).map((k) => k.id)).toEqual(["b", "a"]);
  });
});

describe("pickAutoSelect", () => {
  it("prefers a running full match over a stopped one", () => {
    const kernels = [kernel("stopped-k"), kernel("idle-k", "idle")];
    const matches = [
      match("stopped-k", "full", [], "stopped"),
      match("idle-k", "full", [], "idle"),
    ];
    expect(pickAutoSelect(matches, kernels)).toBe("idle-k");
  });

  it("falls back to the first full match", () => {
    const kernels = [kernel("a"), kernel("b")];
    expect(pickAutoSelect([match("a", "full")], kernels)).toBe("a");
  });

  it("never picks a partial match", () => {
    const kernels = [kernel("a")];
    expect(pickAutoSelect([match("a", "partial", ["x"])], kernels)).toBeNull();
  });

  it("never silently picks a kernel we couldn't verify", () => {
    const kernels = [kernel("a", "idle")];
    expect(pickAutoSelect([match("a", "unknown", [], "idle", [], ["x"])], kernels)).toBeNull();
  });

  it("ignores matches for kernels not in the list", () => {
    expect(pickAutoSelect([match("ghost", "full")], [kernel("a")])).toBeNull();
  });

  it("prefers a provably-complete stopped kernel over a running unverified one", () => {
    const kernels = [kernel("custom-k", "idle"), kernel("verified-k")];
    const matches = [
      match("custom-k", "full", [], "idle", ["a", "b"]),
      match("verified-k", "full", [], "stopped"),
    ];
    expect(pickAutoSelect(matches, kernels)).toBe("verified-k");
  });
});

describe("fallbackSuggestion", () => {
  it("slugifies the node name into a docker-safe id", () => {
    const suggestion = fallbackSuggestion("Ümlaut / node! (v2.0)", ["numpy"]);
    expect(suggestion.config.id).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(suggestion.config.id.endsWith("-kernel")).toBe(true);
  });

  it("dedupes packages and mirrors the backend shape", () => {
    const suggestion = fallbackSuggestion("My Node", ["numpy", "numpy", "pandas>=2"]);
    expect(suggestion.config.packages).toEqual(["numpy", "pandas>=2"]);
    expect(suggestion.config.image_flavour).toBe("base");
    expect(suggestion.covered_by_flavour).toEqual([]);
    expect(suggestion.flavour_image_available).toBeNull();
  });

  it("empty name falls back to custom-node", () => {
    expect(fallbackSuggestion("", []).config.id).toBe("custom-node-kernel");
    expect(fallbackSuggestion("", []).config.name).toBe("Custom node kernel");
  });

  it("dodges existing kernel ids with a numeric suffix", () => {
    const suggestion = fallbackSuggestion("My Node", [], ["my-node-kernel", "my-node-kernel-2"]);
    expect(suggestion.config.id).toBe("my-node-kernel-3");
  });
});

describe("mergePackages", () => {
  it("dedupes while preserving order", () => {
    expect(mergePackages(["a", "b"], ["b", "c"])).toEqual(["a", "b", "c"]);
  });

  it("replaces conflicting same-name specs — the incoming spec wins", () => {
    expect(mergePackages(["pandas>=1,<2", "numpy"], ["pandas>=2"])).toEqual(["numpy", "pandas>=2"]);
    expect(mergePackages(["Scikit_Learn>=1.0"], ["scikit-learn>=1.5"])).toEqual([
      "scikit-learn>=1.5",
    ]);
  });

  it("handles empty inputs", () => {
    expect(mergePackages([], [])).toEqual([]);
    expect(mergePackages([], ["a"])).toEqual(["a"]);
  });
});
