import { describe, expect, it } from "vitest";
import { buildArtifactOptions, formatTypeShort, matchesTypeFilter } from "./artifactFilter";
import type { ArtifactOption, GlobalArtifactOption } from "../interface";

describe("matchesTypeFilter", () => {
  it("matches everything when the filter is empty or absent", () => {
    expect(matchesTypeFilter("xgboost.Booster", [])).toBe(true);
    expect(matchesTypeFilter("xgboost.Booster")).toBe(true);
  });

  it("matches an exact dotted type", () => {
    expect(matchesTypeFilter("xgboost.Booster", ["xgboost.Booster"])).toBe(true);
    expect(matchesTypeFilter("xgboost.Booster", ["xgboost.Model"])).toBe(false);
  });

  it("honours a prefix glob", () => {
    expect(matchesTypeFilter("xgboost.Booster", ["xgboost.*"])).toBe(true);
    expect(matchesTypeFilter("lightgbm.Booster", ["xgboost.*"])).toBe(false);
  });

  it("honours a suffix glob", () => {
    expect(matchesTypeFilter("xgboost.Booster", ["*.Booster"])).toBe(true);
    expect(matchesTypeFilter("xgboost.Model", ["*.Booster"])).toBe(false);
  });

  it("treats regex specials in the type as literal", () => {
    expect(matchesTypeFilter("pkg.Foo[Bar]", ["pkg.Foo[Bar]"])).toBe(true);
    expect(matchesTypeFilter("pkg.Foo[Bar]", ["pkg.*"])).toBe(true);
    // A dot in the glob is literal, not a regex wildcard.
    expect(matchesTypeFilter("pkgXFoo", ["pkg.Foo"])).toBe(false);
  });

  it("is case-sensitive", () => {
    expect(matchesTypeFilter("xgboost.Booster", ["xgboost.booster"])).toBe(false);
  });

  it("ORs multiple globs", () => {
    expect(matchesTypeFilter("xgboost.Booster", ["nope", "xgboost.*"])).toBe(true);
    expect(matchesTypeFilter("xgboost.Booster", ["nope", "also.nope"])).toBe(false);
  });
});

describe("formatTypeShort", () => {
  it("returns the last dot segment", () => {
    expect(formatTypeShort("xgboost.sklearn.XGBClassifier")).toBe("XGBClassifier");
  });

  it("returns the input unchanged when there is no dot", () => {
    expect(formatTypeShort("Booster")).toBe("Booster");
  });
});

describe("buildArtifactOptions", () => {
  const upstream: ArtifactOption[] = [
    { name: "model", type_name: "Booster", module: "xgboost" },
    { name: "scaler", type_name: "StandardScaler", module: "sklearn" },
  ];
  const global: GlobalArtifactOption[] = [
    { name: "model", python_type: "lightgbm.Booster", version: 3 },
    { name: "encoder", python_type: "sklearn.OneHotEncoder", version: 1 },
  ];

  it("routes to the upstream list for scope 'upstream' (and undefined default)", () => {
    const expected = [
      ["model", "model (Booster)"],
      ["scaler", "scaler (StandardScaler)"],
    ];
    expect(buildArtifactOptions("upstream", upstream, global)).toEqual(expected);
    expect(buildArtifactOptions(undefined, upstream, global)).toEqual(expected);
  });

  it("routes to the global list for scope 'global'", () => {
    expect(buildArtifactOptions("global", upstream, global)).toEqual([
      ["model", "model (v3 · Booster)"],
      ["encoder", "encoder (v1 · OneHotEncoder)"],
    ]);
  });

  it("scope 'all' dedupes by name, upstream entry wins", () => {
    // "model" exists on both sides; the upstream entry + label survives, the
    // global one is dropped. Non-colliding "encoder" is appended.
    expect(buildArtifactOptions("all", upstream, global)).toEqual([
      ["model", "model (Booster)"],
      ["scaler", "scaler (StandardScaler)"],
      ["encoder", "encoder (v1 · OneHotEncoder)"],
    ]);
  });

  it("scope 'all' applies the type filter to both sides", () => {
    const up: ArtifactOption[] = [
      { name: "a", type_name: "Booster", module: "xgboost" },
      { name: "b", type_name: "Booster", module: "lightgbm" },
    ];
    const gl: GlobalArtifactOption[] = [
      { name: "c", python_type: "xgboost.XGBModel", version: 1 },
      { name: "d", python_type: "sklearn.Foo", version: 2 },
    ];
    const out = buildArtifactOptions("all", up, gl, ["xgboost.*"]);
    expect(out.map(([name]) => name)).toEqual(["a", "c"]);
  });

  it("returns an empty list when both sources are empty", () => {
    expect(buildArtifactOptions("all", [], [])).toEqual([]);
    expect(buildArtifactOptions("upstream", [], [])).toEqual([]);
    expect(buildArtifactOptions("global", [], [])).toEqual([]);
  });
});

describe("global artifacts collapse to their newest version", () => {
  // A bare name resolves to the newest version server-side, so older rows were N
  // choices that all meant the same artifact.
  const versioned: GlobalArtifactOption[] = [
    { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 3 },
    { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 2 },
    { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 1 },
    { name: "scaler", python_type: "sklearn.StandardScaler", namespace_id: 1, version: 1 },
  ];

  it("keeps one option per artifact, at its highest version", () => {
    expect(buildArtifactOptions("global", [], versioned)).toEqual([
      ["churn", "churn (v3 · Booster)"],
      ["scaler", "scaler (v1 · StandardScaler)"],
    ]);
  });

  it("picks the highest version regardless of arrival order", () => {
    const shuffled = [versioned[1], versioned[2], versioned[0]];
    expect(buildArtifactOptions("global", [], shuffled)).toEqual([["churn", "churn (v3 · Booster)"]]);
  });

  it("collapses for scope 'all' too, after the upstream-wins dedupe", () => {
    const upstream: ArtifactOption[] = [{ name: "scaler", type_name: "StandardScaler" }];
    expect(buildArtifactOptions("all", upstream, versioned)).toEqual([
      ["scaler", "scaler (StandardScaler)"],
      ["churn", "churn (v3 · Booster)"],
    ]);
  });

  it("keeps same-named artifacts in different namespaces apart", () => {
    // The backend rejects a bare-name lookup that is ambiguous across namespaces —
    // merging them here would hide that conflict instead of surfacing it.
    const twoNamespaces: GlobalArtifactOption[] = [
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 2 },
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 7, version: 5 },
    ];
    expect(buildArtifactOptions("global", [], twoNamespaces)).toEqual([
      ["churn", "churn (v2 · Booster)"],
      ["churn", "churn (v5 · Booster)"],
    ]);
  });

  it("treats a missing namespace_id as one bucket", () => {
    const noNamespace: GlobalArtifactOption[] = [
      { name: "churn", python_type: "xgboost.Booster", version: 1 },
      { name: "churn", python_type: "xgboost.Booster", version: 4 },
    ];
    expect(buildArtifactOptions("global", [], noNamespace)).toEqual([
      ["churn", "churn (v4 · Booster)"],
    ]);
  });

  it("applies the type filter before collapsing", () => {
    const mixed: GlobalArtifactOption[] = [
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 9 },
      { name: "churn", python_type: "sklearn.Pipeline", namespace_id: 1, version: 2 },
    ];
    // v9 is filtered out, so the surviving sklearn row must win — not "no options".
    expect(buildArtifactOptions("global", [], mixed, ["sklearn.*"])).toEqual([
      ["churn", "churn (v2 · Pipeline)"],
    ]);
  });
});
