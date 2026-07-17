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
