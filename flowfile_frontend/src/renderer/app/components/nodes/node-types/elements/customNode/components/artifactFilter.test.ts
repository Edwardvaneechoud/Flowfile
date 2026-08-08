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
      { value: "model", label: "model (Booster)", key: "model" },
      { value: "scaler", label: "scaler (StandardScaler)", key: "scaler" },
    ];
    expect(buildArtifactOptions("upstream", upstream, global)).toEqual(expected);
    expect(buildArtifactOptions(undefined, upstream, global)).toEqual(expected);
  });

  it("routes to the global list for scope 'global'", () => {
    expect(buildArtifactOptions("global", upstream, global)).toEqual([
      { value: "model", label: "model (Booster)", key: "::model", namespaceId: null },
      { value: "encoder", label: "encoder (OneHotEncoder)", key: "::encoder", namespaceId: null },
    ]);
  });

  it("scope 'all' dedupes by name, upstream entry wins", () => {
    // "model" exists on both sides; the upstream entry + label survives, the
    // global one is dropped. Non-colliding "encoder" is appended.
    expect(buildArtifactOptions("all", upstream, global).map((o) => o.value)).toEqual([
      "model",
      "scaler",
      "encoder",
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
    expect(out.map((o) => o.value)).toEqual(["a", "c"]);
  });

  it("returns an empty list when both sources are empty", () => {
    expect(buildArtifactOptions("all", [], [])).toEqual([]);
    expect(buildArtifactOptions("upstream", [], [])).toEqual([]);
    expect(buildArtifactOptions("global", [], [])).toEqual([]);
  });
});

describe("global artifact options", () => {
  // Two artifacts that happen to share a name are distinct artifacts, so each one
  // needs a value core can resolve on its own: the namespace-qualified reference.
  // Collapsing them (or emitting the bare name twice) is what made the picker
  // offer indistinguishable entries that then failed as ambiguous at run time.
  const twoNamespaces: GlobalArtifactOption[] = [
    {
      name: "churn",
      python_type: "xgboost.Booster",
      namespace_id: 1,
      namespace_path: "General.default",
      version: 2,
    },
    {
      name: "churn",
      python_type: "xgboost.Booster",
      namespace_id: 7,
      namespace_path: "General.news-predictions",
      version: 5,
    },
  ];

  it("qualifies same-named artifacts and labels the namespace only then", () => {
    expect(buildArtifactOptions("global", [], twoNamespaces)).toEqual([
      {
        value: "General.default::churn",
        label: "churn (Booster) — General.default",
        key: "1::churn",
        namespaceId: 1,
      },
      {
        value: "General.news-predictions::churn",
        label: "churn (Booster) — General.news-predictions",
        key: "7::churn",
        namespaceId: 7,
      },
    ]);
  });

  it("gives every option in a result set a unique value and key", () => {
    const mixed: GlobalArtifactOption[] = [
      ...twoNamespaces,
      {
        name: "scaler",
        python_type: "sklearn.StandardScaler",
        namespace_id: 1,
        namespace_path: "General.default",
        version: 1,
      },
    ];
    const out = buildArtifactOptions("global", [], mixed);
    expect(new Set(out.map((o) => o.value)).size).toBe(out.length);
    expect(new Set(out.map((o) => o.key)).size).toBe(out.length);
  });

  it("never puts a version number in a label", () => {
    const out = buildArtifactOptions("global", [], twoNamespaces);
    for (const opt of out) {
      expect(opt.label).not.toMatch(/\bv\d+\b/);
    }
  });

  it("leaves an unambiguous name unsuffixed but still qualifies its value", () => {
    const single: GlobalArtifactOption[] = [
      {
        name: "scaler",
        python_type: "sklearn.StandardScaler",
        namespace_id: 3,
        namespace_path: "General.models",
        version: 4,
      },
    ];
    expect(buildArtifactOptions("global", [], single)).toEqual([
      {
        value: "General.models::scaler",
        label: "scaler (StandardScaler)",
        key: "3::scaler",
        namespaceId: 3,
      },
    ]);
  });

  it("keeps upstream values bare — qualified values are global-scope only", () => {
    const upstream: ArtifactOption[] = [{ name: "churn", type_name: "Booster" }];
    const out = buildArtifactOptions("all", upstream, twoNamespaces);
    // The upstream "churn" shadows both globals by bare name, as before.
    expect(out).toEqual([{ value: "churn", label: "churn (Booster)", key: "churn" }]);
  });

  it("falls back to the bare name when the core ships no namespace_path", () => {
    const legacy: GlobalArtifactOption[] = [
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 2 },
    ];
    expect(buildArtifactOptions("global", [], legacy)).toEqual([
      { value: "churn", label: "churn (Booster)", key: "1::churn", namespaceId: 1 },
    ]);
  });

  it("collapses to the newest version when the server sends every row", () => {
    // Safety net for an older core that ignores ?latest_only=true.
    const versioned: GlobalArtifactOption[] = [
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 1 },
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 3 },
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 2 },
    ];
    const out = buildArtifactOptions("global", [], versioned);
    expect(out).toHaveLength(1);
    expect(out[0].label).toBe("churn (Booster)");
  });

  it("treats a missing namespace_id as one bucket", () => {
    const noNamespace: GlobalArtifactOption[] = [
      { name: "churn", python_type: "xgboost.Booster", version: 1 },
      { name: "churn", python_type: "xgboost.Booster", version: 4 },
    ];
    expect(buildArtifactOptions("global", [], noNamespace)).toEqual([
      { value: "churn", label: "churn (Booster)", key: "::churn", namespaceId: null },
    ]);
  });

  it("applies the type filter before collapsing", () => {
    const mixed: GlobalArtifactOption[] = [
      { name: "churn", python_type: "xgboost.Booster", namespace_id: 1, version: 9 },
      { name: "churn", python_type: "sklearn.Pipeline", namespace_id: 1, version: 2 },
    ];
    // The xgboost row is filtered out, so the surviving sklearn row must win.
    expect(buildArtifactOptions("global", [], mixed, ["sklearn.*"])).toEqual([
      { value: "churn", label: "churn (Pipeline)", key: "1::churn", namespaceId: 1 },
    ]);
  });
});
