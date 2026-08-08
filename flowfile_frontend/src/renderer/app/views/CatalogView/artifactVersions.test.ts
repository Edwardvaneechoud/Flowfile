import { describe, expect, it } from "vitest";
import type { ArtifactVersionInfo } from "../../types";
import {
  ARTIFACT_VERSIONS_PAGE_SIZE,
  artifactRef,
  isLatest,
  latestVersionNumber,
  pageAfterDelete,
  pruneCandidates,
  totalPages,
} from "./artifactVersions";

const version = (v: number, sizeBytes: number | null = 100): ArtifactVersionInfo => ({
  id: 1000 + v,
  version: v,
  created_at: "2026-08-01T00:00:00",
  size_bytes: sizeBytes,
  sha256: `sha-${v}`,
  python_type: "xgboost.core.Booster",
  blob_exists: true,
});

describe("artifactRef", () => {
  it("qualifies the name with the namespace path", () => {
    expect(artifactRef("churn", ["General", "news-predictions"])).toBe(
      "General.news-predictions::churn",
    );
  });

  it("falls back to the bare name without a namespace path", () => {
    expect(artifactRef("churn", [])).toBe("churn");
  });
});

describe("totalPages", () => {
  it("floors at 1 for an empty list", () => {
    expect(totalPages(0)).toBe(1);
  });

  it("does not add an empty page on an exact multiple", () => {
    expect(totalPages(50, 25)).toBe(2);
  });

  it("counts a partial last page", () => {
    expect(totalPages(51, 25)).toBe(3);
  });

  it("floors at 1 for a nonsensical page size", () => {
    expect(totalPages(42, 0)).toBe(1);
  });
});

describe("pageAfterDelete", () => {
  it("steps back when the current page no longer exists", () => {
    expect(pageAfterDelete(3, 50, 25)).toBe(2);
  });

  it("stays put while the page still has rows", () => {
    expect(pageAfterDelete(2, 51, 25)).toBe(2);
  });

  it("never returns page 0 for an emptied list", () => {
    expect(pageAfterDelete(4, 0, 25)).toBe(1);
  });
});

describe("latestVersionNumber / isLatest", () => {
  it("takes the artifact's own version when the page holds only older rows", () => {
    expect(latestVersionNumber(42, [version(3), version(2)])).toBe(42);
  });

  it("takes a higher version from the loaded page", () => {
    expect(latestVersionNumber(3, [version(43), version(3)])).toBe(43);
  });

  it("flags only the max version as latest", () => {
    const latest = latestVersionNumber(42, [version(42), version(41)]);
    expect(isLatest(42, latest)).toBe(true);
    expect(isLatest(41, latest)).toBe(false);
  });
});

describe("pruneCandidates", () => {
  const versions = [version(5, 10), version(4, 20), version(3, null), version(2, 40), version(1)];

  it("deletes nothing when keep covers the whole history", () => {
    expect(pruneCandidates(versions, 5).toDelete).toEqual([]);
    expect(pruneCandidates(versions, 9).toDelete).toEqual([]);
  });

  it("keeps only the newest when keep is 1", () => {
    const { toDelete } = pruneCandidates(versions, 1);
    expect(toDelete.map((v) => v.version)).toEqual([4, 3, 2, 1]);
  });

  it("never deletes the newest version even for keep < 1", () => {
    const { toDelete } = pruneCandidates(versions, 0);
    expect(toDelete.map((v) => v.version)).toEqual([4, 3, 2, 1]);
  });

  it("sums freed bytes treating null as 0", () => {
    const { freedBytes } = pruneCandidates(versions, 2);
    expect(freedBytes).toBe(140);
  });

  it("sorts an out-of-order list before slicing", () => {
    const { toDelete } = pruneCandidates([version(1), version(5), version(3)], 2);
    expect(toDelete.map((v) => v.version)).toEqual([1]);
  });
});

describe("page size", () => {
  it("matches the house page size", () => {
    expect(ARTIFACT_VERSIONS_PAGE_SIZE).toBe(25);
  });
});
