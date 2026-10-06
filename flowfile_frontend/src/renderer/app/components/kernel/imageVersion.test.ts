import { describe, it, expect } from "vitest";
import { imageUpdateAvailable, parseImageVersion } from "./imageVersion";

const REPO = "edwardvaneechoud/flowfile-kernel-notebook";

describe("parseImageVersion", () => {
  it("reads a dotted numeric tag", () => {
    expect(parseImageVersion(`${REPO}:0.22.1`)).toEqual([0, 22, 1]);
    expect(parseImageVersion("registry.local:5000/team/kernel:1.2")).toEqual([1, 2]);
  });

  it("is null for non-version tags", () => {
    expect(parseImageVersion("flowfile-kernel-notebook:local")).toBeNull();
    expect(parseImageVersion("flowfile-kernel-notebook")).toBeNull();
    expect(parseImageVersion(`${REPO}:0.22.1-rc1`)).toBeNull();
  });
});

describe("imageUpdateAvailable", () => {
  it("flags an older release of the same repo", () => {
    expect(imageUpdateAvailable(`${REPO}:0.22.0`, `${REPO}:0.22.1`)).toEqual({
      available: true,
      latest: `${REPO}:0.22.1`,
    });
    expect(imageUpdateAvailable(`${REPO}:0.22.1`, `${REPO}:0.22.1`)).toEqual({
      available: false,
      latest: `${REPO}:0.22.1`,
    });
    expect(imageUpdateAvailable(`${REPO}:0.23.0`, `${REPO}:0.22.1`)?.available).toBe(false);
  });

  it("stays silent for other repos, dev builds and unknown tags", () => {
    expect(imageUpdateAvailable("flowfile-kernel-notebook:local", `${REPO}:0.22.1`)).toBeNull();
    expect(imageUpdateAvailable("flowfile-kernel-derived-notebook:latest", `${REPO}:0.22.1`)).toBeNull();
    expect(imageUpdateAvailable(null, `${REPO}:0.22.1`)).toBeNull();
    expect(imageUpdateAvailable(`${REPO}:0.22.0`, undefined)).toBeNull();
  });
});
