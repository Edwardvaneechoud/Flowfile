import { describe, expect, it } from "vitest";

import { executionLocationLabel, executionLocationOptions } from "./executionLocation";

const keys = (current: string, backendIds: string[]) =>
  executionLocationOptions(current, backendIds).map((option) => option.key);

describe("executionLocationOptions", () => {
  it("offers auto and the configured backends, never local", () => {
    expect(keys("auto", ["worker:team", "worker:gpu"])).toEqual([
      "auto",
      "worker:team",
      "worker:gpu",
    ]);
  });

  it("keeps a flow's backend id that is no longer configured, last", () => {
    expect(keys("worker:gone", ["worker:team"])).toEqual(["auto", "worker:team", "worker:gone"]);
  });

  it("offers auto and the flow's own value when the backends cannot be listed", () => {
    expect(keys("auto", [])).toEqual(["auto"]);
    expect(keys("worker:team", [])).toEqual(["auto", "worker:team"]);
  });

  it("offers exactly local and remote when core answers one of them", () => {
    expect(keys("local", ["worker:team"])).toEqual(["local", "remote"]);
    expect(keys("remote", ["worker:team"])).toEqual(["local", "remote"]);
  });
});

describe("executionLocationLabel", () => {
  it("names each value", () => {
    expect(executionLocationLabel("local")).toBe("Local");
    expect(executionLocationLabel("remote")).toBe("Remote");
    expect(executionLocationLabel("auto")).toBe("Auto (default worker)");
    expect(executionLocationLabel("worker:team")).toBe("worker:team");
  });
});
