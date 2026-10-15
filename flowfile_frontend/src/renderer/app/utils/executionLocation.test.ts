import { describe, expect, it } from "vitest";

import {
  executionLocationLabel,
  executionLocationOptions,
  nodeExecutionLocationLabel,
  nodeExecutionLocationOptions,
} from "./executionLocation";

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

const nodeKeys = (current: string, backendIds: string[]) =>
  nodeExecutionLocationOptions(current, backendIds).map((option) => option.key);

describe("nodeExecutionLocationOptions", () => {
  it("offers auto, the default worker and the configured backends, never local", () => {
    expect(nodeKeys("auto", ["worker:team", "worker:gpu"])).toEqual([
      "auto",
      "worker",
      "worker:team",
      "worker:gpu",
    ]);
  });

  it("keeps a node's backend id that is no longer configured, last", () => {
    expect(nodeKeys("worker:gone", ["worker:team"])).toEqual([
      "auto",
      "worker",
      "worker:team",
      "worker:gone",
    ]);
  });

  it("keeps a node saved as local, last, and offers local to no other node", () => {
    expect(nodeKeys("local", ["worker:team"])).toEqual(["auto", "worker", "worker:team", "local"]);
    expect(nodeKeys("worker", ["worker:team"])).not.toContain("local");
  });
});

describe("nodeExecutionLocationLabel", () => {
  it("names each value", () => {
    expect(nodeExecutionLocationLabel("auto")).toBe("Auto (the flow's backend)");
    expect(nodeExecutionLocationLabel("worker")).toBe("Default worker");
    expect(nodeExecutionLocationLabel("local")).toBe("Local");
    expect(nodeExecutionLocationLabel("worker:team")).toBe("worker:team");
  });
});
