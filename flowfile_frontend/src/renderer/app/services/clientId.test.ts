// @vitest-environment happy-dom
// A pop-out keeps its own client id key: the browser copies the opener's sessionStorage into it.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const load = async () => {
  vi.resetModules();
  return import("./clientId");
};

describe("clientId", () => {
  beforeEach(() => {
    sessionStorage.clear();
    window.location.hash = "";
  });

  afterEach(() => {
    window.location.hash = "";
  });

  it("mints one id per window and keeps it across reloads", async () => {
    const first = await load();
    expect(first.clientId).toBeTruthy();
    expect(sessionStorage.getItem(first.STORAGE_KEY)).toBe(first.clientId);
    const second = await load();
    expect(second.clientId).toBe(first.clientId);
  });

  it("gives a pop-out its own id even with the opener's id copied in", async () => {
    const designer = await load();
    window.location.hash = "#/notebook?flow=4";
    const popout = await load();
    expect(popout.clientId).not.toBe(designer.clientId);
    expect(sessionStorage.getItem(popout.POPOUT_STORAGE_KEY)).toBe(popout.clientId);
    expect(sessionStorage.getItem(popout.STORAGE_KEY)).toBe(designer.clientId);
    const reloaded = await load();
    expect(reloaded.clientId).toBe(popout.clientId);
  });

  it("treats every kind's window route as a pop-out", async () => {
    const designer = await load();
    for (const hash of ["#/popout/table?flow=3", "#/popout/logs?flow=3", "#/popout/ai?flow=3"]) {
      sessionStorage.removeItem("flowfile.client_id.popout.v1");
      window.location.hash = hash;
      const popout = await load();
      expect(popout.clientId).not.toBe(designer.clientId);
      expect(sessionStorage.getItem(popout.POPOUT_STORAGE_KEY)).toBe(popout.clientId);
    }
  });
});
