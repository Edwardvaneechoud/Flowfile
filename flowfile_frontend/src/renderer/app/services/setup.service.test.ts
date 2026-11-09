// What the core can do, read from the one status fetch the app makes before its first route. A status that says
// nothing of it, an older core's, means a core that computes, previews and signs in by token.
import axios, { type AxiosAdapter } from "axios";
import { afterEach, describe, expect, it, vi } from "vitest";

const load = async () => {
  vi.resetModules();
  return (await import("./setup.service")).default;
};

const answering =
  (data: object): AxiosAdapter =>
  async (config) => ({ data, status: 200, statusText: "", headers: {}, config });

const BASE = { setup_required: false, master_key_configured: true };
const EVERYTHING = { computes: true, previews: true, cookie_session: false };

afterEach(() => {
  axios.defaults.adapter = undefined;
});

describe("the setup service's capabilities", () => {
  it("are what a core did before it said anything, until the status is fetched", async () => {
    const setupService = await load();
    expect(setupService.capabilities()).toEqual(EVERYTHING);
  });

  it("are the status's own once it is fetched", async () => {
    axios.defaults.adapter = answering({
      ...BASE,
      mode: "server",
      capabilities: { computes: false, previews: false, cookie_session: true },
    });
    const setupService = await load();
    await setupService.getSetupStatus();
    expect(setupService.computes()).toBe(false);
    expect(setupService.previews()).toBe(false);
    expect(setupService.cookieSession()).toBe(true);
  });

  it("stay what a core did before for a status that carries none", async () => {
    axios.defaults.adapter = answering({ ...BASE, mode: "docker" });
    const setupService = await load();
    await setupService.getSetupStatus();
    expect(setupService.computes()).toBe(true);
    expect(setupService.previews()).toBe(true);
    expect(setupService.cookieSession()).toBe(false);
  });
});
