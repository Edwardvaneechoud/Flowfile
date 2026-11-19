// @vitest-environment happy-dom
// The Hub card as an administrator sees it: the sign-in through the browser and the password form behind it, the
// connected desktop's facts, and on a server that joined its hub the line saying whose hub it belongs to, with
// nothing to type.
import { h, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { desktop, none, server } from "../../__fixtures__/hubConnection";
import { element, fakeStorage, mountIn, settle, text } from "../../__fixtures__/mount";
import { httpError } from "../../__fixtures__/promises";
import type { HubConnection } from "../../api/hubConnection.api";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  start: vi.fn(),
  status: vi.fn(),
  cancel: vi.fn(),
  openExternal: vi.fn(),
}));

vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: vi.fn(),
  disconnectHub: vi.fn(),
  startBrowserSignIn: mocks.start,
  getBrowserSignIn: mocks.status,
  cancelBrowserSignIn: mocks.cancel,
  PASSWORD_ONLY: "password_only",
}));
vi.mock("element-plus", () => ({ ElMessage: { success: vi.fn(), error: vi.fn() } }));
vi.mock("../../../lib/desktop", () => ({ desktop: { openExternal: mocks.openExternal } }));

import { ElMessage } from "element-plus";
import { connectHub } from "../../api/hubConnection.api";
import { BROWSER_POLL_MS } from "../../stores/hub-connection-store";
import HubPanel from "./HubPanel.vue";

const stubs: Record<string, FunctionalComponent> = {
  "el-collapse-transition": (_, { slots }) => slots.default?.(),
  "el-button": element("button"),
  "el-input": (_, { attrs }) => {
    const { modelValue, "onUpdate:modelValue": update, ...rest } = attrs;
    return h("input", {
      ...rest,
      value: modelValue,
      onInput: (e: Event) =>
        (update as (v: string) => void)?.((e.target as HTMLInputElement).value),
    });
  },
  "router-link": element("a"),
};

let host: HTMLElement;

const mount = async (connection: HubConnection) => {
  mocks.get.mockResolvedValue(connection);
  host = await mountIn(HubPanel, { pinia: createPinia(), stubs });
};

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);
const click = async (testId: string) => {
  (find(testId) as HTMLElement).click();
  await settle();
};
const type = (testId: string, value: string) => {
  const input = find(testId) as HTMLInputElement;
  input.value = value;
  input.dispatchEvent(new Event("input"));
};

const AUTHORIZE = "https://hub.acme.test:8443/hub/authorize?state=s";

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  mocks.get.mockReset();
  mocks.start.mockReset().mockResolvedValue({ flow_id: "f1", authorize_url: AUTHORIZE });
  mocks.status.mockReset().mockResolvedValue({ status: "waiting", detail: null, connection: null });
  mocks.cancel.mockReset().mockResolvedValue(undefined);
  mocks.openExternal.mockReset().mockResolvedValue(undefined);
  vi.mocked(connectHub).mockReset();
  vi.mocked(ElMessage.error).mockReset();
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("HubPanel", () => {
  it("offers the hub's address and a sign-in through the browser first, with the password form behind a link", async () => {
    await mount(none);

    expect(find("hub-url")).not.toBeNull();
    expect(find("hub-device-name")).not.toBeNull();
    expect(find("hub-browser-sign-in")).not.toBeNull();
    expect(find("hub-username")).toBeNull();
    expect(find("hub-password")).toBeNull();
    expect(find("hub-connect")).toBeNull();
    expect(find("hub-status")).toBeNull();
    expect(find("hub-joined")).toBeNull();
    expect(text(find("hub-use-password"))).toBe("Use a username and password instead");

    await click("hub-use-password");

    expect(find("hub-username")).not.toBeNull();
    expect(find("hub-password")).not.toBeNull();
    expect(find("hub-connect")).not.toBeNull();
    expect(find("hub-browser-sign-in")).toBeNull();
    await click("hub-use-browser");
    expect(find("hub-browser-sign-in")).not.toBeNull();
  });

  it("signs in through the browser: opens the hub's page, waits with a Cancel, and shows the connection", async () => {
    vi.useFakeTimers();
    await mount(none);
    type("hub-url", "https://hub.acme.test:8443");
    await settle();
    await click("hub-browser-sign-in");

    expect(mocks.start).toHaveBeenCalledWith({
      hub_url: "https://hub.acme.test:8443",
      device_name: "ana-laptop",
    });
    expect(mocks.openExternal).toHaveBeenCalledWith(AUTHORIZE);
    expect(text(find("hub-browser-waiting"))).toContain(
      "Waiting for you to sign in in your browser…",
    );
    expect(find("hub-browser-reopen")?.getAttribute("href")).toBe(AUTHORIZE);
    expect(find("hub-browser-cancel")).not.toBeNull();
    expect(find("hub-browser-sign-in")).toBeNull();
    expect(find("hub-use-password")).toBeNull();

    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS);
    await settle();
    expect(mocks.status).toHaveBeenCalledWith("f1");
    expect(find("hub-browser-waiting")).not.toBeNull();

    mocks.status.mockResolvedValue({ status: "connected", detail: null, connection: desktop });
    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS);
    await settle();

    expect(find("hub-browser-waiting")).toBeNull();
    expect(text(find("hub-status"))).toContain("Connected to https://hub.acme.test:8443");
    expect(find("hub-disconnect")).not.toBeNull();
  });

  it("stops waiting on Cancel and tells core to close the sign-in", async () => {
    vi.useFakeTimers();
    await mount(none);
    type("hub-url", "https://hub.acme.test:8443");
    await settle();
    await click("hub-browser-sign-in");

    await click("hub-browser-cancel");

    expect(mocks.cancel).toHaveBeenCalledWith("f1");
    expect(find("hub-browser-waiting")).toBeNull();
    expect(find("hub-browser-sign-in")).not.toBeNull();
    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS * 3);
    expect(mocks.status).not.toHaveBeenCalled();
  });

  it("shows why a sign-in through the browser failed and offers it again", async () => {
    vi.useFakeTimers();
    mocks.status.mockResolvedValue({
      status: "failed",
      detail: "You declined the sign-in on the hub.",
      connection: null,
    });
    await mount(none);
    type("hub-url", "https://hub.acme.test:8443");
    await settle();
    await click("hub-browser-sign-in");
    await vi.advanceTimersByTimeAsync(BROWSER_POLL_MS);
    await settle();

    expect(ElMessage.error).toHaveBeenCalledWith("You declined the sign-in on the hub.");
    expect(find("hub-browser-waiting")).toBeNull();
    expect(find("hub-browser-sign-in")).not.toBeNull();
  });

  it("falls back to the password form, saying why, when the hub does not offer browser sign-in", async () => {
    const sentence =
      "The hub at https://old.acme.test does not offer signing in through the browser; use a username and password instead.";
    mocks.start.mockRejectedValue(httpError(400, { kind: "password_only", message: sentence }));
    await mount(none);
    type("hub-url", "https://old.acme.test");
    await settle();
    await click("hub-browser-sign-in");

    expect(text(find("hub-password-only"))).toBe(sentence);
    expect(find("hub-username")).not.toBeNull();
    expect(find("hub-connect")).not.toBeNull();
    expect(find("hub-use-browser")).toBeNull();
    expect(ElMessage.error).not.toHaveBeenCalled();
    expect(mocks.openExternal).not.toHaveBeenCalled();

    type("hub-url", "https://new.acme.test");
    await settle();
    expect(find("hub-password-only")).toBeNull();
    expect(find("hub-use-browser")).not.toBeNull();
  });

  it("shows the password form first, with no browser option, where this machine cannot sign in through a browser", async () => {
    await mount({ ...none, browser_sign_in: false });

    expect(find("hub-username")).not.toBeNull();
    expect(find("hub-connect")).not.toBeNull();
    expect(find("hub-browser-sign-in")).toBeNull();
    expect(find("hub-use-browser")).toBeNull();
    expect(find("hub-use-password")).toBeNull();
  });

  it("starts folded, and says coming soon only where no hub is connected", async () => {
    await mount(none);
    expect(find("hub-connection-toggle")!.getAttribute("aria-expanded")).toBe("false");
    expect(text(find("hub-summary"))).toBe("Not connected");
    expect(text(find("hub-coming-soon"))).toBe("Coming soon.");

    await mount(desktop);
    expect(find("hub-coming-soon")).toBeNull();
    await mount(server);
    expect(find("hub-coming-soon")).toBeNull();
  });

  it("connects with the password form as before when Enter is pressed, once every field it needs is filled", async () => {
    await mount(none);
    await click("hub-use-password");
    const enter = () =>
      find("hub-password")!.dispatchEvent(
        new KeyboardEvent("keyup", { key: "Enter", bubbles: true }),
      );

    type("hub-url", "https://hub.acme.test:8443");
    type("hub-username", "ana");
    enter();
    await settle();
    expect(connectHub).not.toHaveBeenCalled();

    type("hub-password", "secret");
    enter();
    await settle();
    expect(connectHub).toHaveBeenCalledTimes(1);
    expect(connectHub).toHaveBeenCalledWith(
      expect.objectContaining({
        hub_url: "https://hub.acme.test:8443",
        username: "ana",
        password: "secret",
      }),
    );
    expect(mocks.start).not.toHaveBeenCalled();
  });

  it("says which hub a desktop is connected to, as whom, with its version and when the session ends", async () => {
    await mount(desktop);

    const status = find("hub-status")!;
    expect(status.className).toContain("compute-card__status--success");
    expect(text(status)).toContain(
      "Connected to https://hub.acme.test:8443 as the device ana-laptop.",
    );
    const facts = Array.from(host.querySelectorAll(".hub-card__facts dt, .hub-card__facts dd")).map(
      text,
    );
    expect(facts.slice(0, 4)).toEqual(["Account", "Ana Lima", "Hub version", "0.3.0"]);
    expect(facts[4]).toBe("Session expires");
    expect(find("hub-disconnect")).not.toBeNull();
    expect(find("hub-url")).toBeNull();
  });

  it("says whose hub a joined server belongs to, and offers nothing to type or press", async () => {
    await mount(server);

    expect(text(find("hub-joined"))).toContain(
      "This server belongs to Acme Data's hub at https://hub.acme.test:8443.",
    );
    expect(text(find("hub-account"))).toBe("Signed in as Ana Lima.");
    expect(find("hub-url-link")?.getAttribute("href")).toBe("https://hub.acme.test:8443");
    expect(host.querySelector(".hub-card__facts")).toBeNull();
    expect(find("hub-url")).toBeNull();
    expect(find("hub-connect")).toBeNull();
    expect(find("hub-disconnect")).toBeNull();
    expect(find("hub-servers-link")).not.toBeNull();
  });

  it("names the hub by its address when it does not answer its name", async () => {
    await mount({ ...server, org_name: null });

    expect(text(find("hub-joined"))).toContain(
      "This server belongs to the hub at https://hub.acme.test:8443.",
    );
  });

  it("keeps saying so on a joined server whose hub session ended, with why, and still asks for no password", async () => {
    await mount({
      ...server,
      connected: false,
      reachable: false,
      account: null,
      error: "the hub ended this session",
    });

    const status = find("hub-joined")!;
    expect(status.className).toContain("compute-card__status--warning");
    expect(text(status)).toContain("This server belongs to Acme Data's hub");
    expect(text(status)).toContain("The hub is not answering: the hub ended this session");
    expect(find("hub-account")).toBeNull();
    expect(find("hub-url")).toBeNull();
    expect(find("hub-password")).toBeNull();
    expect(find("hub-connect")).toBeNull();
  });
});
