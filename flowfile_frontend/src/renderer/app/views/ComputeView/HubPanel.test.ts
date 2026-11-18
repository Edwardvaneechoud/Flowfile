// @vitest-environment happy-dom
// The Hub card as an administrator sees it: the connect form, the connected desktop's facts, and on a server
// that joined its hub the line saying whose hub it belongs to, with nothing to type.
import { h, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { desktop, none, server } from "../../__fixtures__/hubConnection";
import { element, fakeStorage, mountIn, settle, text } from "../../__fixtures__/mount";
import type { HubConnection } from "../../api/hubConnection.api";

const mocks = vi.hoisted(() => ({ get: vi.fn() }));

vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: mocks.get,
  connectHub: vi.fn(),
  disconnectHub: vi.fn(),
}));
vi.mock("element-plus", () => ({ ElMessage: { success: vi.fn(), error: vi.fn() } }));

import { connectHub } from "../../api/hubConnection.api";
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

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  mocks.get.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("HubPanel", () => {
  it("offers the hub's address and an account to a desktop with no hub, and nothing else", async () => {
    await mount(none);

    expect(find("hub-url")).not.toBeNull();
    expect(find("hub-username")).not.toBeNull();
    expect(find("hub-password")).not.toBeNull();
    expect(find("hub-connect")).not.toBeNull();
    expect(find("hub-status")).toBeNull();
    expect(find("hub-joined")).toBeNull();
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

  it("connects when Enter is pressed in the form, once every field it needs is filled", async () => {
    await mount(none);
    const type = (testId: string, value: string) => {
      const input = find(testId) as HTMLInputElement;
      input.value = value;
      input.dispatchEvent(new Event("input"));
    };
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
