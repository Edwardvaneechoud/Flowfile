// @vitest-environment happy-dom
// The card as an admin sees it: the store a run uses, where it comes from, and the fields only when it is this node's.
import { createApp, h, nextTick, type App, type FunctionalComponent } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ExchangeStoreState } from "../../api/system.api";

const mocks = vi.hoisted(() => ({
  get: vi.fn(),
  set: vi.fn(),
}));

vi.mock("../../api/system.api", () => ({
  getExchangeStore: mocks.get,
  setExchangeStore: mocks.set,
}));

vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: vi.fn() },
}));

import ExchangeStoreCard from "./ExchangeStoreCard.vue";

const element = (tag: string): FunctionalComponent => {
  const stub: FunctionalComponent = (_, { attrs, slots }) => h(tag, attrs, slots.default?.());
  stub.inheritAttrs = false;
  return stub;
};

const stubs: Record<string, FunctionalComponent> = {
  "el-collapse-transition": (_, { slots }) => slots.default?.(),
  "el-button": element("button"),
  "el-input": element("input"),
  "el-input-number": element("input"),
  "el-select": element("select"),
  "el-option": element("option"),
};

const none: ExchangeStoreState = {
  uri: null,
  connection: null,
  maxRows: null,
  maxRowsError: null,
  source: "none",
  envOverride: false,
  saved: { uri: null, connection: null, maxRows: null },
  hub: null,
  connections: ["exchange"],
};

const hubStore = { uri: "s3://org-bucket/exchange", connection: "org-exchange", maxRows: 5000 };

const fromHub: ExchangeStoreState = {
  ...none,
  ...hubStore,
  source: "hub",
  hub: hubStore,
};

const saved: ExchangeStoreState = {
  ...none,
  uri: "s3://node-bucket/flowfile",
  connection: "exchange",
  maxRows: 1000,
  source: "saved",
  saved: { uri: "s3://node-bucket/flowfile", connection: "exchange", maxRows: 1000 },
};

const fromEnv: ExchangeStoreState = {
  ...none,
  uri: "s3://env-bucket/flowfile",
  connection: "exchange",
  source: "env",
  envOverride: true,
};

const settle = async () => {
  for (let i = 0; i < 5; i++) await Promise.resolve();
  await nextTick();
};

const text = (el: Element | null) => el?.textContent?.replace(/\s+/g, " ").trim() ?? "";

let app: App | null = null;
let host: HTMLElement;

const mount = async (state: ExchangeStoreState) => {
  mocks.get.mockResolvedValue(state);
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp(ExchangeStoreCard);
  for (const [name, stub] of Object.entries(stubs)) app.component(name, stub);
  app.mount(host);
  await settle();
};

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);

const summary = async () => {
  (find("exchange-store-toggle") as HTMLButtonElement).click();
  await nextTick();
  return text(find("exchange-store-summary"));
};

beforeEach(() => {
  mocks.get.mockReset();
  mocks.set.mockReset();
});

afterEach(() => {
  app?.unmount();
  app = null;
  host.remove();
});

describe("ExchangeStoreCard", () => {
  it("says the hub sets the store, shows it and offers nothing to edit", async () => {
    await mount(fromHub);

    const status = text(find("exchange-store-status"));
    expect(status).toContain("Set by your hub");
    expect(status).toContain(
      "Results cross through s3://org-bucket/exchange with the connection org-exchange, at most 5000 rows per input.",
    );
    expect(find("exchange-store-status")?.className).toContain("exchange-card__status--success");
    expect(host.querySelector("input, select")).toBeNull();
    expect(find("exchange-store-save")).toBeNull();
    expect(find("exchange-store-clear")).toBeNull();
    expect(find("exchange-store-hub")).toBeNull();
    expect(await summary()).toBe("Set by your hub");
  });

  it("leaves the row limit out of a hub's store that has none", async () => {
    const unlimited = { ...hubStore, maxRows: null };
    await mount({ ...fromHub, ...unlimited, hub: unlimited });

    const status = text(find("exchange-store-status"));
    expect(status).toContain("Set by your hub");
    expect(status).toContain(
      "Results cross through s3://org-bucket/exchange with the connection org-exchange.",
    );
  });

  it("still says why the environment's row limit refuses every crossing under the hub's store", async () => {
    const detail = "FLOWFILE_EXCHANGE_MAX_ROWS must be a whole number of rows, not 'ten'";
    await mount({ ...fromHub, maxRows: null, maxRowsError: detail });

    expect(text(find("exchange-store-status"))).toContain("Set by your hub");
    expect(find("exchange-store-status")?.className).toContain("exchange-card__status--warning");
    expect(text(find("exchange-store-max-rows-error"))).toContain(detail);
    expect(host.querySelector("input, select")).toBeNull();
    expect(await summary()).toBe("Row limit needs attention");
  });

  it("names the hub's store in one line when the node's own store wins", async () => {
    await mount({ ...saved, hub: hubStore });

    expect(text(find("exchange-store-hub"))).toBe(
      "Your hub sets s3://org-bucket/exchange. Clear this store to use it.",
    );
    expect(text(find("exchange-store-status"))).toContain("Exchange store configured");
    expect(find("exchange-store-uri")).not.toBeNull();
    expect(find("exchange-store-connection")).not.toBeNull();
    expect(find("exchange-store-max-rows")).not.toBeNull();
    expect(find("exchange-store-save")).not.toBeNull();
    expect(find("exchange-store-clear")).not.toBeNull();
    expect(await summary()).toBe("Configured");
  });

  it("shows no hub line for a saved store when no hub sets one", async () => {
    await mount(saved);

    expect(find("exchange-store-hub")).toBeNull();
    expect(text(find("exchange-store-status"))).toContain("Exchange store configured");
    expect(find("exchange-store-save")).not.toBeNull();
    expect(find("exchange-store-clear")).not.toBeNull();
  });

  it("offers the fields when nothing is configured", async () => {
    await mount(none);

    expect(text(find("exchange-store-status"))).toContain("Not configured");
    expect(find("exchange-store-uri")).not.toBeNull();
    expect(find("exchange-store-save")).not.toBeNull();
    expect(find("exchange-store-clear")).toBeNull();
    expect(find("exchange-store-hub")).toBeNull();
    expect(await summary()).toBe("Not configured");
  });

  it("keeps the environment's override as it is, hub or not", async () => {
    await mount({ ...fromEnv, hub: hubStore });

    expect(text(find("exchange-store-status"))).toContain(
      "Results cross through s3://env-bucket/flowfile with the connection exchange (from the environment).",
    );
    expect(find("exchange-store-env")).not.toBeNull();
    expect(find("exchange-store-hub")).toBeNull();
    expect(find("exchange-store-uri")).not.toBeNull();
    expect(find("exchange-store-save")).not.toBeNull();
    expect(await summary()).toBe("Configured");
  });
});
