// @vitest-environment happy-dom
// The sign-in page as the server's sign-in method shapes it: a password form, or one button to the hub.
import { createApp, nextTick, type App } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  getSignInMethod: vi.fn(),
  takeHubSignInError: vi.fn(),
}));

vi.mock("../../services/auth.service", () => ({ default: mocks }));
vi.mock("../../stores/auth-store", () => ({
  useAuthStore: () => ({ login: vi.fn(), authError: null }),
}));
vi.mock("vue-router", () => ({ useRouter: () => ({ push: vi.fn() }) }));

import LoginView from "./LoginView.vue";

const text = (el: Element | null) => el?.textContent?.replace(/\s+/g, " ").trim() ?? "";

let app: App | null = null;
let host: HTMLElement;

const mount = async () => {
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp(LoginView);
  app.mount(host);
  for (let i = 0; i < 5; i++) await Promise.resolve();
  await nextTick();
};

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);

const throughHub = {
  method: "hub",
  url: "https://hub.example.com:8443/open/12",
  hub_url: "https://hub.example.com:8443",
};

beforeEach(() => {
  mocks.getSignInMethod.mockReset();
  mocks.takeHubSignInError.mockReset().mockReturnValue(null);
});

afterEach(() => {
  app?.unmount();
  app = null;
  host.remove();
});

describe("LoginView", () => {
  it("sends people to the hub with one button and asks for no password on a server that joined one", async () => {
    mocks.getSignInMethod.mockResolvedValue(throughHub);

    await mount();

    const button = find("login-hub") as HTMLAnchorElement | null;
    expect(text(button)).toBe("Sign in with hub.example.com");
    expect(button?.getAttribute("href")).toBe("https://hub.example.com:8443/open/12");
    expect(host.querySelector("form, input")).toBeNull();
    expect(find("login-error")).toBeNull();
  });

  it("asks for a username and password on a server that signs people in itself", async () => {
    mocks.getSignInMethod.mockResolvedValue({ method: "password" });

    await mount();

    expect(host.querySelector("form #username")).not.toBeNull();
    expect(host.querySelector("form input[type=password]")).not.toBeNull();
    expect(find("login-hub")).toBeNull();
  });

  it("shows the server's sentence for a sign-in through the hub it refused", async () => {
    const sentence = "The hub refused this sign-in: this sign-in token was already used.";
    mocks.takeHubSignInError.mockReturnValue(sentence);
    mocks.getSignInMethod.mockResolvedValue(throughHub);

    await mount();

    expect(text(find("login-error"))).toBe(sentence);
    expect(find("login-hub")).not.toBeNull();
  });
});
