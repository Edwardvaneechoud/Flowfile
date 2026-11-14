// @vitest-environment happy-dom
// The sign-in page as the server's sign-in method shapes it: a password form, or one button to the hub.
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mountIn, text } from "../../__fixtures__/mount";

const mocks = vi.hoisted(() => ({
  getSignInMethod: vi.fn(),
  takeHubSignInError: vi.fn(),
  startHubSignIn: vi.fn(),
  replace: vi.fn(),
  push: vi.fn(),
  login: vi.fn(),
  query: {} as Record<string, string>,
}));

vi.mock("../../services/auth.service", () => ({ default: mocks }));
vi.mock("../../stores/auth-store", () => ({
  useAuthStore: () => ({ login: mocks.login, authError: null }),
}));
vi.mock("vue-router", () => ({
  useRouter: () => ({ push: mocks.push, replace: mocks.replace }),
  useRoute: () => ({ query: mocks.query }),
}));

import LoginView from "./LoginView.vue";

let host: HTMLElement;

const mount = async () => {
  host = await mountIn(LoginView);
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
  mocks.startHubSignIn.mockReset();
  mocks.replace.mockReset().mockResolvedValue(undefined);
  mocks.push.mockReset().mockResolvedValue(undefined);
  mocks.login.mockReset().mockResolvedValue(true);
  mocks.query = {};
});

describe("LoginView", () => {
  it("sends people to the hub with one button and asks for no password on a server that joined one", async () => {
    mocks.getSignInMethod.mockResolvedValue(throughHub);

    await mount();

    const button = find("login-hub") as HTMLAnchorElement | null;
    expect(text(button)).toBe("Sign in with hub.example.com:8443");
    expect(button?.getAttribute("href")).toBe("https://hub.example.com:8443/open/12");
    expect(host.querySelector("form, input")).toBeNull();
    expect(find("login-error")).toBeNull();
  });

  it("starts the sign-in through the hub from the button, which keeps a state for its return", async () => {
    mocks.getSignInMethod.mockResolvedValue(throughHub);
    await mount();
    expect(mocks.startHubSignIn).not.toHaveBeenCalled();

    const click = new MouseEvent("click", { bubbles: true, cancelable: true });
    find("login-hub")?.dispatchEvent(click);

    expect(click.defaultPrevented).toBe(true);
    expect(mocks.startHubSignIn).toHaveBeenCalledWith(
      "https://hub.example.com:8443/open/12",
      undefined,
    );
  });

  it("starts the sign-in through the hub at once at the sign-in start, `#/login?hub_signin=1`", async () => {
    mocks.query = { hub_signin: "1" };
    mocks.getSignInMethod.mockResolvedValue(throughHub);

    await mount();

    expect(mocks.startHubSignIn).toHaveBeenCalledTimes(1);
    expect(mocks.startHubSignIn).toHaveBeenCalledWith(
      "https://hub.example.com:8443/open/12",
      undefined,
    );
    expect(mocks.replace).not.toHaveBeenCalled();
  });

  it("brings the sign-in through the hub back to the page that asked for it", async () => {
    mocks.query = { redirect: "/main/catalog?tab=runs&runId=17" };
    mocks.getSignInMethod.mockResolvedValue(throughHub);
    await mount();

    (find("login-hub") as HTMLElement).click();

    expect(mocks.startHubSignIn).toHaveBeenCalledWith(
      "https://hub.example.com:8443/open/12",
      "/main/catalog?tab=runs&runId=17",
    );
  });

  it.each([
    [
      "the page that asked for it",
      "/main/catalog?tab=runs&runId=17",
      "/main/catalog?tab=runs&runId=17",
    ],
    ["home when no page asked", undefined, { name: "home" }],
    ["home for an address outside this app", "//elsewhere.example.com", { name: "home" }],
  ])("opens %s after a password sign-in", async (_case, redirect, destination) => {
    mocks.query = redirect ? { redirect } : {};
    mocks.getSignInMethod.mockResolvedValue({ method: "password" });
    await mount();
    const input = (selector: string, value: string) => {
      const field = host.querySelector(selector) as HTMLInputElement;
      field.value = value;
      field.dispatchEvent(new Event("input"));
    };
    input("#username", "alice");
    input("#password", "secret");
    await new Promise((resolve) => setTimeout(resolve));

    host.querySelector("form")!.dispatchEvent(new Event("submit", { cancelable: true }));
    await new Promise((resolve) => setTimeout(resolve));

    expect(mocks.login).toHaveBeenCalledWith("alice", "secret");
    expect(mocks.push).toHaveBeenCalledWith(destination);
  });

  it("starts nothing at a plain `#/login`", async () => {
    mocks.getSignInMethod.mockResolvedValue(throughHub);

    await mount();

    expect(mocks.startHubSignIn).not.toHaveBeenCalled();
    expect(mocks.replace).not.toHaveBeenCalled();
  });

  it("asks for a password at the sign-in start on a server that joined no hub, and drops the flag", async () => {
    mocks.query = { hub_signin: "1" };
    mocks.getSignInMethod.mockResolvedValue({ method: "password" });

    await mount();

    expect(mocks.startHubSignIn).not.toHaveBeenCalled();
    expect(mocks.replace).toHaveBeenCalledWith({ path: undefined, query: {} });
    expect(host.querySelector("form #username")).not.toBeNull();
    expect(find("login-hub")).toBeNull();
  });

  it("shows the password form at once, before the server answers how it signs people in", async () => {
    mocks.getSignInMethod.mockReturnValue(new Promise(() => undefined));

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
