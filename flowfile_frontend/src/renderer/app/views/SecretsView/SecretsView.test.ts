// @vitest-environment happy-dom
// The Secrets page knows a server that joined its hub from the hub connection the editor already reads, and only
// there says the hub keeps the secrets, offers Change and hides Share.
import { createPinia, setActivePinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { none, server } from "../../__fixtures__/hubConnection";
import { fakeStorage, mountIn } from "../../__fixtures__/mount";
import { useHubConnectionStore } from "../../stores/hub-connection-store";

const mocks = vi.hoisted(() => ({
  getHubConnection: vi.fn(),
  getSignInMethod: vi.fn(),
}));

vi.mock("../../api/hubConnection.api", () => ({ getHubConnection: mocks.getHubConnection }));
vi.mock("../../api/secrets.api", () => ({ SecretsApi: {} }));
vi.mock("./secretApi", () => ({ fetchSecretsApi: async () => [{ name: "warehouse", id: 1 }] }));
vi.mock("../../services/auth.service", () => ({
  default: { getSignInMethod: mocks.getSignInMethod },
  authService: { getSignInMethod: mocks.getSignInMethod },
}));
vi.mock("../../composables/useResourceSharing", async () => {
  const { ref } = await import("vue");
  return { useResourceSharing: () => ({ isMultiUser: ref(true), isOwned: () => true }) };
});
vi.mock("../../components/sharing/ShareDialog.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/SharedBadge.vue", () => ({ default: { render: () => null } }));

import SecretsView from "./SecretsView.vue";

const HUB_LINE = "This server keeps your secrets on its hub";

describe("SecretsView", () => {
  let host: HTMLElement;
  let pinia: ReturnType<typeof createPinia>;

  beforeEach(() => {
    const storage = fakeStorage();
    storage.setItem("secrets-view-mode", "manage");
    vi.stubGlobal("localStorage", storage);
    pinia = createPinia();
    setActivePinia(pinia);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  const mount = async () => {
    host = await mountIn(SecretsView, { pinia });
  };

  const button = (label: string) => host.querySelector(`[aria-label="${label} secret warehouse"]`);

  it("says the hub keeps a joined server's secrets, without asking how it signs people in", async () => {
    mocks.getHubConnection.mockResolvedValue(server);

    await mount();

    expect(mocks.getSignInMethod).not.toHaveBeenCalled();
    expect(mocks.getHubConnection).toHaveBeenCalledTimes(1);
    expect(host.textContent).toContain(HUB_LINE);
    expect(button("Change")).not.toBeNull();
    expect(button("Share")).toBeNull();
  });

  it("keeps Share and says nothing of a hub on a server that joined none", async () => {
    mocks.getHubConnection.mockResolvedValue(none);

    await mount();

    expect(mocks.getSignInMethod).not.toHaveBeenCalled();
    expect(host.textContent).not.toContain(HUB_LINE);
    expect(button("Change")).toBeNull();
    expect(button("Share")).not.toBeNull();
  });

  it("reads the connection the editor already loaded", async () => {
    mocks.getHubConnection.mockResolvedValue(server);
    await useHubConnectionStore().load();

    await mount();

    expect(mocks.getHubConnection).toHaveBeenCalledTimes(1);
    expect(host.textContent).toContain(HUB_LINE);
  });
});
