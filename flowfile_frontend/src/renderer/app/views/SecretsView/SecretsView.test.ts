// @vitest-environment happy-dom
// The Secrets page knows a server that joined its hub from the hub connection the editor already reads, and only
// there lists the secrets kept on this server.
import { createPinia, setActivePinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { none, server } from "../../__fixtures__/hubConnection";
import { fakeStorage, mountIn } from "../../__fixtures__/mount";
import { useHubConnectionStore } from "../../stores/hub-connection-store";

const mocks = vi.hoisted(() => ({
  getHubConnection: vi.fn(),
  getKept: vi.fn(),
  getSignInMethod: vi.fn(),
}));

vi.mock("../../api/hubConnection.api", () => ({ getHubConnection: mocks.getHubConnection }));
vi.mock("../../api/secrets.api", () => ({ SecretsApi: { getKept: mocks.getKept } }));
vi.mock("./secretApi", () => ({ fetchSecretsApi: async () => [] }));
vi.mock("../../services/auth.service", () => ({
  default: { getSignInMethod: mocks.getSignInMethod },
  authService: { getSignInMethod: mocks.getSignInMethod },
}));
vi.mock("../../composables/useResourceSharing", async () => {
  const { ref } = await import("vue");
  return { useResourceSharing: () => ({ isMultiUser: ref(false), isOwned: () => true }) };
});
vi.mock("../../components/sharing/ShareDialog.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/SharedBadge.vue", () => ({ default: { render: () => null } }));

import SecretsView from "./SecretsView.vue";

describe("SecretsView", () => {
  let host: HTMLElement;
  let pinia: ReturnType<typeof createPinia>;

  beforeEach(() => {
    const storage = fakeStorage();
    storage.setItem("secrets-view-mode", "manage");
    vi.stubGlobal("localStorage", storage);
    pinia = createPinia();
    setActivePinia(pinia);
    mocks.getKept.mockResolvedValue([{ name: "warehouse", reason: "Used by a schedule here" }]);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  const mount = async () => {
    host = await mountIn(SecretsView, { pinia });
  };

  it("lists the secrets kept on a joined server without asking how it signs people in", async () => {
    mocks.getHubConnection.mockResolvedValue(server);

    await mount();

    expect(mocks.getSignInMethod).not.toHaveBeenCalled();
    expect(mocks.getHubConnection).toHaveBeenCalledTimes(1);
    expect(mocks.getKept).toHaveBeenCalledTimes(1);
    expect(host.textContent).toContain("Kept on this server (1)");
  });

  it("asks for no kept secrets on a server that joined no hub", async () => {
    mocks.getHubConnection.mockResolvedValue(none);

    await mount();

    expect(mocks.getSignInMethod).not.toHaveBeenCalled();
    expect(mocks.getKept).not.toHaveBeenCalled();
    expect(host.textContent).not.toContain("Kept on this server");
  });

  it("reads the connection the editor already loaded", async () => {
    mocks.getHubConnection.mockResolvedValue(server);
    await useHubConnectionStore().load();

    await mount();

    expect(mocks.getHubConnection).toHaveBeenCalledTimes(1);
    expect(mocks.getKept).toHaveBeenCalledTimes(1);
  });
});
