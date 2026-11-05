// @vitest-environment happy-dom
// The catalog panel's Hub badge follows the tree's rule: none on a server whose every catalog is the hub's, one on a
// hub catalog shown beside this machine's, never on this machine's own.
import { createApp, h, nextTick, type App, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { NamespaceTree } from "../../types/catalog.types";
import type { HubPresence } from "./hubRows";

vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: vi.fn() },
  ElMessageBox: { confirm: vi.fn() },
}));
vi.mock("../../services/axios.config", () => ({ default: {} }));
vi.mock("../../api/catalog.api", () => ({ CatalogApi: { deleteNamespace: vi.fn() } }));
vi.mock("../CloudConnectionView/api", () => ({
  fetchCloudStorageConnectionsInterfaces: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canManage: () => false,
    canManageGrants: () => false,
    isShared: () => false,
    sharedLabel: () => "",
    sharedTitle: () => "",
  }),
}));
vi.mock("./EditCatalogStorageModal.vue", () => ({ default: { render: () => null } }));

import NamespaceDetailPanel from "./NamespaceDetailPanel.vue";

const element = (tag: string): FunctionalComponent => {
  const stub: FunctionalComponent = (_, { attrs, slots }) => h(tag, attrs, slots.default?.());
  stub.inheritAttrs = false;
  return stub;
};

const stubs: Record<string, FunctionalComponent> = {
  "el-tooltip": (_, { slots }) => h("span", slots.default?.()),
  "el-button": element("button"),
};

const catalog = (over: Partial<NamespaceTree> = {}): NamespaceTree =>
  ({
    id: 1,
    name: "Sales",
    level: 0,
    parent_id: null,
    storage_uri: "s3://acme/data",
    storage_connection_name: null,
    children: [],
    tables: [],
    flows: [],
    artifacts: [],
    visualizations: [],
    notebooks: [],
    source: "local",
    ...over,
  }) as unknown as NamespaceTree;

const hubCatalog = catalog({ id: -10, name: "main", source: "hub", hub_id: 10 });

let app: App | null = null;
let host: HTMLElement;

const mount = async (namespace: NamespaceTree, hubPresence: HubPresence) => {
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp(NamespaceDetailPanel, { namespace, hubPresence });
  app.use(createPinia());
  for (const [name, stub] of Object.entries(stubs)) app.component(name, stub);
  app.mount(host);
  await nextTick();
};

const badge = () => host.querySelector('[data-testid="hub-badge"]');

afterEach(() => {
  app?.unmount();
  app = null;
  host?.remove();
});

describe("NamespaceDetailPanel", () => {
  it("badges a hub catalog shown beside this machine's", async () => {
    await mount(hubCatalog, "beside");

    expect(badge()).not.toBeNull();
    expect(host.querySelector('[data-testid="hub-managed-note"]')).not.toBeNull();
  });

  it("badges nothing on a server whose every catalog is the hub's", async () => {
    await mount(hubCatalog, "only");

    expect(badge()).toBeNull();
    expect(host.querySelector('[data-testid="hub-managed-note"]')).not.toBeNull();
  });

  it("badges none of this machine's own, with or without a hub", async () => {
    await mount(catalog(), "beside");
    expect(badge()).toBeNull();
    app?.unmount();
    host.remove();

    await mount(catalog(), "none");
    expect(badge()).toBeNull();
  });
});
