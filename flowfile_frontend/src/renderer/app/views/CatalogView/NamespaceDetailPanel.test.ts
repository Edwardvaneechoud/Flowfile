// @vitest-environment happy-dom
// The catalog panel says a hub catalog is managed on the hub, and says nothing of the kind for this machine's own.
import { h, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { describe, expect, it, vi } from "vitest";
import { ns } from "../../__fixtures__/catalogTree";
import { element, mountIn } from "../../__fixtures__/mount";
import type { NamespaceTree } from "../../types/catalog.types";

vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: vi.fn() },
  ElMessageBox: { confirm: vi.fn() },
}));
vi.mock("../../services/axios.config", () => ({ default: {} }));
const { syncMock } = vi.hoisted(() => ({ syncMock: vi.fn() }));
vi.mock("../../api/catalog.api", () => ({
  CatalogApi: {
    deleteNamespace: vi.fn(),
    syncExternalCatalog: syncMock,
    getNamespaceTree: vi.fn().mockResolvedValue([]),
  },
}));
vi.mock("../CloudConnectionView/api", () => ({
  fetchCloudStorageConnectionsInterfaces: vi.fn().mockResolvedValue([]),
}));
const sharing = vi.hoisted(() => ({ owned: false }));

vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canManage: () => sharing.owned,
    canManageGrants: () => sharing.owned,
    isShared: () => false,
    sharedLabel: () => "",
    sharedTitle: () => "",
  }),
}));
vi.mock("./EditCatalogStorageModal.vue", () => ({ default: { render: () => null } }));
vi.mock("./ExternalCatalogModal.vue", () => ({ default: { render: () => null } }));

import NamespaceDetailPanel from "./NamespaceDetailPanel.vue";

const stubs: Record<string, FunctionalComponent> = {
  "el-tooltip": (_, { slots }) => h("span", slots.default?.()),
  "el-button": element("button"),
};

const storage_uri = "s3://acme/data";
const catalog = ns({ storage_uri });
const hubCatalog = ns({ storage_uri, id: -10, name: "main", source: "hub" });

let host: HTMLElement;

const mount = async (namespace: NamespaceTree) => {
  host = await mountIn(NamespaceDetailPanel, {
    props: { namespace },
    pinia: createPinia(),
    stubs,
  });
};

const note = () => host.querySelector('[data-testid="hub-managed-note"]');

describe("NamespaceDetailPanel", () => {
  it("notes that a hub catalog is managed on the hub, and not this machine's own", async () => {
    await mount(hubCatalog);
    expect(note()).not.toBeNull();

    await mount(catalog);
    expect(note()).toBeNull();
  });

  it("lists only the schemas with a model to open, and no section for a catalog without one", async () => {
    const schema = (id: number, name: string, readable: boolean) =>
      ns({ id, name, level: 1, parent_id: 1, semantic_model_readable: readable });
    const section = () => host.querySelector('[data-testid="catalog-semantic-section"]');
    const listed = () =>
      Array.from(host.querySelectorAll('[data-testid="catalog-semantic-schema"] .name')).map(
        (el) => el.textContent,
      );

    await mount({ ...catalog, children: [schema(2, "mine", true), schema(3, "public", false)] });
    expect(section()).not.toBeNull();
    expect(listed()).toEqual(["mine"]);

    await mount({
      ...catalog,
      semantic_model_readable: false,
      children: [schema(3, "public", false)],
    });
    expect(section()).toBeNull();
  });

  it("offers Delete and Edit storage only where the person manages the catalog", async () => {
    sharing.owned = true;
    const actions = () =>
      ["catalog-delete", "catalog-edit-storage"].filter(
        (id) => host.querySelector(`[data-testid="${id}"]`) !== null,
      );
    try {
      await mount(catalog);
      expect(actions()).toEqual(["catalog-delete", "catalog-edit-storage"]);

      await mount({ ...catalog, can_manage: false });
      expect(actions()).toEqual([]);
    } finally {
      sharing.owned = false;
    }
  });
});

describe("NamespaceDetailPanel for a catalog kept elsewhere", () => {
  const external = {
    kind: "iceberg_rest",
    uri: "http://127.0.0.1:18191/api/catalog",
    warehouse: "signed",
    sign_in: "client_credentials" as const,
    client_id: "flowfile",
    scope: "PRINCIPAL_ROLE:ALL",
    token_endpoint: null,
    synced_at: null,
    sync_error: "No catalog answers at http://127.0.0.1:18191/api/catalog.",
    not_shown: 2,
  };

  it("shows how it is reached and why it is behind, no storage editor and no secret", async () => {
    syncMock.mockReset().mockResolvedValue({
      added: [],
      gone: [],
      back: [],
      not_shown: 2,
      synced_at: null,
      skipped: true,
      error: null,
    });
    await mount(ns({ id: 7, name: "polaris", kept_elsewhere: true, external }));

    const panel = (host.querySelector('[data-testid="external-panel"]')?.textContent ?? "").replace(
      /\s+/g,
      " ",
    );
    expect(panel).toContain(external.uri);
    expect(panel).toContain("A client id and secret (flowfile)");
    expect(host.querySelector('[data-testid="catalog-edit-storage"]')).toBeNull();
    expect(host.querySelector('[data-testid="catalog-delete"]')).toBeNull();
    expect(host.querySelector('[data-testid="external-sync-error"]')?.textContent).toContain(
      "No catalog answers",
    );
    expect(host.textContent).toContain("2 namespaces of this catalog are not shown");
    expect(host.textContent).not.toMatch(/secret:|client_secret/);
    expect(syncMock).toHaveBeenCalledWith(7, false);
  });
});
