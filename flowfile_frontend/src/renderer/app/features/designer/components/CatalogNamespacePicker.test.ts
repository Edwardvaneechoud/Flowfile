// @vitest-environment happy-dom
import { createPinia } from "pinia";
import { describe, expect, it, vi } from "vitest";
import { ns } from "../../../__fixtures__/catalogTree";
import { mountIn, text } from "../../../__fixtures__/mount";

const tree = vi.hoisted(() => {
  // A module the picker imports reads localStorage as it loads.
  vi.stubGlobal("localStorage", {
    getItem: () => null,
    setItem: () => undefined,
    removeItem: () => undefined,
  });
  return { rows: [] as unknown[] };
});
vi.mock("../../../api", () => ({ CatalogApi: { getNamespaceTree: async () => tree.rows } }));
vi.mock("../../../services/axios.config", () => ({ default: {} }));

import CatalogNamespacePicker from "./CatalogNamespacePicker.vue";

describe("CatalogNamespacePicker", () => {
  it("browses only the catalogs core offers: a joined server's own is left out", async () => {
    tree.rows = [
      ns({ id: 1, name: "General", offered: false }),
      ns({ id: -3, name: "main", source: "hub", place: "Acme" }),
    ];

    const host = await mountIn(CatalogNamespacePicker, {
      props: { modelValue: null },
      pinia: createPinia(),
    });

    expect(text(host)).toContain("main");
    expect(text(host)).not.toContain("General");
  });
});
