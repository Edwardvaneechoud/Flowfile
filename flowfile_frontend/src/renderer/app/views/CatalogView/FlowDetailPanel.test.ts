// @vitest-environment happy-dom
// A flow of a remote catalog (a hub row) has the page a local registered flow has: its run history, its schedules
// with enable, disable and delete, and Favourite, each through core's own catalog route by the id its row carries.
// It also has Run, Add schedule, a schedule's Run now and Cancel; left out for it: what needs the flow's file here.
import { h, type FunctionalComponent } from "vue";
import { createPinia, setActivePinia } from "pinia";
import { describe, expect, it, vi } from "vitest";
import { flow } from "../../__fixtures__/catalogTree";
import { element, mountIn, text } from "../../__fixtures__/mount";
import type { FlowRegistration, FlowSchedule } from "../../types";

const api = vi.hoisted(() => ({ updateSchedule: vi.fn(), getSchedules: vi.fn() }));

vi.mock("../../api/catalog.api", () => ({ CatalogApi: api }));
vi.mock("../../services/setup.service", () => ({ default: { computes: () => true } }));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canShare: () => true,
    canManage: () => true,
    canManageGrants: () => true,
  }),
}));
vi.mock("./RunHistoryTable.vue", () => ({
  default: {
    props: ["registrationId"],
    render() {
      return h("div", {
        "data-testid": "run-history",
        "data-registration": (this as any).registrationId,
      });
    },
  },
}));
vi.mock("./components/ScheduleTable.vue", () => ({
  default: {
    props: ["schedules", "disableRunNow"],
    emits: ["toggleSchedule"],
    render() {
      const self = this as any;
      return h(
        "button",
        {
          "data-testid": "schedules",
          "data-run-now-off": String(self.disableRunNow),
          onClick: () => self.$emit("toggleSchedule", self.schedules[0].id, false),
        },
        String(self.schedules.length),
      );
    },
  },
}));
vi.mock("./ApiEndpointPanel.vue", () => ({
  default: { render: () => h("div", { "data-testid": "api-panel" }) },
}));
vi.mock("../../components/sharing/ShareDialog.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/SharedBadge.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/common", () => ({
  CollapsibleSection: {
    props: ["title"],
    render() {
      const self = this as any;
      return h("section", { "data-section": self.title }, [
        self.$slots.actions?.(),
        self.$slots.default?.(),
      ]);
    },
  },
  EmptyState: { render: () => null },
}));

import FlowDetailPanel from "./FlowDetailPanel.vue";
import { useCatalogStore } from "../../stores/catalog-store";

const stubs: Record<string, FunctionalComponent> = {
  "el-button": element("button"),
  "el-dropdown": (_, { slots }) => h("span", slots.default?.()),
  "el-dropdown-menu": () => null,
  "el-dropdown-item": () => null,
};

const hubFlow = flow({
  id: -201,
  name: "orders-clean",
  source: "hub",
  flow_path: "hub://flows/u-1",
});

const mount = async (row: FlowRegistration) => {
  const pinia = createPinia();
  setActivePinia(pinia);
  const store = useCatalogStore();
  store.flowSchedules = [{ id: -7, registration_id: row.id, enabled: true } as FlowSchedule];
  const favourited = vi.fn();
  const host = await mountIn(FlowDetailPanel, {
    props: { flow: row, artifacts: [], onToggleFavorite: favourited },
    pinia,
    stubs,
  });
  return { host, favourited };
};

const buttons = (host: HTMLElement) =>
  Array.from(host.querySelectorAll(".header-actions button")).map((button) => text(button));

describe("FlowDetailPanel", () => {
  it("shows a hub flow its run history, its schedules and Favourite, by the id its row carries", async () => {
    const { host, favourited } = await mount(hubFlow);

    expect(
      host.querySelector("[data-testid='run-history']")?.getAttribute("data-registration"),
    ).toBe("-201");
    expect(text(host.querySelector("[data-testid='schedules']"))).toBe("1");
    expect(buttons(host)).toEqual(["Run Flow", "Favorite", "Share", "Delete"]);
    (host.querySelectorAll(".header-actions button")[1] as HTMLButtonElement).click();
    expect(favourited).toHaveBeenCalledWith(-201);
  });

  it("pauses a hub flow's schedule through the schedule route a local one uses", async () => {
    api.updateSchedule.mockResolvedValue({});
    api.getSchedules.mockResolvedValue([]);
    const { host } = await mount(hubFlow);

    (host.querySelector("[data-testid='schedules']") as HTMLButtonElement).click();
    expect(api.updateSchedule).toHaveBeenCalledWith(-7, { enabled: false });
  });

  it("gives a hub flow Run, Add schedule, Run now and Cancel", async () => {
    const pinia = createPinia();
    setActivePinia(pinia);
    const store = useCatalogStore();
    store.flowSchedules = [{ id: -7, registration_id: -201, enabled: true } as FlowSchedule];
    store.activeRuns = [{ id: -9, registration_id: -201 } as any];
    const running = await mountIn(FlowDetailPanel, {
      props: { flow: hubFlow, artifacts: [] },
      pinia,
      stubs,
    });
    expect(buttons(running)).toContain("Cancel Run");

    const { host } = await mount(hubFlow);
    expect(buttons(host)).toContain("Run Flow");
    expect(text(host.querySelector("[data-section='Schedules'] button:not([data-testid])"))).toBe(
      "Add",
    );
    const schedules = host.querySelector("[data-testid='schedules']");
    expect(schedules?.getAttribute("data-run-now-off")).toBe("false");
  });

  it("gives a hub flow Rename and Delete through core's routes by its own id", async () => {
    const pinia = createPinia();
    setActivePinia(pinia);
    const deleted = vi.fn();
    const renamed = vi.fn();
    const host = await mountIn(FlowDetailPanel, {
      props: { flow: hubFlow, artifacts: [], onDeleteFlow: deleted, onRenameFlow: renamed },
      pinia,
      stubs,
    });
    (host.querySelector("[title='Delete flow']") as HTMLButtonElement).click();
    expect(deleted).toHaveBeenCalledWith(-201);
    expect(host.querySelector("[title='Rename']")).not.toBeNull();
  });

  it("gives a hub flow Share and leaves out what needs the file here, the API panel", async () => {
    const { host } = await mount(hubFlow);

    expect(buttons(host)).toContain("Share");
    expect(host.querySelector("[data-testid='api-panel']")).toBeNull();
    expect(host.querySelector(".missing-banner")).toBeNull();
  });

  it("keeps a local flow's page as it was: Run, Open, Copy, Share, Delete and Add schedule", async () => {
    const { host } = await mount(flow());

    expect(buttons(host)).toEqual(
      expect.arrayContaining([
        "Run Flow",
        "Open in Designer",
        "Modify in notebook",
        "Favorite",
        "Share",
        "Delete",
      ]),
    );
    expect(text(host.querySelector("[data-section='Schedules'] button:not([data-testid])"))).toBe(
      "Add",
    );
    expect(host.querySelector("[data-testid='api-panel']")).not.toBeNull();
  });
});
