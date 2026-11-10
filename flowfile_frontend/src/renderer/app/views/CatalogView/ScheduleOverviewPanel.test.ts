// @vitest-environment happy-dom
// The Schedules page shows the status of the scheduler that fires what it lists: the hub's for the hub's schedules,
// this machine's only for this machine's own, and the counts over everything listed.
import { h, type FunctionalComponent } from "vue";
import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ns } from "../../__fixtures__/catalogTree";
import { element, mountIn, text } from "../../__fixtures__/mount";
import { useCatalogStore } from "../../stores/catalog-store";
import type { FlowSchedule, SchedulerStatus } from "../../types/catalog.types";

vi.mock("../../services/axios.config", () => ({ default: {} }));
vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../../services/auth.service", () => ({
  authService: { isInDesktopMode: () => true },
}));
vi.mock("./components/ScheduleTable.vue", () => ({ default: { render: () => null } }));

import ScheduleOverviewPanel from "./ScheduleOverviewPanel.vue";

const stubs: Record<string, FunctionalComponent> = {
  "el-button": element("button"),
  "el-input": element("input"),
  "el-popover": (_, { slots }) => h("span", slots.reference?.()),
};

const schedule = (id: number, source: "local" | "hub"): FlowSchedule =>
  ({ id, registration_id: id, enabled: true, source, schedule_type: "interval" }) as FlowSchedule;

let host: HTMLElement;
let pinia: ReturnType<typeof createPinia>;

const mount = async (opts: {
  schedules: FlowSchedule[];
  scheduler: SchedulerStatus;
  remote: SchedulerStatus | null;
  ownOffered: boolean;
}) => {
  const store = useCatalogStore(pinia);
  store.schedules = opts.schedules;
  store.schedulerStatus = opts.scheduler;
  store.remoteSchedulerStatus = opts.remote;
  store.tree = [
    ns({ id: 1, offered: opts.ownOffered } as never),
    ns({ id: -10, source: "hub", hub_id: 10 }),
  ];
  host = await mountIn(ScheduleOverviewPanel, { pinia, stubs });
};

const local = () => host.querySelector('[data-testid="local-scheduler-status"]');
const remote = () => host.querySelector('[data-testid="remote-scheduler-status"]');
const total = () => text(host.querySelector(".summary-card .summary-value"));

describe("ScheduleOverviewPanel", () => {
  beforeEach(() => {
    pinia = createPinia();
    setActivePinia(pinia);
  });

  it("on a joined server shows the hub's scheduler and not this machine's stopped one", async () => {
    await mount({
      schedules: [schedule(-7, "hub"), schedule(-8, "hub")],
      scheduler: { active: false },
      remote: { active: true },
      ownOffered: false,
    });
    expect(local()).toBeNull();
    expect(text(remote())).toBe("The hub's scheduler running");
    expect(host.querySelector(".scheduler-warning")).toBeNull();
    expect(total()).toBe("2");
  });

  it("shows no local banner when every listed schedule is the hub's and the hub says nothing", async () => {
    await mount({
      schedules: [schedule(-7, "hub")],
      scheduler: { active: false },
      remote: null,
      ownOffered: true,
    });
    expect(local()).toBeNull();
    expect(remote()).toBeNull();
  });

  it("beside the hub's schedules words this machine's scheduler for this machine's own", async () => {
    await mount({
      schedules: [schedule(3, "local"), schedule(-7, "hub")],
      scheduler: { active: false },
      remote: { active: true },
      ownOffered: true,
    });
    expect(text(local())).toContain("This machine's scheduler not running");
    expect(local()?.querySelector("button")).not.toBeNull();
    expect(text(remote())).toBe("The hub's scheduler running");
    expect(total()).toBe("2");
  });

  it("on a plain core shows this machine's scheduler as before", async () => {
    await mount({
      schedules: [schedule(3, "local")],
      scheduler: { active: false },
      remote: null,
      ownOffered: true,
    });
    expect(text(local())).toContain("Scheduler not running");
    expect(text(local())).not.toContain("This machine");
    expect(remote()).toBeNull();
  });
});
