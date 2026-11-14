// @vitest-environment happy-dom
// The Schedules page shows each scheduler that fires what it lists, as core lists them (this machine's first, each
// saying whose beside another's), this machine's only where its own catalog is offered, and counts over everything.
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
  schedulers: SchedulerStatus[];
  ownOffered: boolean;
}) => {
  const store = useCatalogStore(pinia);
  store.schedules = opts.schedules;
  store.schedulers = opts.schedulers;
  store.tree = [ns({ id: 1, offered: opts.ownOffered } as never), ns({ id: -10, source: "hub" })];
  host = await mountIn(ScheduleOverviewPanel, { pinia, stubs });
};

const bars = () => Array.from(host.querySelectorAll('[data-testid="scheduler-status"]'));
const total = () => text(host.querySelector(".summary-card .summary-value"));

describe("ScheduleOverviewPanel", () => {
  beforeEach(() => {
    pinia = createPinia();
    setActivePinia(pinia);
  });

  it("on a joined server shows the hub's scheduler and not this machine's stopped one", async () => {
    await mount({
      schedules: [schedule(-7, "hub"), schedule(-8, "hub")],
      schedulers: [
        { place: "On this server", active: false },
        { place: "Acme Data", active: true },
      ],
      ownOffered: false,
    });
    expect(bars().map(text)).toEqual(["Acme Data: Scheduler running"]);
    expect(bars()[0].querySelector("button")).toBeNull();
    expect(host.querySelector(".scheduler-warning")).toBeNull();
    expect(total()).toBe("2");
  });

  it("beside the hub's scheduler says which is this machine's, and starts only this machine's", async () => {
    await mount({
      schedules: [schedule(3, "local"), schedule(-7, "hub")],
      schedulers: [
        { place: "On this computer", active: false },
        { place: "Acme Data", active: true, heartbeat_at: "2026-11-01T12:00:00Z" },
      ],
      ownOffered: true,
    });
    const [own, theirs] = bars();
    expect(text(own)).toBe("On this computer: Scheduler not running Start");
    expect(own.querySelector("button")).not.toBeNull();
    expect(text(theirs)).toContain("Acme Data: Scheduler running");
    expect(text(theirs)).toContain("Last heartbeat");
    expect(theirs.querySelector("button")).toBeNull();
    expect(total()).toBe("2");
  });

  it("on a plain core shows this machine's scheduler as before", async () => {
    await mount({
      schedules: [schedule(3, "local")],
      schedulers: [{ active: false }],
      ownOffered: true,
    });
    expect(bars()).toHaveLength(1);
    expect(text(bars()[0])).toContain("Scheduler not running");
    expect(text(bars()[0])).not.toContain(":");
    expect(host.querySelector(".scheduler-warning")).not.toBeNull();
  });
});
