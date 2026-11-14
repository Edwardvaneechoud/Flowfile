// @vitest-environment happy-dom
// A schedule's "Runs on" over the real Element Plus select: what it shows for each value, and that only a pick emits.
import type { FunctionalComponent } from "vue";
import { ElOption, ElSelect } from "element-plus";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mountIn, settle, text } from "../../../__fixtures__/mount";

const mocks = vi.hoisted(() => ({ list: vi.fn() }));
vi.mock("../../../api/computeBackends.api", () => ({ listComputeBackends: mocks.list }));

import ScheduleBackendSelect from "./ScheduleBackendSelect.vue";

const components = { "el-select": ElSelect, "el-option": ElOption } as unknown as Record<
  string,
  FunctionalComponent
>;
const update = vi.fn();

const mount = (modelValue: string) =>
  mountIn(ScheduleBackendSelect, {
    props: { modelValue, "onUpdate:modelValue": update },
    stubs: components,
  });

const shown = (host: HTMLElement) => text(host.querySelector(".el-select__placeholder"));
const items = () => Array.from(document.querySelectorAll<HTMLElement>(".el-select-dropdown__item"));

beforeEach(() => {
  update.mockReset();
  mocks.list.mockReset().mockResolvedValue([]);
});

describe("ScheduleBackendSelect", () => {
  it("shows the flow's own setting for no backend, with no backends and with a failed listing", async () => {
    expect(shown(await mount(""))).toBe("The flow's own setting");
    expect(items().map(text)).toEqual(["The flow's own setting", "Default worker"]);

    mocks.list.mockRejectedValue(new Error("offline"));
    expect(shown(await mount(""))).toBe("The flow's own setting");
    expect(update).not.toHaveBeenCalled();
  });

  it("keeps showing a backend that is no longer configured", async () => {
    mocks.list.mockResolvedValue([{ id: "worker:team", verdict: null }]);
    const host = await mount("worker:gone");
    expect(shown(host)).toBe("worker:gone");
    expect(items().map(text)).toEqual([
      "The flow's own setting",
      "Default worker",
      "worker:team · Not checked",
      "worker:gone",
    ]);
    expect(update).not.toHaveBeenCalled();
  });

  it("emits the picked backend, and an empty string for the flow's own setting", async () => {
    await mount("worker");
    items()[0].click();
    await settle();
    expect(update.mock.calls).toEqual([[""]]);
  });
});
