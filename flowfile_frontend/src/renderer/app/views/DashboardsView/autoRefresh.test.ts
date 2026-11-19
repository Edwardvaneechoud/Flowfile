import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  AUTO_REFRESH_CHOICES,
  autoRefreshLabel,
  createAutoRefresher,
  savedAutoRefresh,
} from "./autoRefresh";

const MIN = 60_000;

function setup({ busy = false } = {}) {
  let hidden = false;
  let externalBusy = busy;
  const resolvers: (() => void)[] = [];
  const refresh = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        resolvers.push(resolve);
      }),
  );
  const refresher = createAutoRefresher({
    refresh,
    isHidden: () => hidden,
    isBusy: () => externalBusy,
  });
  return {
    refresh,
    refresher,
    finishAll: async () => {
      resolvers.splice(0).forEach((r) => r());
      await Promise.resolve();
      await Promise.resolve();
    },
    setHidden: (value: boolean) => {
      hidden = value;
    },
    setBusy: (value: boolean) => {
      externalBusy = value;
    },
  };
}

describe("createAutoRefresher", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("refreshes once per interval, the first a whole interval after it starts", async () => {
    const { refresh, refresher, finishAll } = setup();
    refresher.setInterval(60);
    expect(refresh).not.toHaveBeenCalled();
    vi.advanceTimersByTime(MIN - 1);
    expect(refresh).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(refresh).toHaveBeenCalledTimes(1);
    await finishAll();
    vi.advanceTimersByTime(MIN);
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("does nothing while off, and a changed interval starts over", async () => {
    const { refresh, refresher, finishAll } = setup();
    refresher.setInterval(null);
    vi.advanceTimersByTime(10 * MIN);
    expect(refresh).not.toHaveBeenCalled();

    refresher.setInterval(300);
    vi.advanceTimersByTime(4 * MIN);
    refresher.setInterval(60);
    vi.advanceTimersByTime(MIN);
    expect(refresh).toHaveBeenCalledTimes(1);
    await finishAll();
    expect(refresher.seconds).toBe(60);

    refresher.setInterval(null);
    vi.advanceTimersByTime(10 * MIN);
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it("skips a tick while its own refresh is still running", async () => {
    const { refresh, refresher, finishAll } = setup();
    refresher.setInterval(60);
    vi.advanceTimersByTime(MIN);
    vi.advanceTimersByTime(MIN);
    vi.advanceTimersByTime(MIN);
    expect(refresh).toHaveBeenCalledTimes(1);
    await finishAll();
    vi.advanceTimersByTime(MIN);
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("skips a tick while a refresh started elsewhere is running", () => {
    const { refresh, refresher, setBusy } = setup({ busy: true });
    refresher.setInterval(60);
    vi.advanceTimersByTime(MIN);
    expect(refresh).not.toHaveBeenCalled();
    setBusy(false);
    vi.advanceTimersByTime(MIN);
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it("keeps running after a refresh fails", async () => {
    const refresh = vi.fn(() => Promise.reject(new Error("worker down")));
    const refresher = createAutoRefresher({ refresh, isHidden: () => false });
    refresher.setInterval(60);
    vi.advanceTimersByTime(MIN);
    await Promise.resolve();
    await Promise.resolve();
    vi.advanceTimersByTime(MIN);
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("does not refresh while hidden and catches up once on return", async () => {
    const { refresh, refresher, setHidden, finishAll } = setup();
    refresher.setInterval(60);
    setHidden(true);
    refresher.visibilityChanged();
    vi.advanceTimersByTime(5 * MIN);
    expect(refresh).not.toHaveBeenCalled();

    setHidden(false);
    refresher.visibilityChanged();
    expect(refresh).toHaveBeenCalledTimes(1);
    await finishAll();
    // The interval starts over from the catch-up.
    vi.advanceTimersByTime(MIN - 1);
    expect(refresh).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    expect(refresh).toHaveBeenCalledTimes(2);
  });

  it("does not refresh on return when no tick was missed", () => {
    const { refresh, refresher, setHidden } = setup();
    refresher.setInterval(60);
    setHidden(true);
    refresher.visibilityChanged();
    vi.advanceTimersByTime(MIN / 2);
    setHidden(false);
    refresher.visibilityChanged();
    expect(refresh).not.toHaveBeenCalled();
  });

  it("does not catch up while off", () => {
    const { refresh, refresher, setHidden } = setup();
    refresher.setInterval(60);
    setHidden(true);
    vi.advanceTimersByTime(MIN);
    refresher.setInterval(null);
    setHidden(false);
    refresher.visibilityChanged();
    expect(refresh).not.toHaveBeenCalled();
  });

  it("stops for good", () => {
    const { refresh, refresher, setHidden } = setup();
    refresher.setInterval(60);
    setHidden(true);
    vi.advanceTimersByTime(MIN);
    refresher.stop();
    setHidden(false);
    refresher.visibilityChanged();
    refresher.setInterval(60);
    vi.advanceTimersByTime(10 * MIN);
    expect(refresh).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("autoRefreshLabel", () => {
  it("names every choice", () => {
    expect(AUTO_REFRESH_CHOICES.map(autoRefreshLabel)).toEqual([
      "Off",
      "1 min",
      "5 min",
      "15 min",
      "30 min",
      "1 hour",
    ]);
    expect(autoRefreshLabel(undefined)).toBe("Off");
    expect(autoRefreshLabel(7200)).toBe("2 hours");
    expect(autoRefreshLabel(90)).toBe("90 s");
  });
});

describe("savedAutoRefresh", () => {
  it("reads a saved interval and treats anything else as off", () => {
    expect(savedAutoRefresh(300)).toBe(300);
    expect(savedAutoRefresh(60)).toBe(60);
    expect(savedAutoRefresh(undefined)).toBeNull();
    expect(savedAutoRefresh(null)).toBeNull();
    expect(savedAutoRefresh(30)).toBeNull();
    expect(savedAutoRefresh("300")).toBeNull();
    expect(savedAutoRefresh(Number.NaN)).toBeNull();
  });
});
