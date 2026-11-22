import { describe, expect, it, vi } from "vitest";
import { createDashboardCreation, newDashboardName, type CreationPhase } from "./dashboardCreation";

const deferred = <T>() => {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

describe("newDashboardName", () => {
  it("is the plain name when the library does not hold it", () => {
    expect(newDashboardName([])).toBe("Untitled dashboard");
    expect(newDashboardName(["Sales"])).toBe("Untitled dashboard");
  });

  it("takes the next free number when the name is taken", () => {
    expect(newDashboardName(["Untitled dashboard"])).toBe("Untitled dashboard 2");
    expect(newDashboardName(["untitled dashboard", "Untitled dashboard 2"])).toBe(
      "Untitled dashboard 3",
    );
  });
});

describe("createDashboardCreation", () => {
  it("starts created for an existing dashboard and never sends", async () => {
    const send = vi.fn();
    const creation = createDashboardCreation({ id: 7, send });
    expect(creation.phase()).toBe("created");
    expect(creation.id()).toBe(7);
    expect(creation.isCreated()).toBe(true);
    await expect(creation.create({})).rejects.toThrow();
    expect(send).not.toHaveBeenCalled();
  });

  it("goes pending → creating → created and keeps the id", async () => {
    const phases: CreationPhase[] = [];
    const answer = deferred<{ id: number }>();
    const creation = createDashboardCreation({
      send: () => answer.promise,
      onChange: (phase) => phases.push(phase),
    });
    expect(creation.phase()).toBe("pending");
    expect(creation.id()).toBeNull();
    const request = creation.create({ name: "a" });
    expect(creation.phase()).toBe("creating");
    answer.resolve({ id: 12 });
    await expect(request).resolves.toEqual({ id: 12 });
    expect(creation.phase()).toBe("created");
    expect(creation.id()).toBe(12);
    expect(phases).toEqual(["creating", "created"]);
  });

  it("sends one create however often it is asked while in flight", async () => {
    const answer = deferred<{ id: number }>();
    const send = vi.fn(() => answer.promise);
    const creation = createDashboardCreation({ send });
    const first = creation.create({ n: 1 });
    const second = creation.create({ n: 2 });
    expect(second).toBe(first);
    answer.resolve({ id: 3 });
    await first;
    expect(send).toHaveBeenCalledTimes(1);
    expect(send).toHaveBeenCalledWith({ n: 1 });
    await expect(creation.create({ n: 3 })).rejects.toThrow();
    expect(send).toHaveBeenCalledTimes(1);
  });

  it("is failed after a refused create and sends again on the next call", async () => {
    const send = vi
      .fn<(p: unknown) => Promise<{ id: number }>>()
      .mockRejectedValueOnce(new Error("network"))
      .mockResolvedValueOnce({ id: 5 });
    const creation = createDashboardCreation({ send });
    await expect(creation.create({})).rejects.toThrow("network");
    expect(creation.phase()).toBe("failed");
    expect(creation.id()).toBeNull();
    await expect(creation.create({})).resolves.toEqual({ id: 5 });
    expect(creation.phase()).toBe("created");
    expect(send).toHaveBeenCalledTimes(2);
  });
});
