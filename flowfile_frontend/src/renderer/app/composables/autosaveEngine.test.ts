// Unit tests for the pure autosave engine: debounce/max-wait scheduling,
// no-op suppression, single-flight saves, retry backoff, error classification,
// and the conflict/blocked/suspend/dispose lifecycles. Node env, fake timers.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  AUTOSAVE_DEBOUNCE_MS,
  AUTOSAVE_MAX_WAIT_MS,
  AUTOSAVE_RETRY_MAX_MS,
  classifyAutosaveError,
  createAutosaveEngine,
  type AutosaveEngine,
  type AutosaveSnapshot,
  type AutosaveState,
  type SaveReason,
} from "./autosaveEngine";

const httpError = (status: number, data?: unknown, message = `HTTP ${status}`) =>
  Object.assign(new Error(message), { response: { status, data } });

interface Deferred<T = void> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
}

const deferred = <T = void>(): Deferred<T> => {
  let resolve!: (value: T) => void;
  let reject!: (err: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
};

const advance = (ms: number) => vi.advanceTimersByTimeAsync(ms);
const tick = () => vi.advanceTimersByTimeAsync(0);

interface Harness {
  engine: AutosaveEngine;
  saves: Array<{ payload: string; reason: SaveReason }>;
  states: AutosaveState[];
  setSnapshot: (snapshot: AutosaveSnapshot<string> | null) => void;
  setSaveImpl: (impl: () => Promise<void>) => void;
}

const makeHarness = (
  initial: AutosaveSnapshot<string> | null = { payload: "p1", serialized: "s1" },
): Harness => {
  let snapshot = initial;
  let saveImpl: () => Promise<void> = () => Promise.resolve();
  const saves: Array<{ payload: string; reason: SaveReason }> = [];
  const states: AutosaveState[] = [];
  const engine = createAutosaveEngine<string>({
    getSnapshot: () => snapshot,
    save: (payload, ctx) => {
      saves.push({ payload, reason: ctx.reason });
      return saveImpl();
    },
    onChange: (state) => states.push(state),
  });
  return {
    engine,
    saves,
    states,
    setSnapshot: (next) => {
      snapshot = next;
    },
    setSaveImpl: (impl) => {
      saveImpl = impl;
    },
  };
};

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("classifyAutosaveError", () => {
  it("classifies a 409 with an object stale_write detail as conflict", () => {
    expect(classifyAutosaveError(httpError(409, { detail: { error: "stale_write" } }))).toBe(
      "conflict",
    );
  });

  it("classifies a 409 with a string detail as rejected", () => {
    expect(classifyAutosaveError(httpError(409, { detail: "stale_write" }))).toBe("rejected");
  });

  it("classifies a 409 without a detail as rejected", () => {
    expect(classifyAutosaveError(httpError(409))).toBe("rejected");
    expect(classifyAutosaveError(httpError(409, { detail: { error: "other" } }))).toBe("rejected");
  });

  it("maps blocked statuses onto their kinds", () => {
    expect(classifyAutosaveError(httpError(403))).toBe("blocked-forbidden");
    expect(classifyAutosaveError(httpError(404))).toBe("blocked-gone");
    expect(classifyAutosaveError(httpError(410))).toBe("blocked-gone");
    expect(classifyAutosaveError(httpError(401))).toBe("blocked-auth");
  });

  it("treats validation failures as rejected", () => {
    expect(classifyAutosaveError(httpError(400))).toBe("rejected");
    expect(classifyAutosaveError(httpError(422))).toBe("rejected");
  });

  it("treats timeouts, throttling, 5xx, and network errors as retryable", () => {
    expect(classifyAutosaveError(httpError(408))).toBe("retryable");
    expect(classifyAutosaveError(httpError(429))).toBe("retryable");
    expect(classifyAutosaveError(httpError(500))).toBe("retryable");
    expect(classifyAutosaveError(httpError(503))).toBe("retryable");
    expect(classifyAutosaveError(new Error("Network Error"))).toBe("retryable");
    expect(classifyAutosaveError(undefined)).toBe("retryable");
    expect(classifyAutosaveError("boom")).toBe("retryable");
  });
});

describe("debounce and maxWait scheduling", () => {
  it("saves exactly at the debounce boundary", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    expect(h.engine.state().status).toBe("dirty");
    await advance(AUTOSAVE_DEBOUNCE_MS - 1);
    expect(h.saves).toHaveLength(0);
    await advance(1);
    expect(h.saves).toEqual([{ payload: "p1", reason: "debounce" }]);
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().lastSavedAt).not.toBeNull();
    expect(h.engine.isDirty()).toBe(false);
  });

  it("restarts the trailing debounce on every change", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    await advance(1500);
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS - 1);
    expect(h.saves).toHaveLength(0);
    await advance(1);
    expect(h.saves).toHaveLength(1);
  });

  it("guarantees exactly one save at maxWait under continuous edits", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    for (let t = 1000; t < AUTOSAVE_MAX_WAIT_MS; t += 1000) {
      await advance(1000);
      h.engine.notifyChange();
    }
    expect(h.saves).toHaveLength(0);
    await advance(1000);
    expect(h.saves).toEqual([{ payload: "p1", reason: "maxwait" }]);
    expect(h.engine.state().status).toBe("idle");
  });

  it("suppresses the save when the snapshot matches the baseline", async () => {
    const h = makeHarness();
    h.engine.reset("s1");
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(0);
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.isDirty()).toBe(false);
    await advance(AUTOSAVE_MAX_WAIT_MS);
    expect(h.saves).toHaveLength(0);
  });

  it("stays dirty without timers when getSnapshot returns null", async () => {
    const h = makeHarness(null);
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(0);
    expect(h.engine.state().status).toBe("dirty");
    expect(h.engine.isDirty()).toBe(true);
    await advance(AUTOSAVE_MAX_WAIT_MS * 2);
    expect(h.saves).toHaveLength(0);
    h.setSnapshot({ payload: "p1", serialized: "s1" });
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);
  });
});

describe("single-flight", () => {
  it("supersedes an in-flight save with exactly one follow-up using the newer snapshot", async () => {
    const h = makeHarness();
    const gate = deferred();
    h.setSaveImpl(() => gate.promise);
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);
    expect(h.engine.state().status).toBe("saving");

    h.setSnapshot({ payload: "p2", serialized: "s2" });
    h.engine.notifyChange();
    expect(h.engine.isDirty()).toBe(true);
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);

    h.setSaveImpl(() => Promise.resolve());
    gate.resolve();
    await tick();
    expect(h.engine.state().status).toBe("dirty");
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(2);
    expect(h.saves[1]).toEqual({ payload: "p2", reason: "debounce" });
    expect(h.engine.state().status).toBe("idle");
  });

  it("owns saving during an async getSnapshot; a change redirties without a double attempt", async () => {
    let snapPromise: Promise<AutosaveSnapshot<string> | null>;
    const gate = deferred<AutosaveSnapshot<string> | null>();
    snapPromise = gate.promise;
    const saves: Array<{ payload: string; reason: SaveReason }> = [];
    const engine = createAutosaveEngine<string>({
      getSnapshot: () => snapPromise,
      save: async (payload, ctx) => {
        saves.push({ payload, reason: ctx.reason });
      },
    });
    engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(engine.state().status).toBe("saving");
    expect(saves).toHaveLength(0);

    engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(engine.state().status).toBe("saving");
    expect(saves).toHaveLength(0);

    snapPromise = Promise.resolve({ payload: "p2", serialized: "s2" });
    gate.resolve({ payload: "p1", serialized: "s1" });
    await tick();
    expect(saves).toHaveLength(1);
    expect(engine.state().status).toBe("dirty");
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(saves).toHaveLength(2);
    expect(saves[1]).toEqual({ payload: "p2", reason: "debounce" });
    expect(engine.state().status).toBe("idle");
  });
});

describe("retry backoff", () => {
  it("backs off 3s/6s/12s/24s, caps at 30s, and resets retryCount on success", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(httpError(500)));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);
    expect(h.engine.state().status).toBe("retrying");
    expect(h.engine.state().retryCount).toBe(1);

    const expectRetryAfter = async (delayMs: number, expectedSaves: number) => {
      await advance(delayMs - 1);
      expect(h.saves).toHaveLength(expectedSaves - 1);
      await advance(1);
      expect(h.saves).toHaveLength(expectedSaves);
    };
    await expectRetryAfter(3000, 2);
    await expectRetryAfter(6000, 3);
    await expectRetryAfter(12000, 4);
    await expectRetryAfter(24000, 5);
    await expectRetryAfter(30000, 6);
    expect(h.saves.slice(1).every((s) => s.reason === "retry")).toBe(true);

    h.setSaveImpl(() => Promise.resolve());
    await advance(30000);
    expect(h.saves).toHaveLength(7);
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().retryCount).toBe(0);
    expect(h.engine.state().lastError).toBeNull();
  });

  it("a change during retrying cancels the retry timer and rejoins the debounce path", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(httpError(503)));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("retrying");

    h.setSaveImpl(() => Promise.resolve());
    h.setSnapshot({ payload: "p2", serialized: "s2" });
    h.engine.notifyChange();
    expect(h.engine.state().status).toBe("dirty");
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(2);
    expect(h.saves[1]).toEqual({ payload: "p2", reason: "debounce" });
    expect(h.engine.state().status).toBe("idle");
    await advance(AUTOSAVE_RETRY_MAX_MS * 2);
    expect(h.saves).toHaveLength(2);
  });
});

describe("conflict", () => {
  const conflictError = () => httpError(409, { detail: { error: "stale_write" } });

  it("parks in conflict, never re-arms, and exits via resolveConflictReloaded", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(conflictError()));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("conflict");
    expect(h.engine.isDirty()).toBe(true);

    h.engine.notifyChange();
    await advance(AUTOSAVE_MAX_WAIT_MS * 6);
    expect(h.saves).toHaveLength(1);
    expect(h.engine.state().status).toBe("conflict");

    h.engine.resolveConflictReloaded("s2");
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().retryCount).toBe(0);
    expect(h.engine.isDirty()).toBe(false);

    h.setSaveImpl(() => Promise.resolve());
    h.setSnapshot({ payload: "p3", serialized: "s3" });
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(2);
    expect(h.engine.state().status).toBe("idle");
  });

  it("markSaved also exits conflict", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(conflictError()));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("conflict");

    h.engine.markSaved("s1");
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().lastSavedAt).not.toBeNull();
    expect(h.engine.state().lastError).toBeNull();
    expect(h.engine.isDirty()).toBe(false);
  });
});

describe("blocked", () => {
  it("403 blocks with kind forbidden; edits stay pending but never re-arm", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(httpError(403)));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("blocked");
    expect(h.engine.state().blockKind).toBe("forbidden");

    h.engine.notifyChange();
    expect(h.engine.isDirty()).toBe(true);
    await advance(AUTOSAVE_MAX_WAIT_MS * 6);
    expect(h.saves).toHaveLength(1);
    expect(h.engine.state().status).toBe("blocked");

    h.engine.markSaved("s1");
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().blockKind).toBeNull();
  });

  it("maps 404/410 to gone and 401 to auth", async () => {
    for (const [status, kind] of [
      [404, "gone"],
      [410, "gone"],
      [401, "auth"],
    ] as const) {
      const h = makeHarness();
      h.setSaveImpl(() => Promise.reject(httpError(status)));
      h.engine.notifyChange();
      await advance(AUTOSAVE_DEBOUNCE_MS);
      expect(h.engine.state().status).toBe("blocked");
      expect(h.engine.state().blockKind).toBe(kind);
    }
  });
});

describe("rejected", () => {
  it("returns to dirty with the error and retries only on the next change", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(httpError(422, undefined, "invalid payload")));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);
    expect(h.engine.state().status).toBe("dirty");
    expect(h.engine.state().lastError).toBe("invalid payload");

    await advance(AUTOSAVE_MAX_WAIT_MS * 6);
    expect(h.saves).toHaveLength(1);

    h.setSaveImpl(() => Promise.resolve());
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(2);
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().lastError).toBeNull();
  });
});

describe("flush", () => {
  it("fires the pending save immediately with reason flush and returns true", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    const ok = await h.engine.flush();
    expect(ok).toBe(true);
    expect(h.saves).toEqual([{ payload: "p1", reason: "flush" }]);
    expect(h.engine.state().status).toBe("idle");
  });

  it("returns true without saving when nothing is dirty", async () => {
    const h = makeHarness();
    const ok = await h.engine.flush();
    expect(ok).toBe(true);
    expect(h.saves).toHaveLength(0);
  });

  it("returns false and fires no network under conflict", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(httpError(409, { detail: { error: "stale_write" } })));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("conflict");

    const ok = await h.engine.flush();
    expect(ok).toBe(false);
    expect(h.saves).toHaveLength(1);
  });

  it("returns false and fires no network when blocked", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(httpError(403)));
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("blocked");

    const ok = await h.engine.flush();
    expect(ok).toBe(false);
    expect(h.saves).toHaveLength(1);
  });

  it("drains the in-flight save and then the redirtied snapshot", async () => {
    const h = makeHarness();
    const gate = deferred();
    h.setSaveImpl(() => gate.promise);
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);

    h.setSnapshot({ payload: "p2", serialized: "s2" });
    h.engine.notifyChange();
    h.setSaveImpl(() => Promise.resolve());
    const flushPromise = h.engine.flush();
    gate.resolve();
    const ok = await flushPromise;
    expect(ok).toBe(true);
    expect(h.saves).toHaveLength(2);
    expect(h.saves[1]).toEqual({ payload: "p2", reason: "flush" });
    expect(h.engine.state().status).toBe("idle");
  });

  it("returns false without attempting while suspended and dirty", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    h.engine.suspend();
    const ok = await h.engine.flush();
    expect(ok).toBe(false);
    expect(h.saves).toHaveLength(0);
  });
});

describe("suspend/resume", () => {
  it("timers that fire while suspended do nothing; resume re-arms the debounce", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    h.engine.suspend();
    await advance(AUTOSAVE_MAX_WAIT_MS * 2);
    expect(h.saves).toHaveLength(0);
    expect(h.engine.state().status).toBe("dirty");

    h.engine.resume();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);
    expect(h.engine.state().status).toBe("idle");
  });

  it("changes while suspended mark dirty but start no attempt", async () => {
    const h = makeHarness();
    h.engine.suspend();
    h.engine.notifyChange();
    expect(h.engine.state().status).toBe("dirty");
    expect(h.engine.isDirty()).toBe(true);
    await advance(AUTOSAVE_MAX_WAIT_MS * 2);
    expect(h.saves).toHaveLength(0);

    h.engine.resume();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);
  });

  it("resume without pending changes stays idle", async () => {
    const h = makeHarness();
    h.engine.suspend();
    h.engine.resume();
    await advance(AUTOSAVE_MAX_WAIT_MS);
    expect(h.saves).toHaveLength(0);
    expect(h.engine.state().status).toBe("idle");
  });
});

describe("markSaved and reset", () => {
  it("markSaved adopts the baseline and stamps lastSavedAt", async () => {
    const h = makeHarness();
    expect(h.engine.state().lastSavedAt).toBeNull();
    h.engine.markSaved("s1");
    expect(h.engine.state().lastSavedAt).not.toBeNull();
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(0);
    expect(h.engine.state().status).toBe("idle");
  });

  it("reset adopts the baseline but leaves lastSavedAt untouched", async () => {
    const h = makeHarness();
    h.engine.reset("s1");
    expect(h.engine.state().lastSavedAt).toBeNull();
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(0);
    expect(h.engine.state().status).toBe("idle");

    h.setSnapshot({ payload: "p2", serialized: "s2" });
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.saves).toHaveLength(1);
    const savedAt = h.engine.state().lastSavedAt;
    expect(savedAt).not.toBeNull();
    await advance(5000);
    h.engine.reset("s3");
    expect(h.engine.state().lastSavedAt).toBe(savedAt);
  });
});

describe("dispose", () => {
  it("clears timers and turns notifyChange into a no-op", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    h.engine.dispose();
    await advance(AUTOSAVE_MAX_WAIT_MS * 2);
    expect(h.saves).toHaveLength(0);

    h.engine.notifyChange();
    await advance(AUTOSAVE_MAX_WAIT_MS * 2);
    expect(h.saves).toHaveLength(0);
  });
});

describe("onChange", () => {
  it("emits a fresh state object per transition", async () => {
    const h = makeHarness();
    h.engine.notifyChange();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.states.map((s) => s.status)).toEqual(["dirty", "saving", "idle"]);
    expect(h.states[0]).not.toBe(h.states[1]);
    expect(h.engine.state()).not.toBe(h.engine.state());
  });
});

describe("dirtySince tracking (status-dot staleness)", () => {
  it("is set on the first unsaved change and cleared on a successful save", async () => {
    const h = makeHarness();
    expect(h.engine.state().dirtySince).toBeNull();
    h.engine.notifyChange();
    expect(h.engine.state().dirtySince).not.toBeNull();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().dirtySince).toBeNull();
  });

  it("survives a failed save so staleness accrues from the original edit", async () => {
    const h = makeHarness();
    h.setSaveImpl(() => Promise.reject(httpError(500)));
    h.engine.notifyChange();
    const before = h.engine.state().dirtySince;
    expect(before).not.toBeNull();
    await advance(AUTOSAVE_DEBOUNCE_MS);
    expect(h.engine.state().status).toBe("retrying");
    expect(h.engine.state().dirtySince).toBe(before);
  });

  it("refreshes to the redirty time when changes arrive during a save", async () => {
    const h = makeHarness();
    const gate = deferred();
    h.setSaveImpl(() => gate.promise);
    h.engine.notifyChange();
    const original = h.engine.state().dirtySince;
    await advance(AUTOSAVE_DEBOUNCE_MS);
    await advance(500);
    h.setSnapshot({ payload: "p2", serialized: "s2" });
    h.engine.notifyChange();
    gate.resolve();
    await tick();
    const after = h.engine.state().dirtySince;
    expect(h.engine.state().status).toBe("dirty");
    expect(after).not.toBeNull();
    expect(after as number).toBeGreaterThan(original as number);
  });

  it("clears on markSaved and reset", () => {
    const h = makeHarness();
    h.engine.notifyChange();
    expect(h.engine.state().dirtySince).not.toBeNull();
    h.engine.markSaved("s1");
    expect(h.engine.state().dirtySince).toBeNull();
    h.engine.notifyChange();
    expect(h.engine.state().dirtySince).not.toBeNull();
    h.engine.reset("s1");
    expect(h.engine.state().dirtySince).toBeNull();
  });
});

describe("enterConflict (out-of-band stale writes)", () => {
  it("enters conflict, blocks re-arming, and exits via resolveConflictReloaded", async () => {
    const h = makeHarness();
    h.engine.enterConflict("changed elsewhere");
    expect(h.engine.state().status).toBe("conflict");
    expect(h.engine.state().lastError).toBe("changed elsewhere");
    expect(h.engine.state().dirtySince).not.toBeNull();
    expect(h.engine.isDirty()).toBe(true);
    h.engine.notifyChange();
    await advance(AUTOSAVE_MAX_WAIT_MS * 2);
    expect(h.saves).toHaveLength(0);
    h.engine.resolveConflictReloaded("s1");
    expect(h.engine.state().status).toBe("idle");
    expect(h.engine.state().dirtySince).toBeNull();
  });
});
