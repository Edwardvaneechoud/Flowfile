import { beforeEach, describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import type { NotificationChannel, NotificationRule } from "../types";

const {
  getChannelsMock,
  createChannelMock,
  updateChannelMock,
  deleteChannelMock,
  testChannelMock,
  testChannelUrlMock,
  getRulesMock,
  createRuleMock,
  updateRuleMock,
  deleteRuleMock,
  getHistoryMock,
} = vi.hoisted(() => ({
  getChannelsMock: vi.fn(),
  createChannelMock: vi.fn(),
  updateChannelMock: vi.fn(),
  deleteChannelMock: vi.fn(),
  testChannelMock: vi.fn(),
  testChannelUrlMock: vi.fn(),
  getRulesMock: vi.fn(),
  createRuleMock: vi.fn(),
  updateRuleMock: vi.fn(),
  deleteRuleMock: vi.fn(),
  getHistoryMock: vi.fn(),
}));

vi.mock("../api/notifications.api", () => ({
  NotificationsApi: {
    getChannels: getChannelsMock,
    createChannel: createChannelMock,
    updateChannel: updateChannelMock,
    deleteChannel: deleteChannelMock,
    testChannel: testChannelMock,
    testChannelUrl: testChannelUrlMock,
    getRules: getRulesMock,
    createRule: createRuleMock,
    updateRule: updateRuleMock,
    deleteRule: deleteRuleMock,
    getHistory: getHistoryMock,
  },
}));

import { useNotificationsStore } from "./notifications-store";

const channel = (overrides: Partial<NotificationChannel> = {}): NotificationChannel =>
  ({
    id: 1,
    owner_id: 1,
    name: "#data-alerts",
    channel_type: "slack",
    webhook_url_preview: "https://hooks.slack.com/…f3ab",
    enabled: true,
    created_at: "2026-01-01T00:00:00",
    updated_at: "2026-01-01T00:00:00",
    ...overrides,
  }) as NotificationChannel;

const rule = (overrides: Partial<NotificationRule> = {}): NotificationRule =>
  ({
    id: 10,
    owner_id: 1,
    channel_id: 1,
    channel_name: "#data-alerts",
    channel_type: "slack",
    registration_id: null,
    flow_name: null,
    schedule_id: null,
    schedule_name: null,
    on_failure: true,
    on_success: false,
    on_recovery: true,
    enabled: true,
    created_at: "2026-01-01T00:00:00",
    updated_at: "2026-01-01T00:00:00",
    ...overrides,
  }) as NotificationRule;

const httpError = (detail: string) => ({ response: { data: { detail } } });

beforeEach(() => {
  setActivePinia(createPinia());
  for (const mock of [
    getChannelsMock,
    createChannelMock,
    updateChannelMock,
    deleteChannelMock,
    testChannelMock,
    testChannelUrlMock,
    getRulesMock,
    createRuleMock,
    updateRuleMock,
    deleteRuleMock,
    getHistoryMock,
  ]) {
    mock.mockReset();
  }
});

describe("notifications-store channels", () => {
  it("loads channels and clears the loading flag", async () => {
    const store = useNotificationsStore();
    getChannelsMock.mockResolvedValue([channel()]);

    await store.loadChannels();

    expect(store.channels).toHaveLength(1);
    expect(store.hasChannels).toBe(true);
    expect(store.loadingChannels).toBe(false);
    expect(store.error).toBeNull();
  });

  it("surfaces the backend detail as the store error instead of throwing", async () => {
    const store = useNotificationsStore();
    getChannelsMock.mockRejectedValue(httpError("nope"));

    await store.loadChannels();

    expect(store.channels).toEqual([]);
    expect(store.error).toBe("nope");
    expect(store.loadingChannels).toBe(false);
  });

  it("appends a created channel", async () => {
    const store = useNotificationsStore();
    store.channels = [channel()];
    createChannelMock.mockResolvedValue(channel({ id: 2, name: "#ops" }));

    const created = await store.createChannel({
      name: "#ops",
      channel_type: "slack",
      webhook_url: "https://hooks.slack.com/services/x",
    });

    expect(created.id).toBe(2);
    expect(store.channels.map((c) => c.id)).toEqual([1, 2]);
  });

  it("rethrows a create failure so the dialog can toast the 422 detail", async () => {
    const store = useNotificationsStore();
    createChannelMock.mockRejectedValue(httpError("Webhook URL points at a private address"));

    await expect(
      store.createChannel({ name: "x", channel_type: "generic", webhook_url: "http://10.0.0.1" }),
    ).rejects.toMatchObject({
      response: { data: { detail: "Webhook URL points at a private address" } },
    });
    expect(store.channels).toEqual([]);
  });

  it("repoints the denormalized channel name/type on rules when a channel is renamed", async () => {
    const store = useNotificationsStore();
    store.channels = [channel()];
    store.rules = [rule(), rule({ id: 11, channel_id: 2, channel_name: "other" })];
    updateChannelMock.mockResolvedValue(channel({ name: "#renamed", channel_type: "discord" }));

    await store.updateChannel(1, { name: "#renamed" });

    expect(store.channels[0].name).toBe("#renamed");
    expect(store.rules[0].channel_name).toBe("#renamed");
    expect(store.rules[0].channel_type).toBe("discord");
    expect(store.rules[1].channel_name).toBe("other");
  });

  it("drops the channel, its rules and its test result on delete", async () => {
    const store = useNotificationsStore();
    store.channels = [channel(), channel({ id: 2 })];
    store.rules = [rule(), rule({ id: 11, channel_id: 2 })];
    store.testResults = { 1: { ok: true, error: null } };
    deleteChannelMock.mockResolvedValue(undefined);

    await store.deleteChannel(1);

    expect(store.channels.map((c) => c.id)).toEqual([2]);
    expect(store.rules.map((r) => r.id)).toEqual([11]);
    expect(store.testResults[1]).toBeUndefined();
  });
});

describe("notifications-store channel tests", () => {
  it("keeps the verdict per channel and clears the in-flight marker", async () => {
    const store = useNotificationsStore();
    testChannelMock.mockResolvedValue({ ok: true, error: null });

    const result = await store.testChannel(1);

    expect(result.ok).toBe(true);
    expect(store.testResults[1]).toEqual({ ok: true, error: null });
    expect(store.testingChannelId).toBeNull();
  });

  it("turns a rejected test into a failed verdict rather than throwing", async () => {
    const store = useNotificationsStore();
    testChannelMock.mockRejectedValue(httpError("404 from Slack"));

    const result = await store.testChannel(1);

    expect(result).toEqual({ ok: false, error: "404 from Slack" });
    expect(store.testResults[1].ok).toBe(false);
    expect(store.testingChannelId).toBeNull();
  });

  it("never throws when testing an unsaved URL", async () => {
    const store = useNotificationsStore();
    testChannelUrlMock.mockRejectedValue(httpError("bad scheme"));

    await expect(
      store.testChannelUrl({ channel_type: "generic", webhook_url: "ftp://x" }),
    ).resolves.toEqual({ ok: false, error: "bad scheme" });
  });
});

describe("notifications-store rules", () => {
  it("replaces the whole list on an unscoped load", async () => {
    const store = useNotificationsStore();
    store.rules = [rule({ id: 99 })];
    getRulesMock.mockResolvedValue([rule({ id: 10 }), rule({ id: 11, schedule_id: 5 })]);

    await store.loadRules();

    expect(getRulesMock).toHaveBeenCalledWith(undefined);
    expect(store.rules.map((r) => r.id)).toEqual([10, 11]);
    expect(store.loadingRules).toBe(false);
  });

  it("merges a schedule-scoped load without dropping the global rules", async () => {
    const store = useNotificationsStore();
    store.rules = [rule({ id: 10 }), rule({ id: 11, schedule_id: 5 })];
    getRulesMock.mockResolvedValue([rule({ id: 12, schedule_id: 5 })]);

    await store.loadRules({ scheduleId: 5 });

    expect(getRulesMock).toHaveBeenCalledWith({ scheduleId: 5 });
    // The stale rule 11 for this schedule is gone; the global rule 10 survives.
    expect(store.rules.map((r) => r.id).sort()).toEqual([10, 12]);
  });

  it("merges a registration-scoped load without touching schedule-scoped rules", async () => {
    const store = useNotificationsStore();
    store.rules = [
      rule({ id: 10 }),
      rule({ id: 11, registration_id: 7 }),
      rule({ id: 12, registration_id: 7, schedule_id: 5 }),
    ];
    getRulesMock.mockResolvedValue([rule({ id: 13, registration_id: 7 })]);

    await store.loadRules({ registrationId: 7 });

    expect(getRulesMock).toHaveBeenCalledWith({ registrationId: 7 });
    // The stale flow-scoped rule 11 is gone; the global rule 10 and the
    // schedule-scoped rule 12 (same flow, different scope) both survive.
    expect(store.rules.map((r) => r.id).sort()).toEqual([10, 12, 13]);
  });

  it("scopes the getters by registration and schedule", () => {
    const store = useNotificationsStore();
    store.rules = [
      rule({ id: 10 }),
      rule({ id: 11, schedule_id: 5 }),
      rule({ id: 12, registration_id: 7 }),
      rule({ id: 13, registration_id: 7, schedule_id: 8 }),
    ];

    expect(store.globalRules.map((r) => r.id)).toEqual([10]);
    expect(store.rulesForSchedule(5).map((r) => r.id)).toEqual([11]);
    expect(store.rulesForFlow(7).map((r) => r.id)).toEqual([12]);
  });

  it("adds, patches and removes rules in place", async () => {
    const store = useNotificationsStore();
    createRuleMock.mockResolvedValue(rule({ id: 10 }));
    updateRuleMock.mockResolvedValue(rule({ id: 10, on_success: true }));
    deleteRuleMock.mockResolvedValue(undefined);

    await store.createRule({ channel_id: 1 });
    expect(store.rules.map((r) => r.id)).toEqual([10]);

    await store.updateRule(10, { on_success: true });
    expect(store.rules[0].on_success).toBe(true);

    await store.deleteRule(10);
    expect(store.rules).toEqual([]);
  });

  it("records a rules load failure as an error and stops loading", async () => {
    const store = useNotificationsStore();
    getRulesMock.mockRejectedValue(httpError("boom"));

    await store.loadRules();

    expect(store.error).toBe("boom");
    expect(store.loadingRules).toBe(false);
  });
});

describe("notifications-store history", () => {
  it("loads the delivery history with the default limit", async () => {
    const store = useNotificationsStore();
    getHistoryMock.mockResolvedValue([]);

    await store.loadHistory();

    expect(getHistoryMock).toHaveBeenCalledWith(200);
    expect(store.loadingHistory).toBe(false);
  });

  it("loads channels, rules and history together on initialize", async () => {
    const store = useNotificationsStore();
    getChannelsMock.mockResolvedValue([channel()]);
    getRulesMock.mockResolvedValue([rule()]);
    getHistoryMock.mockResolvedValue([]);

    await store.initialize();

    expect(getChannelsMock).toHaveBeenCalledTimes(1);
    expect(getRulesMock).toHaveBeenCalledTimes(1);
    expect(getHistoryMock).toHaveBeenCalledTimes(1);
    expect(store.channels).toHaveLength(1);
    expect(store.globalRules).toHaveLength(1);
  });
});
