// Notifications Store - alert channels, the rules that route run outcomes to
// them, and the recent delivery history.
//
// Split from catalog-store (which owns schedules) the same way community-nodes-store
// is split off: /notifications is its own router with its own api module, and
// keeping it separate stops every catalog-store unit test from having to mock it.
//
// Convention mirrors catalog-store: `load*` swallows failures into `error` so a
// panel still renders, while mutations rethrow so the caller can surface the
// backend's 422 `detail` in a toast.
import { defineStore } from "pinia";
import { NotificationsApi } from "../api/notifications.api";
import type {
  NotificationChannel,
  NotificationChannelCreate,
  NotificationChannelUpdate,
  NotificationHistoryItem,
  NotificationRule,
  NotificationRuleCreate,
  NotificationRuleUpdate,
  NotificationTestResult,
  NotificationUrlTest,
} from "../types";

interface NotificationsState {
  channels: NotificationChannel[];
  /** Every rule the user owns, whatever its scope. */
  rules: NotificationRule[];
  history: NotificationHistoryItem[];
  loadingChannels: boolean;
  loadingRules: boolean;
  loadingHistory: boolean;
  /** Id of the channel whose "Test" button is currently in flight. */
  testingChannelId: number | null;
  /** Last test verdict per channel id, so the row can show it inline. */
  testResults: Record<number, NotificationTestResult>;
  error: string | null;
}

export const HISTORY_LIMIT = 50;

export const useNotificationsStore = defineStore("notifications", {
  state: (): NotificationsState => ({
    channels: [],
    rules: [],
    history: [],
    loadingChannels: false,
    loadingRules: false,
    loadingHistory: false,
    testingChannelId: null,
    testResults: {},
    error: null,
  }),

  getters: {
    hasChannels: (state): boolean => state.channels.length > 0,

    enabledChannels: (state): NotificationChannel[] => state.channels.filter((c) => c.enabled),

    /** Rules that apply to every flow the user owns (no flow and no schedule scope). */
    globalRules: (state): NotificationRule[] =>
      state.rules.filter((r) => r.registration_id === null && r.schedule_id === null),

    rulesForSchedule:
      (state) =>
      (scheduleId: number): NotificationRule[] =>
        state.rules.filter((r) => r.schedule_id === scheduleId),

    rulesForFlow:
      (state) =>
      (registrationId: number): NotificationRule[] =>
        state.rules.filter((r) => r.registration_id === registrationId && r.schedule_id === null),
  },

  actions: {
    // -- Channels --

    async loadChannels() {
      this.loadingChannels = true;
      try {
        this.channels = await NotificationsApi.getChannels();
      } catch (e: any) {
        this.error = e?.response?.data?.detail ?? e?.message ?? "Failed to load channels";
      } finally {
        this.loadingChannels = false;
      }
    },

    async createChannel(body: NotificationChannelCreate): Promise<NotificationChannel> {
      const created = await NotificationsApi.createChannel(body);
      this.channels = [...this.channels, created];
      return created;
    },

    async updateChannel(id: number, body: NotificationChannelUpdate): Promise<NotificationChannel> {
      const updated = await NotificationsApi.updateChannel(id, body);
      this.channels = this.channels.map((c) => (c.id === id ? updated : c));
      // The channel name/type is denormalized onto every rule row that uses it.
      this.rules = this.rules.map((r) =>
        r.channel_id === id
          ? { ...r, channel_name: updated.name, channel_type: updated.channel_type }
          : r,
      );
      return updated;
    },

    /** The backend cascades: rules pointing at this channel go away with it. */
    async deleteChannel(id: number): Promise<void> {
      await NotificationsApi.deleteChannel(id);
      this.channels = this.channels.filter((c) => c.id !== id);
      this.rules = this.rules.filter((r) => r.channel_id !== id);
      this.clearTestResult(id);
    },

    /** Send a real message through a saved channel; the verdict is kept per channel. */
    async testChannel(id: number): Promise<NotificationTestResult> {
      this.testingChannelId = id;
      try {
        const result = await NotificationsApi.testChannel(id);
        this.testResults = { ...this.testResults, [id]: result };
        return result;
      } catch (e: any) {
        const result: NotificationTestResult = {
          ok: false,
          error: e?.response?.data?.detail ?? e?.message ?? "Test failed",
        };
        this.testResults = { ...this.testResults, [id]: result };
        return result;
      } finally {
        this.testingChannelId = null;
      }
    },

    /** Test a typed-in URL before saving it. Never throws — the dialog shows the verdict. */
    async testChannelUrl(body: NotificationUrlTest): Promise<NotificationTestResult> {
      try {
        return await NotificationsApi.testChannelUrl(body);
      } catch (e: any) {
        return {
          ok: false,
          error: e?.response?.data?.detail ?? e?.message ?? "Test failed",
        };
      }
    },

    clearTestResult(id: number) {
      const next = { ...this.testResults };
      delete next[id];
      this.testResults = next;
    },

    // -- Rules --

    /**
     * Load rules. Without a scope this replaces the whole list; with one it merges
     * the scope's rows in, so a schedule panel can refresh itself without dropping
     * the global rules the Alerts panel is showing.
     */
    async loadRules(scope?: { registrationId?: number | null; scheduleId?: number | null }) {
      this.loadingRules = true;
      try {
        const rules = await NotificationsApi.getRules(scope);
        const scoped =
          (scope?.registrationId ?? null) !== null || (scope?.scheduleId ?? null) !== null;
        if (!scoped) {
          this.rules = rules;
        } else {
          const incoming = new Set(rules.map((r) => r.id));
          const isSameScope = (r: NotificationRule) =>
            scope?.scheduleId != null
              ? r.schedule_id === scope.scheduleId
              : r.registration_id === scope?.registrationId && r.schedule_id === null;
          this.rules = [
            ...this.rules.filter((r) => !incoming.has(r.id) && !isSameScope(r)),
            ...rules,
          ];
        }
      } catch (e: any) {
        this.error = e?.response?.data?.detail ?? e?.message ?? "Failed to load alert rules";
      } finally {
        this.loadingRules = false;
      }
    },

    async createRule(body: NotificationRuleCreate): Promise<NotificationRule> {
      const created = await NotificationsApi.createRule(body);
      this.rules = [...this.rules, created];
      return created;
    },

    async updateRule(id: number, body: NotificationRuleUpdate): Promise<NotificationRule> {
      const updated = await NotificationsApi.updateRule(id, body);
      this.rules = this.rules.map((r) => (r.id === id ? updated : r));
      return updated;
    },

    async deleteRule(id: number): Promise<void> {
      await NotificationsApi.deleteRule(id);
      this.rules = this.rules.filter((r) => r.id !== id);
    },

    // -- History --

    async loadHistory(limit = HISTORY_LIMIT) {
      this.loadingHistory = true;
      try {
        this.history = await NotificationsApi.getHistory(limit);
      } catch (e: any) {
        this.error = e?.response?.data?.detail ?? e?.message ?? "Failed to load notifications";
      } finally {
        this.loadingHistory = false;
      }
    },

    /** Everything the Alerts panel needs in one go. */
    async initialize() {
      await Promise.all([this.loadChannels(), this.loadRules(), this.loadHistory()]);
    },
  },
});
