// Notifications API Service - alert channels (Slack / Discord / Teams / generic
// webhook), the rules that route run outcomes to them, and the delivery history.
import axios from "../services/axios.config";
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

export class NotificationsApi {
  // ====== Channels ======

  static async getChannels(): Promise<NotificationChannel[]> {
    const response = await axios.get<NotificationChannel[]>("/notifications/channels");
    return response.data;
  }

  static async createChannel(body: NotificationChannelCreate): Promise<NotificationChannel> {
    const response = await axios.post<NotificationChannel>("/notifications/channels", body);
    return response.data;
  }

  static async updateChannel(
    id: number,
    body: NotificationChannelUpdate,
  ): Promise<NotificationChannel> {
    const response = await axios.put<NotificationChannel>(`/notifications/channels/${id}`, body);
    return response.data;
  }

  /** Deleting a channel also removes every rule that pointed at it. */
  static async deleteChannel(id: number): Promise<void> {
    await axios.delete(`/notifications/channels/${id}`);
  }

  /** Send a real test message through a saved channel (synchronous). */
  static async testChannel(id: number): Promise<NotificationTestResult> {
    const response = await axios.post<NotificationTestResult>(`/notifications/channels/${id}/test`);
    return response.data;
  }

  /** Test a webhook URL before it is saved, so the create dialog can verify first. */
  static async testChannelUrl(body: NotificationUrlTest): Promise<NotificationTestResult> {
    const response = await axios.post<NotificationTestResult>(
      "/notifications/channels/test-url",
      body,
    );
    return response.data;
  }

  // ====== Rules ======

  /** Omit both scopes to list every rule; pass one to list that scope's rules. */
  static async getRules(scope?: {
    registrationId?: number | null;
    scheduleId?: number | null;
  }): Promise<NotificationRule[]> {
    const params: Record<string, any> = {};
    if (scope?.registrationId !== undefined && scope.registrationId !== null) {
      params.registration_id = scope.registrationId;
    }
    if (scope?.scheduleId !== undefined && scope.scheduleId !== null) {
      params.schedule_id = scope.scheduleId;
    }
    const response = await axios.get<NotificationRule[]>("/notifications/rules", { params });
    return response.data;
  }

  static async createRule(body: NotificationRuleCreate): Promise<NotificationRule> {
    const response = await axios.post<NotificationRule>("/notifications/rules", body);
    return response.data;
  }

  static async updateRule(id: number, body: NotificationRuleUpdate): Promise<NotificationRule> {
    const response = await axios.put<NotificationRule>(`/notifications/rules/${id}`, body);
    return response.data;
  }

  static async deleteRule(id: number): Promise<void> {
    await axios.delete(`/notifications/rules/${id}`);
  }

  // ====== History ======

  static async getHistory(limit = 50): Promise<NotificationHistoryItem[]> {
    const response = await axios.get<NotificationHistoryItem[]>("/notifications/history", {
      params: { limit },
    });
    return response.data;
  }
}
