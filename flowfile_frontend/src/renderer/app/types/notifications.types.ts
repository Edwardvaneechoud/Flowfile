// Notification (alerting) DTOs — mirrors flowfile_core's /notifications router.
//
// Three related resources: a *channel* is a webhook endpoint (Slack/Discord/Teams/
// generic), a *rule* says which run outcomes are forwarded to a channel and for
// which scope, and *history* records the delivery attempts.

export type NotificationChannelType = "slack" | "discord" | "teams" | "generic";

export interface NotificationChannel {
  id: number;
  owner_id: number;
  name: string;
  channel_type: NotificationChannelType;
  /** Masked preview, e.g. "https://hooks.slack.com/…f3ab" — the full URL never leaves the server. */
  webhook_url_preview: string;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface NotificationChannelCreate {
  name: string;
  channel_type: NotificationChannelType;
  webhook_url: string;
  enabled?: boolean;
}

export interface NotificationChannelUpdate {
  name?: string;
  channel_type?: NotificationChannelType;
  webhook_url?: string;
  enabled?: boolean;
}

export interface NotificationUrlTest {
  channel_type: NotificationChannelType;
  webhook_url: string;
}

/** Result of a synchronous webhook delivery test (saved channel or a typed-in URL). */
export interface NotificationTestResult {
  ok: boolean;
  error: string | null;
}

/**
 * Scope semantics: `schedule_id` set → that schedule's runs; `registration_id` set
 * (without a schedule) → that flow's runs; neither → every flow the user owns.
 */
export interface NotificationRule {
  id: number;
  owner_id: number;
  channel_id: number;
  channel_name: string | null;
  channel_type: string | null;
  registration_id: number | null;
  flow_name: string | null;
  schedule_id: number | null;
  schedule_name: string | null;
  on_failure: boolean;
  on_success: boolean;
  on_recovery: boolean;
  enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface NotificationRuleCreate {
  channel_id: number;
  registration_id?: number | null;
  schedule_id?: number | null;
  on_failure?: boolean;
  on_success?: boolean;
  on_recovery?: boolean;
  enabled?: boolean;
}

export interface NotificationRuleUpdate {
  channel_id?: number;
  on_failure?: boolean;
  on_success?: boolean;
  on_recovery?: boolean;
  enabled?: boolean;
}

export type NotificationEventType = "run_failed" | "run_success" | "run_recovered" | "run_orphaned";

export type NotificationDeliveryStatus = "pending" | "sending" | "sent" | "dead";

export interface NotificationHistoryItem {
  id: number;
  event_type: NotificationEventType;
  run_id: number | null;
  flow_name: string | null;
  channel_name: string | null;
  status: NotificationDeliveryStatus;
  attempts: number;
  last_error: string | null;
  created_at: string;
  sent_at: string | null;
}
