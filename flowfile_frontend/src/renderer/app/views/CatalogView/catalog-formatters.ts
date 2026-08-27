import type {
  FlowSchedule,
  GlobalArtifact,
  NotificationChannelType,
  NotificationDeliveryStatus,
  NotificationEventType,
} from "../../types";
import { describeCron } from "./cron-builder";

/**
 * Compact date format: "Mar 23, 10:30 AM"
 * Used across catalog panels for run timestamps, schedule triggers, artifact dates.
 */
export function formatDate(dateStr: string): string {
  // Backend sends UTC timestamps; ensure JS parses them as UTC
  const normalized =
    dateStr.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(dateStr) ? dateStr : dateStr + "Z";
  return new Date(normalized).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

export function formatSize(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return "--";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

/** Last dotted segment of a python type, e.g. "xgboost.core.Booster" -> "Booster". */
export function formatPythonType(
  pythonType: string | null | undefined,
  fallback = "unknown",
): string {
  if (!pythonType) return fallback;
  const parts = pythonType.split(".");
  return parts[parts.length - 1];
}

export function formatType(artifact: GlobalArtifact): string {
  return formatPythonType(artifact.python_type, artifact.serialization_format ?? "unknown");
}

export function formatNumber(n: number | null | undefined): string {
  if (n === null || n === undefined) return "--";
  return n.toLocaleString();
}

export function formatDuration(seconds: number | null): string {
  if (seconds === null) return "--";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

export function formatScheduleType(schedule: FlowSchedule): string {
  if (schedule.schedule_type === "interval" && schedule.interval_seconds) {
    const mins = Math.floor(schedule.interval_seconds / 60);
    if (mins < 60) return `Every ${mins}m`;
    const hrs = Math.floor(mins / 60);
    const remMins = mins % 60;
    return remMins > 0 ? `Every ${hrs}h ${remMins}m` : `Every ${hrs}h`;
  }
  if (schedule.schedule_type === "cron") {
    return (
      describeCron(schedule.cron_expression, schedule.cron_timezone) ||
      schedule.cron_expression ||
      "Cron schedule"
    );
  }
  if (schedule.schedule_type === "table_trigger") {
    const name =
      schedule.trigger_full_table_name ??
      schedule.trigger_table_name ??
      `#${schedule.trigger_table_id}`;
    return `On refresh: ${name}`;
  }
  if (schedule.schedule_type === "table_set_trigger") {
    const names = schedule.trigger_full_table_names?.length
      ? schedule.trigger_full_table_names
      : (schedule.trigger_table_names ?? []);
    if (names.length > 0) return `Listens to: ${names.join(", ")}`;
    return `Listens to ${schedule.trigger_table_ids?.length ?? 0} tables`;
  }
  return schedule.schedule_type;
}

export function scheduleIcon(schedule: FlowSchedule): string {
  if (schedule.schedule_type === "interval") return "fa-solid fa-clock";
  if (schedule.schedule_type === "cron") return "fa-solid fa-calendar-day";
  if (schedule.schedule_type === "table_set_trigger") return "fa-solid fa-layer-group";
  return "fa-solid fa-table";
}

/**
 * Return a display name for a schedule: description if available,
 * otherwise the schedule type formatting, falling back to "Schedule #ID".
 */
export function getScheduleDisplayName(
  schedule: FlowSchedule | undefined,
  scheduleId: number,
): string {
  if (!schedule) return `Schedule #${scheduleId}`;
  if (schedule.name) return schedule.name;
  if (schedule.description) return schedule.description;
  return formatScheduleType(schedule);
}

export function formatRunType(
  runType: "in_designer_run" | "scheduled" | "manual" | "on_demand",
): string {
  if (runType === "scheduled") return "Scheduled";
  if (runType === "manual") return "Manual";
  if (runType === "on_demand") return "On-demand";
  return "Designer";
}

export function runTypeIcon(
  runType: "in_designer_run" | "scheduled" | "manual" | "on_demand",
): string {
  if (runType === "scheduled") return "fa-solid fa-calendar-days";
  if (runType === "manual") return "fa-solid fa-hand-pointer";
  if (runType === "on_demand") return "fa-solid fa-bolt";
  return "fa-solid fa-pencil-ruler";
}

// ===== Notification channels / rules / history =====

/** Display label for a notification channel type. */
export function channelTypeLabel(type: NotificationChannelType | string | null): string {
  if (type === "slack") return "Slack";
  if (type === "discord") return "Discord";
  if (type === "teams") return "Teams";
  if (type === "generic") return "Webhook";
  return type ?? "Webhook";
}

export function channelTypeIcon(type: NotificationChannelType | string | null): string {
  if (type === "slack") return "fa-brands fa-slack";
  if (type === "discord") return "fa-brands fa-discord";
  if (type === "teams") return "fa-brands fa-microsoft";
  return "fa-solid fa-link";
}

/** Modifier class for the per-type tag; pairs with the `.channel-tag` styles. */
export function channelTypeClass(type: NotificationChannelType | string | null): string {
  if (type === "slack" || type === "discord" || type === "teams") return type;
  return "generic";
}

export function notificationEventLabel(eventType: NotificationEventType | string): string {
  if (eventType === "run_failed") return "Run failed";
  if (eventType === "run_success") return "Run succeeded";
  if (eventType === "run_recovered") return "Run recovered";
  if (eventType === "run_orphaned") return "Run orphaned";
  return eventType;
}

/** One-line explanation of an event, shown as a tooltip in the delivery history. */
export function notificationEventDescription(eventType: NotificationEventType | string): string {
  if (eventType === "run_failed") return "The flow run ended with an error.";
  if (eventType === "run_success") return "The flow run completed successfully.";
  if (eventType === "run_recovered") return "The first successful run after a failure.";
  if (eventType === "run_orphaned")
    return "The run's process died before finishing and the run was closed as failed.";
  return "Flow run event.";
}

export function notificationEventIcon(eventType: NotificationEventType | string): string {
  if (eventType === "run_failed") return "fa-solid fa-circle-xmark";
  if (eventType === "run_success") return "fa-solid fa-circle-check";
  if (eventType === "run_recovered") return "fa-solid fa-heart-pulse";
  if (eventType === "run_orphaned") return "fa-solid fa-link-slash";
  return "fa-solid fa-bell";
}

/** `.status-badge` modifier for a delivery status: sent = green, dead = red, in-flight = amber. */
export function deliveryStatusClass(status: NotificationDeliveryStatus | string): string {
  if (status === "sent") return "success";
  if (status === "dead") return "failure";
  return "pending";
}

export function deliveryStatusLabel(status: NotificationDeliveryStatus | string): string {
  if (status === "sent") return "Sent";
  if (status === "dead") return "Failed";
  if (status === "sending") return "Sending";
  if (status === "pending") return "Pending";
  return status;
}
