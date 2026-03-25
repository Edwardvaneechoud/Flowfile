import type { FlowSchedule, GlobalArtifact } from "../../types";

/**
 * Compact date format: "Mar 23, 10:30 AM"
 * Used across catalog panels for run timestamps, schedule triggers, artifact dates.
 */
export function formatDate(dateStr: string): string {
  // Backend sends UTC timestamps; ensure JS parses them as UTC
  const normalized = dateStr.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(dateStr)
    ? dateStr
    : dateStr + "Z";
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

export function formatType(artifact: GlobalArtifact): string {
  if (artifact.python_type) {
    const parts = artifact.python_type.split(".");
    return parts[parts.length - 1];
  }
  return artifact.serialization_format ?? "unknown";
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
  if (schedule.schedule_type === "table_trigger") {
    const name = schedule.trigger_table_name ?? `#${schedule.trigger_table_id}`;
    return `On refresh: ${name}`;
  }
  if (schedule.schedule_type === "table_set_trigger") {
    const names = schedule.trigger_table_names ?? [];
    if (names.length > 0) return `Listens to: ${names.join(", ")}`;
    return `Listens to ${schedule.trigger_table_ids?.length ?? 0} tables`;
  }
  return schedule.schedule_type;
}

export function scheduleIcon(schedule: FlowSchedule): string {
  if (schedule.schedule_type === "interval") return "fa-solid fa-clock";
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

export function formatRunType(runType: "full_run" | "scheduled" | "manual"): string {
  if (runType === "scheduled") return "Scheduled";
  if (runType === "manual") return "Manual";
  return "Full Run";
}

export function runTypeIcon(runType: "full_run" | "scheduled" | "manual"): string {
  if (runType === "scheduled") return "fa-solid fa-calendar-days";
  if (runType === "manual") return "fa-solid fa-hand-pointer";
  return "fa-solid fa-play";
}
