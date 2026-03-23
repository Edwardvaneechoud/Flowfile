import type { FlowSchedule } from "../../types";

/**
 * Compact date format: "Mar 23, 10:30 AM"
 * Used across catalog panels for run timestamps, schedule triggers, artifact dates.
 */
export function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
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
