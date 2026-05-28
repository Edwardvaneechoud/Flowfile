/**
 * Friendly schedule builder <-> cron expression.
 *
 * The UI never asks users to write cron. They pick a frequency (every N
 * minutes, hourly, daily, weekly, monthly) plus a time / day; `buildCron`
 * turns that into a standard 5-field cron string for the backend. `parseCron`
 * does the reverse so an existing schedule re-opens in the right builder
 * state — anything we don't recognise (incl. hand-written cron) falls back to
 * the "Custom" mode that shows the raw expression. `describeCron` renders any
 * expression as plain English via cronstrue for previews and list rows.
 */
import cronstrue from "cronstrue";

export type CronFrequency = "minutes" | "hourly" | "daily" | "weekly" | "monthly" | "custom";

export interface CronBuilderState {
  frequency: CronFrequency;
  everyN: number; // minutes (1-59) or hours (1-23), depending on frequency
  time: string; // "HH:mm" for daily / weekly / monthly
  weekdays: number[]; // cron day-of-week values 0-6 (0 = Sunday) for weekly
  dayOfMonth: number; // 1-31 for monthly
  expression: string; // raw cron, used in "custom" mode
}

/** Weekday options in Mon-first display order; `value` is the cron day-of-week number. */
export const WEEKDAYS: { value: number; label: string; long: string }[] = [
  { value: 1, label: "Mon", long: "Monday" },
  { value: 2, label: "Tue", long: "Tuesday" },
  { value: 3, label: "Wed", long: "Wednesday" },
  { value: 4, label: "Thu", long: "Thursday" },
  { value: 5, label: "Fri", long: "Friday" },
  { value: 6, label: "Sat", long: "Saturday" },
  { value: 0, label: "Sun", long: "Sunday" },
];

export const FREQUENCY_OPTIONS: { value: CronFrequency; label: string }[] = [
  { value: "minutes", label: "Every N minutes" },
  { value: "hourly", label: "Hourly" },
  { value: "daily", label: "Daily" },
  { value: "weekly", label: "Weekly" },
  { value: "monthly", label: "Monthly" },
  { value: "custom", label: "Custom (cron)" },
];

export function defaultCronState(): CronBuilderState {
  return {
    frequency: "daily",
    everyN: 15,
    time: "09:00",
    weekdays: [1], // Monday
    dayOfMonth: 1,
    expression: "0 9 * * *",
  };
}

/** Detect the user's IANA timezone (e.g. "Europe/Amsterdam"); falls back to UTC. */
export function localTimezone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

function parseTime(time: string): { minute: number; hour: number } {
  const [h, m] = (time || "09:00").split(":");
  const hour = Math.min(23, Math.max(0, parseInt(h, 10) || 0));
  const minute = Math.min(59, Math.max(0, parseInt(m, 10) || 0));
  return { minute, hour };
}

function formatTime(minute: number, hour: number): string {
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${pad(hour)}:${pad(minute)}`;
}

/** Build a 5-field cron expression from friendly builder state. */
export function buildCron(state: CronBuilderState): string {
  const { minute, hour } = parseTime(state.time);
  switch (state.frequency) {
    case "minutes": {
      const n = Math.min(59, Math.max(1, Math.round(state.everyN)));
      return n === 1 ? "* * * * *" : `*/${n} * * * *`;
    }
    case "hourly": {
      const n = Math.min(23, Math.max(1, Math.round(state.everyN)));
      return n === 1 ? "0 * * * *" : `0 */${n} * * *`;
    }
    case "daily":
      return `${minute} ${hour} * * *`;
    case "weekly": {
      const days = state.weekdays.length ? [...state.weekdays].sort((a, b) => a - b) : [1];
      return `${minute} ${hour} * * ${days.join(",")}`;
    }
    case "monthly": {
      const dom = Math.min(31, Math.max(1, Math.round(state.dayOfMonth)));
      return `${minute} ${hour} ${dom} * *`;
    }
    case "custom":
    default:
      return (state.expression || "").trim();
  }
}

const INT_RE = /^\d+$/;
const STEP_RE = /^\*\/(\d+)$/;

/**
 * Parse a cron expression back into builder state. Recognises the shapes that
 * `buildCron` emits; anything else (ranges, multiple step fields, hand-written
 * cron) returns a "custom" state carrying the raw expression.
 */
export function parseCron(expression: string): CronBuilderState {
  const base = defaultCronState();
  const expr = (expression || "").trim();
  base.expression = expr;
  const parts = expr.split(/\s+/);
  if (parts.length !== 5) {
    base.frequency = "custom";
    return base;
  }
  const [min, hr, dom, mon, dow] = parts;

  // Every N minutes: "* * * * *" or "*/N * * * *" (other fields wildcard)
  if (hr === "*" && dom === "*" && mon === "*" && dow === "*") {
    if (min === "*") return { ...base, frequency: "minutes", everyN: 1 };
    const step = min.match(STEP_RE);
    if (step) return { ...base, frequency: "minutes", everyN: parseInt(step[1], 10) };
  }

  // Hourly: "0 * * * *" or "0 */N * * *"
  if (min === "0" && dom === "*" && mon === "*" && dow === "*") {
    if (hr === "*") return { ...base, frequency: "hourly", everyN: 1 };
    const step = hr.match(STEP_RE);
    if (step) return { ...base, frequency: "hourly", everyN: parseInt(step[1], 10) };
  }

  // Time-of-day shapes need numeric minute + hour
  if (INT_RE.test(min) && INT_RE.test(hr)) {
    const time = formatTime(parseInt(min, 10), parseInt(hr, 10));

    // Daily: "m h * * *"
    if (dom === "*" && mon === "*" && dow === "*") {
      return { ...base, frequency: "daily", time };
    }
    // Weekly: "m h * * <days>" — comma-separated single weekday numbers
    if (dom === "*" && mon === "*" && dow !== "*") {
      const days = dow.split(",");
      if (days.every((d) => INT_RE.test(d))) {
        return {
          ...base,
          frequency: "weekly",
          time,
          weekdays: days.map((d) => parseInt(d, 10) % 7),
        };
      }
    }
    // Monthly: "m h <dom> * *"
    if (INT_RE.test(dom) && mon === "*" && dow === "*") {
      return { ...base, frequency: "monthly", time, dayOfMonth: parseInt(dom, 10) };
    }
  }

  base.frequency = "custom";
  return base;
}

/**
 * Plain-English description of a cron expression (e.g. "At 02:00 AM" or
 * "Every 15 minutes"), optionally suffixed with the timezone. Returns an empty
 * string for an invalid/blank expression so callers can hide the preview.
 */
export function describeCron(
  expression: string | null | undefined,
  timezone?: string | null,
): string {
  const expr = (expression || "").trim();
  if (!expr) return "";
  try {
    const text = cronstrue.toString(expr, { verbose: false, throwExceptionOnParseError: true });
    return timezone ? `${text} (${timezone})` : text;
  } catch {
    return "";
  }
}
