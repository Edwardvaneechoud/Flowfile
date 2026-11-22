// The AI dashboard builder's per-step log; kept in the browser only.

export type LogStep = "plan" | "chart" | "story" | "place";
export type LogStatus = "running" | "ok" | "warning" | "failed" | "cancelled";

export interface LogTarget {
  provider: string | null;
  model: string | null;
}

export interface BuildLogEntry {
  /** `plan`, `chart:<check id>`, `story` or `place`. */
  key: string;
  step: LogStep;
  title: string;
  status: LogStatus;
  startedAt: number;
  endedAt: number | null;
  /** The provider and model that answered, else the ones asked for. */
  provider: string | null;
  model: string | null;
  /** What this step asked the model: the question, or a chart's brief. */
  asked?: string;
  /** The model's own words: a chart's explanation, or why it made no chart. */
  answer?: string;
  /** Why it failed, or what it noted, as the user is told. */
  detail?: string;
  /** Core's code for a degraded answer (`timeout`, `no_chart`, `invalid_chart`, …). */
  reason?: string | null;
  /** Model calls the answer took; 2 when core sent its problems back once. */
  attempts?: number;
  /** Core's codes for the silent repairs it made to the answer. */
  repairs?: string[];
  warnings?: string[];
}

export type EntryChange = Partial<Omit<BuildLogEntry, "key" | "step" | "startedAt">>;

export const PROMPT_LOG_HINT =
  "To see the full prompts and the models' replies, start Flowfile with " +
  "FLOWFILE_AI_LOG_PROMPTS=true and read them with: python -m flowfile_core.ai.prompt_log tail 20";

const STEP_LABELS: Record<LogStep, string> = {
  plan: "Plan",
  chart: "Chart",
  story: "Story",
  place: "Add to dashboard",
};

const STATUS_LABELS: Record<LogStatus, string> = {
  running: "running",
  ok: "done",
  warning: "done with notes",
  failed: "failed",
  cancelled: "cancelled",
};

export function startEntry(
  key: string,
  step: LogStep,
  title: string,
  target: LogTarget,
  now: number,
  asked?: string,
): BuildLogEntry {
  return {
    key,
    step,
    title,
    status: "running",
    startedAt: now,
    endedAt: null,
    provider: target.provider,
    model: target.model,
    ...(asked ? { asked } : {}),
  };
}

/** The entry with `change` applied; a settled status stamps the end once. */
export function finishEntry(entry: BuildLogEntry, change: EntryChange, now: number): BuildLogEntry {
  const next: BuildLogEntry = { ...entry };
  for (const [key, value] of Object.entries(change) as [keyof EntryChange, unknown][]) {
    if (value === undefined || value === null || value === "") continue;
    if (Array.isArray(value) && value.length === 0) continue;
    (next as unknown as Record<string, unknown>)[key] = value;
  }
  if (next.status !== "running" && next.endedAt === null) next.endedAt = now;
  return next;
}

/** `ok`, or `warning` when the answer came with warnings or needed a correction round. */
export function settledStatus(warnings: string[] | undefined, attempts?: number): LogStatus {
  return (warnings?.length ?? 0) > 0 || (attempts ?? 0) > 1 ? "warning" : "ok";
}

/** Every running entry settled with `change`, for a build that is over. */
export function settleRunning(
  entries: BuildLogEntry[],
  change: EntryChange,
  now: number,
): BuildLogEntry[] {
  return entries.map((e) => (e.status === "running" ? finishEntry(e, change, now) : e));
}

/** Every running entry marked cancelled, for a build that was stopped. */
export function cancelRunning(entries: BuildLogEntry[], now: number): BuildLogEntry[] {
  return settleRunning(entries, { status: "cancelled" }, now);
}

export function durationText(entry: BuildLogEntry): string {
  if (entry.endedAt === null) return "";
  const ms = Math.max(0, entry.endedAt - entry.startedAt);
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toFixed(1)} s`;
}

export function modelText(entry: BuildLogEntry): string {
  if (!entry.provider) return "";
  return entry.model ? `${entry.provider} (${entry.model})` : entry.provider;
}

export const stepLabel = (step: LogStep): string => STEP_LABELS[step];
export const statusLabel = (status: LogStatus): string => STATUS_LABELS[status];

/** A one-line count for the log's header, e.g. "6 steps · 1 failed · 41.3 s". */
export function logSummary(entries: BuildLogEntry[]): string {
  const failed = entries.filter((e) => e.status === "failed").length;
  const parts = [`${entries.length} step${entries.length === 1 ? "" : "s"}`];
  if (failed) parts.push(`${failed} failed`);
  const starts = entries.map((e) => e.startedAt);
  const ends = entries.map((e) => e.endedAt).filter((t): t is number => t !== null);
  if (starts.length && ends.length === entries.length) {
    parts.push(
      durationText({
        ...entries[0],
        startedAt: Math.min(...starts),
        endedAt: Math.max(...ends),
      }),
    );
  }
  return parts.join(" · ");
}

/** The detail lines of an entry, in the order the dialog shows them. */
export function entryLines(entry: BuildLogEntry): { label: string; text: string }[] {
  const lines: { label: string; text: string }[] = [];
  if (entry.asked) lines.push({ label: "Asked", text: entry.asked });
  if (entry.answer && !entry.detail?.includes(entry.answer)) {
    lines.push({ label: "The model said", text: entry.answer });
  }
  if (entry.detail)
    lines.push({ label: entry.status === "failed" ? "Why" : "Note", text: entry.detail });
  if (entry.reason) lines.push({ label: "Reason code", text: entry.reason });
  if (entry.attempts !== undefined && entry.attempts > 1) {
    lines.push({
      label: "Model calls",
      text: `${entry.attempts}: the first answer had problems and was sent back once`,
    });
  }
  if (entry.repairs?.length) lines.push({ label: "Repaired", text: entry.repairs.join(", ") });
  for (const warning of entry.warnings ?? []) lines.push({ label: "Warning", text: warning });
  return lines;
}

/** The whole log as plain text, for copying into a bug report. */
export function logAsText(entries: BuildLogEntry[]): string {
  const blocks = entries.map((entry) => {
    const head = [
      `${stepLabel(entry.step)}: ${entry.title}`,
      statusLabel(entry.status),
      durationText(entry),
      modelText(entry),
    ]
      .filter(Boolean)
      .join(" · ");
    const lines = entryLines(entry).map((line) => `  ${line.label}: ${line.text}`);
    return [head, ...lines].join("\n");
  });
  return [`AI dashboard build log (${logSummary(entries)})`, ...blocks].join("\n\n");
}
