import { describe, expect, it } from "vitest";

import {
  cancelRunning,
  durationText,
  entryLines,
  finishEntry,
  logAsText,
  logSummary,
  modelText,
  settledStatus,
  startEntry,
} from "./aiBuildLog";

const target = { provider: "openai", model: "gpt-x" };

describe("a log entry", () => {
  it("starts running with what was asked and the target model", () => {
    const entry = startEntry("chart:c1", "chart", "Map", target, 1000, "Map of points");
    expect(entry).toMatchObject({ status: "running", endedAt: null, asked: "Map of points" });
    expect(modelText(entry)).toBe("openai (gpt-x)");
    expect(durationText(entry)).toBe("");
  });

  it("stamps its end once and takes the answering model over the target", () => {
    const entry = startEntry("plan", "plan", "Plan", target, 1000);
    const done = finishEntry(entry, { status: "ok", provider: "openai", model: "gpt-y" }, 3400);
    expect(durationText(done)).toBe("2.4 s");
    expect(modelText(done)).toBe("openai (gpt-y)");
    expect(finishEntry(done, { detail: "later" }, 9000).endedAt).toBe(3400);
  });

  it("ignores empty values, so an older core's missing fields keep the target", () => {
    const entry = startEntry("plan", "plan", "Plan", target, 0);
    const done = finishEntry(
      entry,
      { status: "ok", provider: null, model: undefined, repairs: [], detail: "" },
      10,
    );
    expect(done).toMatchObject({ provider: "openai", model: "gpt-x" });
    expect(done.repairs).toBeUndefined();
    expect(done.detail).toBeUndefined();
    expect(durationText(done)).toBe("10 ms");
  });

  it("is done with notes after warnings or a correction round", () => {
    expect(settledStatus([], 1)).toBe("ok");
    expect(settledStatus(["profile missing"], 1)).toBe("warning");
    expect(settledStatus(undefined, 2)).toBe("warning");
  });
});

describe("the log", () => {
  const plan = finishEntry(
    startEntry("plan", "plan", "Plan", target, 0, "Q?"),
    { status: "ok" },
    1000,
  );
  const declined = finishEntry(
    startEntry("chart:c1", "chart", "Customer locations", target, 1000, "Map of lat and lon"),
    {
      status: "failed",
      reason: "no_chart",
      answer: "The chart options do not support maps.",
      detail: "The AI did not make a chart.",
      attempts: 2,
      repairs: ["shorthand"],
    },
    4000,
  );

  it("lists a failed chart's question, the model's words, the reason and the calls", () => {
    expect(entryLines(declined)).toEqual([
      { label: "Asked", text: "Map of lat and lon" },
      { label: "The model said", text: "The chart options do not support maps." },
      { label: "Why", text: "The AI did not make a chart." },
      { label: "Reason code", text: "no_chart" },
      { label: "Model calls", text: "2: the first answer had problems and was sent back once" },
      { label: "Repaired", text: "shorthand" },
    ]);
  });

  it("sums up steps, failures and the whole time", () => {
    expect(logSummary([plan, declined])).toBe("2 steps · 1 failed · 4.0 s");
    const running = startEntry("story", "story", "Write", target, 4000);
    expect(logSummary([plan, running])).toBe("2 steps");
  });

  it("marks the steps a cancel interrupted", () => {
    const running = startEntry("story", "story", "Write", target, 4000);
    const [kept, cancelled] = cancelRunning([plan, running], 5000);
    expect(kept).toBe(plan);
    expect(cancelled).toMatchObject({ status: "cancelled", endedAt: 5000 });
  });

  it("leaves out the model's words when the reason already quotes them", () => {
    const quoted = finishEntry(
      declined,
      { detail: "No chart: The chart options do not support maps." },
      4000,
    );
    expect(entryLines(quoted).map((l) => l.label)).not.toContain("The model said");
  });

  it("reads as plain text for a bug report", () => {
    const text = logAsText([plan, declined]);
    expect(text.split("\n")[0]).toBe("AI dashboard build log (2 steps · 1 failed · 4.0 s)");
    expect(text).toContain("Chart: Customer locations · failed · 3.0 s · openai (gpt-x)");
    expect(text).toContain("  Reason code: no_chart");
  });
});
