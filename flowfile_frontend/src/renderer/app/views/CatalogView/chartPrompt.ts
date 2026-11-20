import type { IChart } from "@kanaries/graphic-walker/interfaces";
import type { ChartGenerationResponse } from "../../api/ai.api";
import {
  chartFromRequest,
  describeChart,
  type ChartContext,
  type ChartRequest,
} from "./chartRequest";

/** The chart builder as the prompt bar reads it (`VueGraphicWalker.vue`'s exposed functions). */
export interface ChartBuilderHandle {
  applyChart(chart: IChart): boolean;
  currentChart(): IChart | null;
  datasetFields(): ChartContext["fields"] | null;
  graphicWalker(): ChartContext["gw"] | null;
}

/** A message under the bar: `tone` picks its colour, `detail` is a muted second line. */
export interface ChartPromptMessage {
  tone: "info" | "warning" | "error";
  text: string;
  detail?: string;
}

export const PLACEHOLDER_NEW = "Describe a chart";
/** The same line when data values are not sent (the user's choice or the server's rule). */
export const NAMES_ONLY_LINE = "The AI sees the column names and types only, no data values.";

export const PLACEHOLDER_CHANGE = "Describe a change";

/** The line that says what the AI sees. */
export const SENDS_LINE =
  "The AI sees the column names and types, summary statistics and some of the values.";

/** `a`, `a and b`, `a, b and c`. */
export function listPhrases(parts: string[]): string {
  if (parts.length <= 1) return parts[0] ?? "";
  return `${parts.slice(0, -1).join(", ")} and ${parts[parts.length - 1]}`;
}

/** What the bar says when the chart on screen cannot go along with the prompt. */
export function cannotCarryOverMessage(parts: string[]): ChartPromptMessage {
  return {
    tone: "warning",
    text: `This chart has ${listPhrases(parts)}, which the AI cannot carry over. Generate builds a new chart from your prompt alone; undo brings this one back.`,
  };
}

export interface PlannedRequest {
  /** The chart on screen as a chart request, or null for an empty builder or one it cannot describe. */
  currentChart: ChartRequest | null;
  /** The parts that stop the chart on screen from going along; empty when it can. */
  unsupported: string[];
}

/** What goes with the prompt: the described chart on screen, or nothing and the reasons why. */
export function planRequest(
  current: IChart | null,
  ctx: Pick<ChartContext, "fields" | "gw">,
): PlannedRequest {
  if (!current) return { currentChart: null, unsupported: [] };
  const described = describeChart(current, ctx);
  return described.ok
    ? { currentChart: described.request, unsupported: [] }
    : { currentChart: null, unsupported: described.unsupported };
}

const providerErrorName = (message: string | null): string | null =>
  message ? (/\(([A-Za-z_][\w.]*)\)\.?\s*$/.exec(message)?.[1] ?? null) : null;

const UNUSABLE_ANSWER =
  "The AI's answer was not a chart this builder can draw. Try again, or put it another way.";

/** The message for an answer core marked degraded; the chart on screen is left as it is. */
export function degradedMessage(response: ChartGenerationResponse): ChartPromptMessage {
  const serverMessage = response.message?.trim() || null;
  switch (response.reason) {
    case "empty_prompt":
      return { tone: "error", text: "Describe the chart you want first." };
    case "no_columns":
      return { tone: "error", text: "This table has no columns the AI can chart." };
    case "timeout":
      return {
        tone: "error",
        text: "The AI took too long to answer. Try again, or ask for something simpler.",
      };
    case "provider_error": {
      const name = providerErrorName(serverMessage);
      return {
        tone: "error",
        text: `The AI provider could not be reached or returned an error${name ? ` (${name})` : ""}. Check it under Settings → AI and try again.`,
      };
    }
    case "empty_response":
      return { tone: "error", text: "The AI sent back an empty answer. Try again." };
    case "no_chart": {
      const why = response.explanation?.trim() || serverMessage;
      return {
        tone: "warning",
        text: why
          ? `The AI did not make a chart: ${why}`
          : "The AI did not make a chart for this. Try putting it another way.",
      };
    }
    case "parse_error":
    case "validation_error":
    case "invalid_chart":
      return {
        tone: "error",
        text: UNUSABLE_ANSWER,
        ...(serverMessage ? { detail: serverMessage } : {}),
      };
    default:
      return {
        tone: "error",
        text: serverMessage ?? "The AI could not make a chart. Try again.",
      };
  }
}

export type ChartAnswer =
  | { ok: true; chart: IChart; title: string | null; explanation: string | null }
  | { ok: false; message: ChartPromptMessage };

/** Turn core's answer into a chart for `applyChart`, or the message saying why it cannot be drawn. */
export function chartFromAnswer(response: ChartGenerationResponse, ctx: ChartContext): ChartAnswer {
  if (response.degraded || !response.chart) {
    return { ok: false, message: degradedMessage(response) };
  }
  const built = chartFromRequest(response.chart, ctx);
  if (!built.ok) {
    return {
      ok: false,
      message: {
        tone: "error",
        text: "The AI's chart could not be drawn here. Try again, or put it another way.",
        detail: built.error,
      },
    };
  }
  return {
    ok: true,
    chart: built.chart,
    title: response.title?.trim() || null,
    explanation: response.explanation?.trim() || null,
  };
}

const httpDetail = (error: unknown): string | null => {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const first = detail[0] as { msg?: unknown } | undefined;
    return typeof first?.msg === "string" ? first.msg : null;
  }
  return null;
};

/** The message for a request that never got an answer from the route (network, refusal, timeout). */
export function requestFailureMessage(error: unknown): ChartPromptMessage {
  const err = error as { response?: { status?: number }; code?: string } | null;
  const status = err?.response?.status;
  if (status === undefined) {
    if (err?.code === "ECONNABORTED" || err?.code === "ETIMEDOUT") {
      return {
        tone: "error",
        text: "No answer came back in time, so the request was stopped. Try again.",
      };
    }
    return {
      tone: "error",
      text: "Could not reach the Flowfile server. Check the connection and try again.",
    };
  }
  const detail = httpDetail(error);
  switch (status) {
    case 403:
      return { tone: "error", text: "You may not read this table, so the AI cannot chart it." };
    case 404:
      return {
        tone: "error",
        text: "The table or the AI provider was not found.",
        ...(detail ? { detail } : {}),
      };
    case 409:
      return { tone: "error", text: "No AI provider is set up. Add one under Settings → AI." };
    case 422:
      return {
        tone: "error",
        text: "The request was refused. Shorten the prompt and try again.",
        ...(detail ? { detail } : {}),
      };
    default:
      return {
        tone: "error",
        text: `Something went wrong on the server (${status}). Try again.`,
        ...(detail ? { detail } : {}),
      };
  }
}

export const AI_OFF_MESSAGE: ChartPromptMessage = {
  tone: "error",
  text: "AI features are turned off. An administrator can turn them on under Settings → AI.",
};

export const CANCELLED_MESSAGE: ChartPromptMessage = {
  tone: "info",
  text: "Cancelled. The chart is unchanged.",
};

export const BUILDER_RELOADED_MESSAGE: ChartPromptMessage = {
  tone: "warning",
  text: "The chart builder reloaded while the AI was working, so its answer was not applied. Generate again to use it.",
};

export const EDITED_MEANWHILE_MESSAGE: ChartPromptMessage = {
  tone: "warning",
  text: "You changed the chart while the AI was working, so its answer was not applied.",
};

export const APPLY_FAILED_MESSAGE: ChartPromptMessage = {
  tone: "error",
  text: "The chart builder did not take the AI's chart. The chart is unchanged.",
};

/** A stable text for a chart, to tell whether the user changed it while a request was out. */
export const chartKey = (chart: IChart | null): string => (chart ? JSON.stringify(chart) : "");
