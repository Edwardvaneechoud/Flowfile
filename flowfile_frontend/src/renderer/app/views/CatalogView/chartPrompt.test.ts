import { describe, expect, it } from "vitest";
import type { IChart, IMutField } from "@kanaries/graphic-walker/interfaces";
import { normalize } from "@kanaries/graphic-walker/models/normalize";
import {
  expandTerse,
  parseShorthand,
  projectTerse,
  terseComputedFid,
} from "@kanaries/graphic-walker/models/terse";
import type { ChartGenerationResponse } from "../../api/ai.api";
import {
  cannotCarryOverMessage,
  chartFromAnswer,
  chartKey,
  degradedMessage,
  listPhrases,
  planRequest,
  requestFailureMessage,
} from "./chartPrompt";
import { chartFromRequest, type GraphicWalkerFunctions } from "./chartRequest";

const gw: GraphicWalkerFunctions = {
  normalize,
  expandTerse,
  projectTerse,
  parseShorthand,
  terseComputedFid,
};

const FIELDS: IMutField[] = [
  { fid: "region", name: "region", semanticType: "nominal", analyticType: "dimension" },
  { fid: "amount", name: "amount", semanticType: "quantitative", analyticType: "measure" },
];

const response = (over: Partial<ChartGenerationResponse>): ChartGenerationResponse => ({
  chart: null,
  title: null,
  explanation: null,
  warnings: [],
  degraded: true,
  reason: null,
  message: null,
  ...over,
});

const barChart = (): IChart => {
  const built = chartFromRequest(
    { mark: "bar", x: { field: "region" }, y: { field: "amount", aggregate: "sum" } },
    { fields: FIELDS, current: null, gw },
  );
  if (!built.ok) throw new Error(built.error);
  return built.chart;
};

describe("degradedMessage", () => {
  it("gives every reason its own plain message", () => {
    const reasons = [
      "empty_prompt",
      "no_columns",
      "timeout",
      "provider_error",
      "empty_response",
      "no_chart",
      "parse_error",
    ] as const;
    const texts = reasons.map((reason) => degradedMessage(response({ reason })).text);
    expect(new Set(texts).size).toBe(reasons.length);
    expect(texts[2]).toBe(
      "The AI took too long to answer. Try again, or ask for something simpler.",
    );
  });

  it("names the provider's error class without the rest of the server text", () => {
    const message = degradedMessage(
      response({
        reason: "provider_error",
        message: "The AI provider call failed (APIConnectionError).",
      }),
    );
    expect(message.text).toBe(
      "The AI provider could not be reached or returned an error (APIConnectionError). Check it under Settings → AI and try again.",
    );
  });

  it("passes on the model's own reason for not making a chart", () => {
    expect(
      degradedMessage(response({ reason: "no_chart", explanation: "There is no date column." }))
        .text,
    ).toBe("The AI did not make a chart: There is no date column.");
    expect(
      degradedMessage(response({ reason: "no_chart", message: "Only from the message." })).text,
    ).toBe("The AI did not make a chart: Only from the message.");
  });

  it("keeps the checks' sentences as detail for an answer that was not a usable chart", () => {
    for (const reason of ["parse_error", "validation_error", "invalid_chart"] as const) {
      const message = degradedMessage(response({ reason, message: '"revenu" is not a column.' }));
      expect(message.tone).toBe("error");
      expect(message.detail).toBe('"revenu" is not a column.');
    }
  });

  it("falls back to the server's message for a reason it does not know", () => {
    expect(degradedMessage(response({ reason: null, message: "Odd." })).text).toBe("Odd.");
  });
});

describe("requestFailureMessage", () => {
  it("tells a network failure from the client's own timeout", () => {
    expect(requestFailureMessage({ code: "ERR_NETWORK" }).text).toContain(
      "Could not reach the Flowfile server",
    );
    expect(requestFailureMessage({ code: "ECONNABORTED" }).text).toContain(
      "No answer came back in time",
    );
  });

  it("words each refusal of the route", () => {
    const status = (code: number, detail?: unknown) =>
      requestFailureMessage({ response: { status: code, data: { detail } } });
    expect(status(403).text).toBe("You may not read this table, so the AI cannot chart it.");
    expect(status(404, "Catalog table not found")).toEqual({
      tone: "error",
      text: "The table or the AI provider was not found.",
      detail: "Catalog table not found",
    });
    expect(status(409).text).toBe("No AI provider is set up. Add one under Settings → AI.");
    expect(status(422, [{ msg: "String should have at most 2000 characters" }]).detail).toBe(
      "String should have at most 2000 characters",
    );
    expect(status(500).text).toBe("Something went wrong on the server (500). Try again.");
  });
});

describe("the bar's wording", () => {
  it("lists the parts it cannot carry over", () => {
    expect(listPhrases(["a map"])).toBe("a map");
    expect(listPhrases(["a map", "a formula field", "a text label"])).toBe(
      "a map, a formula field and a text label",
    );
    expect(cannotCarryOverMessage(["a map", "a pattern filter"])).toEqual({
      tone: "warning",
      text: "This chart has a map and a pattern filter, which the AI cannot carry over. Generate builds a new chart from your prompt alone; undo brings this one back.",
    });
  });
});

describe("planRequest", () => {
  it("sends nothing for an empty builder", () => {
    expect(planRequest(null, { fields: FIELDS, gw })).toEqual({
      currentChart: null,
      unsupported: [],
    });
  });

  it("sends the chart on screen as a chart request", () => {
    expect(planRequest(barChart(), { fields: FIELDS, gw })).toEqual({
      currentChart: {
        mark: "bar",
        x: { field: "region" },
        y: { field: "amount", aggregate: "sum" },
      },
      unsupported: [],
    });
  });

  it("sends the prompt alone and says why when the chart cannot be described", () => {
    const chart = barChart();
    chart.encodings.details = [chart.encodings.dimensions[0]];
    expect(planRequest(chart, { fields: FIELDS, gw })).toEqual({
      currentChart: null,
      unsupported: ["a detail field"],
    });
  });
});

describe("chartFromAnswer", () => {
  it("builds the chart, carrying the current chart's name", () => {
    const current = { ...barChart(), name: "My chart" };
    const answer = chartFromAnswer(
      response({
        degraded: false,
        chart: { mark: "line", x: { field: "region" }, y: { field: "amount", aggregate: "sum" } },
        title: " Revenue ",
        explanation: "As a line.",
      }),
      { fields: FIELDS, current, gw },
    );
    expect(answer.ok).toBe(true);
    if (!answer.ok) return;
    expect(answer.chart.config.geoms).toEqual(["line"]);
    expect(answer.chart.name).toBe("My chart");
    expect(answer.title).toBe("Revenue");
    expect(answer.explanation).toBe("As a line.");
  });

  it("says why a chart the converter refuses cannot be drawn", () => {
    const answer = chartFromAnswer(
      response({ degraded: false, chart: { mark: "bar", x: { field: "nope" } } }),
      { fields: FIELDS, current: null, gw },
    );
    expect(answer).toEqual({
      ok: false,
      message: {
        tone: "error",
        text: "The AI's chart could not be drawn here. Try again, or put it another way.",
        detail: '"nope" is not a column of this table.',
      },
    });
  });

  it("gives a degraded answer's message", () => {
    const answer = chartFromAnswer(response({ reason: "timeout" }), {
      fields: FIELDS,
      current: null,
      gw,
    });
    expect(answer.ok).toBe(false);
  });
});

describe("chartKey", () => {
  it("is empty for no chart and changes with the chart", () => {
    const chart = barChart();
    expect(chartKey(null)).toBe("");
    expect(chartKey(chart)).toBe(chartKey(JSON.parse(JSON.stringify(chart))));
    expect(chartKey({ ...chart, name: "Other" })).not.toBe(chartKey(chart));
  });
});
