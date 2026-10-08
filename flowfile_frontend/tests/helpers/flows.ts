import { expect, APIRequestContext } from "@playwright/test";

import { API_URL, authHeaders } from "./api";

/** Flow fixtures built through the editor API, shared by the notebook and multi-window specs. */

export async function postEditor(
  request: APIRequestContext,
  token: string,
  path: string,
  params: Record<string, unknown>,
  data?: unknown,
  extraHeaders: Record<string, string> = {},
) {
  const query = new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]));
  const response = await request.post(`${API_URL}${path}?${query}`, {
    headers: { ...authHeaders(token), ...extraHeaders },
    data,
  });
  expect(response.ok(), `${path}: ${await response.text()}`).toBe(true);
  return response;
}

/** The filter node's settings on the canvas, as `{flow_id, node_id, ...}` from the `/node` route. */
export async function filterSettings(
  request: APIRequestContext,
  token: string,
  flowId: number,
  nodeId = 2,
) {
  const response = await request.get(
    `${API_URL}/node?flow_id=${flowId}&node_id=${nodeId}&get_data=false&include_output=false`,
    { headers: authHeaders(token) },
  );
  expect(response.ok()).toBe(true);
  return (await response.json()).setting_input as {
    filter_input: { basic_filter: { field: string; operator: string; value: string } };
  };
}

/** Save the filter node's threshold; `client` is the X-Flowfile-Client the change is made as. */
export async function setFilterValue(
  request: APIRequestContext,
  token: string,
  flowId: number,
  value: string,
  client = "other-window",
) {
  await postEditor(
    request,
    token,
    "/update_settings/",
    { node_type: "filter" },
    {
      flow_id: flowId,
      node_id: 2,
      depending_on_id: 1,
      filter_input: {
        mode: "basic",
        basic_filter: { field: "salary", operator: ">", value },
      },
    },
    { "X-Flowfile-Client": client },
  );
}

/** manual_input (1, four rows) -> filter (2, salary > 60000), through the editor API. */
export async function buildSalaryFlow(request: APIRequestContext, token: string, flowId: number) {
  const add = (id: number, type: string, x: number) =>
    postEditor(request, token, "/editor/add_node/", {
      flow_id: flowId,
      node_id: id,
      node_type: type,
      pos_x: x,
      pos_y: 200,
    });
  await add(1, "manual_input", 100);
  await add(2, "filter", 400);
  await postEditor(
    request,
    token,
    "/update_settings/",
    { node_type: "manual_input" },
    {
      flow_id: flowId,
      node_id: 1,
      raw_data_format: {
        columns: [
          { name: "id", data_type: "Integer" },
          { name: "salary", data_type: "Integer" },
        ],
        data: [
          [1, 2, 3, 4],
          [50000, 75000, 90000, 65000],
        ],
      },
    },
  );
  await postEditor(
    request,
    token,
    "/editor/connect_node/",
    { flow_id: flowId },
    {
      input_connection: { node_id: 2, connection_class: "input-0" },
      output_connection: { node_id: 1, connection_class: "output-0" },
    },
  );
  await setFilterValue(request, token, flowId, "60000", "fixture");
}
