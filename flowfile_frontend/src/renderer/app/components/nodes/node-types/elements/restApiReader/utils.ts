import type { RestApiSettings, NodeRestApiReader } from "../../../../../types/node.types";

export const createNodeRestApiReader = (flowId: number, nodeId: number): NodeRestApiReader => {
  const settings: RestApiSettings = {
    url: "",
    method: "GET",
    headers: {},
    query_params: {},
    json_body: null,
    auth: {
      auth_type: "none",
      api_key_name: "X-API-Key",
      api_key_location: "header",
      basic_username: "",
      secret_name: "",
      secret: null,
    },
    pagination: {
      pagination_type: "none",
      offset_param: "offset",
      limit_param: "limit",
      page_size: 100,
      page_param: "page",
      start_page: 1,
      cursor_param: "cursor",
      cursor_location: "body",
      cursor_response_path: "",
      initial_cursor: "",
      max_pages: 1000,
      max_records: null,
      page_delay_seconds: 0,
    },
    record_path: "",
    timeout_seconds: 30,
    max_retries: 3,
  };
  return {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    rest_api_settings: settings,
    cache_results: false,
    fields: [],
  };
};
