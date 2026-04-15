import type { GoogleAnalyticsSettings, NodeGoogleAnalyticsReader } from "../../../../../types/node.types";

export const createNodeGoogleAnalyticsReader = (
  flowId: number,
  nodeId: number,
): NodeGoogleAnalyticsReader => {
  const settings: GoogleAnalyticsSettings = {
    ga_connection_name: "",
    property_id: "",
    start_date: "7daysAgo",
    end_date: "yesterday",
    metrics: [],
    dimensions: [],
    limit: null,
  };
  return {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    google_analytics_settings: settings,
    cache_results: false,
    fields: [],
  };
};
