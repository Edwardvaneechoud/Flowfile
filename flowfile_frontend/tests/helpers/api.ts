import { APIRequestContext, expect } from "@playwright/test";

/** Shared REST helpers for specs that drive the UI against a real flowfile_core. */

export const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
export const API_URL = process.env.API_URL || "http://localhost:63578";

export const authHeaders = (token: string) => ({ Authorization: `Bearer ${token}` });

export async function getAuthToken(request: APIRequestContext): Promise<string> {
  const response = await request.post(`${API_URL}/auth/token`);
  if (!response.ok()) throw new Error(`Failed to get auth token: ${response.status()}`);
  return (await response.json()).access_token;
}

export async function createFlow(
  request: APIRequestContext,
  token: string,
  name: string,
): Promise<number> {
  const response = await request.post(
    `${API_URL}/editor/create_flow/?name=${encodeURIComponent(name)}&register_in_catalog=false`,
    { headers: authHeaders(token) },
  );
  if (!response.ok()) throw new Error(`create_flow failed: ${response.status()}`);
  return response.json();
}

export async function closeFlow(request: APIRequestContext, token: string, flowId: number) {
  await request.post(`${API_URL}/editor/close_flow/?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
}

/** Node ids of a flow grouped by node type, each list in creation (id) order. */
export async function nodeIdsByType(
  request: APIRequestContext,
  token: string,
  flowId: number,
): Promise<Record<string, string[]>> {
  const response = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  const byType: Record<string, string[]> = {};
  for (const node of (await response.json()).node_inputs ?? []) {
    (byType[node.item] ??= []).push(String(node.id));
  }
  for (const ids of Object.values(byType)) ids.sort((a, b) => Number(a) - Number(b));
  return byType;
}

/** The flow's connections as `source->target` node-id pairs. */
export async function flowEdges(
  request: APIRequestContext,
  token: string,
  flowId: number,
): Promise<string[]> {
  const response = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  return ((await response.json()).node_edges ?? []).map((e: any) => `${e.source}->${e.target}`);
}

/** The node's saved settings as the backend holds them (not what the drawer shows). */
export async function readNodeSettings(
  request: APIRequestContext,
  token: string,
  flowId: number,
  nodeId: string,
): Promise<any> {
  const response = await request.get(
    `${API_URL}/node?flow_id=${flowId}&node_id=${nodeId}&get_data=false`,
    { headers: authHeaders(token) },
  );
  expect(response.ok()).toBe(true);
  return (await response.json())?.setting_input;
}

export interface NodeStepResult {
  node_id: number | string;
  success: boolean | null;
  error?: string | null;
}

export interface RunInfo {
  start_time: string | null;
  success: boolean | null;
  is_running: boolean;
  run_type: string;
  node_step_result: NodeStepResult[];
}

async function runStatus(request: APIRequestContext, token: string, flowId: number) {
  return request.get(`${API_URL}/flow/run_status/?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
}

/** Start time of the flow's latest run, or null before its first run. */
export async function lastRunStart(
  request: APIRequestContext,
  token: string,
  flowId: number,
): Promise<string | null> {
  const response = await runStatus(request, token, flowId);
  return response.ok() ? (await response.json()).start_time : null;
}

/**
 * Wait for a full run that started after `previousStart` to finish. Keying on the start time
 * keeps a finished earlier run from passing for the one just requested.
 */
export async function waitForRun(
  request: APIRequestContext,
  token: string,
  flowId: number,
  previousStart: string | null,
  timeout = 120_000,
): Promise<RunInfo> {
  let info: RunInfo | undefined;
  await expect
    .poll(
      async () => {
        const response = await runStatus(request, token, flowId);
        if (response.status() !== 200) return false;
        info = await response.json();
        return (
          !!info &&
          info.run_type === "full_run" &&
          info.start_time !== previousStart &&
          !info.is_running &&
          info.success !== null
        );
      },
      { timeout, intervals: [500] },
    )
    .toBe(true);
  return info as RunInfo;
}

export function nodeResult(info: RunInfo, nodeId: string): NodeStepResult | undefined {
  return info.node_step_result.find((n) => String(n.node_id) === nodeId);
}
