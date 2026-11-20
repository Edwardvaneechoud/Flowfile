import { test, expect, APIRequestContext, Page } from "@playwright/test";

/**
 * Canvas clipboard routing tests.
 *
 * Regression suite for the copy/paste refactor (sentinel clipboard + panel
 * denylist): a paste aimed at settings-panel chrome must never mutate the
 * canvas, a stale node buffer must never be resurrected by non-matching
 * clipboard text, and a matching sentinel must paste the node.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578)
 *   2. Frontend dev server: `npm run dev:web` (port 8080)
 *   3. Run: `npx playwright test tests/canvas-clipboard.spec.ts`
 *      (NOT covered by `make test_e2e`, which runs only web-flow.spec.ts.)
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";

const SENTINEL_ID = "e2e-clipboard-sentinel";
const SENTINEL_TEXT = `Flowfile nodes · 1 · ${SENTINEL_ID}`;

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  return (await tokenResponse.json()).access_token;
}

async function authPost(request: APIRequestContext, url: string, token: string) {
  return request.post(url, { headers: { Authorization: `Bearer ${token}` } });
}

/** Create a fresh flow with one manual_input node (node_id 1) and return its id. */
async function createFlowWithNode(request: APIRequestContext, token: string): Promise<number> {
  const createResponse = await authPost(
    request,
    `${API_URL}/editor/create_flow/?name=Clipboard_E2E&register_in_catalog=false`,
    token,
  );
  expect(createResponse.ok()).toBe(true);
  const flowId = await createResponse.json();
  const addResponse = await authPost(
    request,
    `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=manual_input&pos_x=120&pos_y=140`,
    token,
  );
  expect(addResponse.ok()).toBe(true);
  return flowId;
}

/** The armed node-copy buffer, in the useFlowClipboard localStorage format. */
function bufferSeed(flowId: number): string {
  return JSON.stringify({
    sentinelId: SENTINEL_ID,
    flowId,
    copiedAt: 0,
    single: {
      nodeIdToCopyFrom: 1,
      type: "manual_input",
      label: "Manual Input",
      description: "",
      numberOfInputs: 0,
      numberOfOutputs: 1,
      typeSnakeCase: "manual_input",
      flowIdToCopyFrom: flowId,
      multi: false,
      inputHandles: [],
      outputHandles: [{ id: "output-0", position: "right" }],
    },
  });
}

/** Boot the app authenticated, with the flow open and the buffer armed. */
async function openFlow(page: Page, token: string, flowId: number) {
  await page.goto(BASE_URL);
  await page.waitForLoadState("load");
  const expiration = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({ token, expiration, flowId, buffer }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
      localStorage.setItem("flowfileNodeClipboard", buffer);
      sessionStorage.setItem("last_flow_id", String(flowId));
    },
    { token, expiration, flowId, buffer: bufferSeed(flowId) },
  );
  await page.reload();
  await page.waitForSelector(".vue-flow__node", { timeout: 15000 });
}

/** Dispatch a synthetic ClipboardEvent carrying `text` on `selector`. */
async function dispatchClipboardEvent(
  page: Page,
  selector: string,
  type: "copy" | "paste",
  text: string,
) {
  const dispatched = await page.evaluate(
    ({ selector, type, text }) => {
      const target = document.querySelector(selector);
      if (!target) return false;
      const clipboardData = new DataTransfer();
      if (type === "paste") clipboardData.setData("text/plain", text);
      const event = new ClipboardEvent(type, { clipboardData, bubbles: true, cancelable: true });
      target.dispatchEvent(event);
      // dispatchEvent returns only after the synchronous document handler returns.
      return { prevented: event.defaultPrevented };
    },
    { selector, type, text },
  );
  expect(dispatched).not.toBe(false);
  return dispatched as { prevented: boolean };
}

async function readGraph(request: APIRequestContext, token: string, flowId: number) {
  const response = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(response.ok(), await response.text()).toBe(true);
  return response.json();
}

async function reloadCounter(page: Page): Promise<number> {
  // Read the real store; do not stub requestReload or suppress recovery.
  return page.evaluate(async () => {
    const modulePath = "/app/stores/flow-store.ts";
    const { useFlowStore } = await import(/* @vite-ignore */ modulePath);
    const store = useFlowStore();
    const audit = window as unknown as { clipboardReloadStacks?: string[] };
    if (!audit.clipboardReloadStacks) {
      audit.clipboardReloadStacks = [];
      store.$onAction(({ name }: { name: string }) => {
        if (name === "requestReload") audit.clipboardReloadStacks!.push(new Error("requestReload observed").stack ?? "no stack");
      }, true);
    }
    return store.pendingReloadCounter;
  });
}

async function paletteChrome(page: Page) {
  const selector = "#dataActions[data-canvas-overlay] > .header";
  await expect(page.locator(selector)).toBeVisible();
  await expect(page.locator(selector)).toContainText("Data actions");
  return selector;
}

async function expectIgnoredPaste(
  page: Page,
  request: APIRequestContext,
  token: string,
  flowId: number,
  selector: string,
  text: string,
) {
  const graphBefore = await readGraph(request, token, flowId);
  const countBefore = await nodeCount(page);
  const mutations: string[] = [];
  const observe = (req: import("@playwright/test").Request) => {
    if (!["GET", "HEAD", "OPTIONS"].includes(req.method()) &&
        /\/(editor\/|update_settings\/|node\/)/.test(new URL(req.url()).pathname)) {
      mutations.push(`${req.method()} ${req.url()}`);
    }
  };
  page.on("request", observe);
  try {
    const dispatched = await dispatchClipboardEvent(page, selector, "paste", text);
    // These paths return synchronously before handleCanvasPaste is scheduled.
    expect(dispatched.prevented).toBe(false);
    expect(await readGraph(request, token, flowId)).toEqual(graphBefore);
    expect(await nodeCount(page)).toBe(countBefore);
    expect(mutations).toEqual([]);
  } finally {
    page.off("request", observe);
  }
}

const nodeCount = (page: Page) => page.locator(".vue-flow__node").count();

test.describe("Canvas clipboard routing", () => {
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  for (const mode of ["single-node", "multi-node", "outgoing-edge"]) {
    const multi = mode !== "single-node";
    const outgoing = mode === "outgoing-edge";
    test(`Run Flow paste renders the current saved interface, not stale clipboard handles (${mode})`, async ({
      page,
      request,
    }) => {
      const created = await authPost(
        request,
        `${API_URL}/editor/create_flow/?name=Clipboard_RunFlow_Interface&register_in_catalog=false`,
        authToken,
      );
      expect(created.ok()).toBe(true);
      const flowId: number = await created.json();
      const added = await authPost(
        request,
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=run_flow&pos_x=700&pos_y=300`,
        authToken,
      );
      expect(added.ok()).toBe(true);
      const saveInterface = async (inputSlots: string[], outputSlots: string[], clientId?: string) => {
        const saved = await request.post(`${API_URL}/update_settings/?node_type=run_flow`, {
          headers: {
            Authorization: `Bearer ${authToken}`,
            ...(clientId ? { "X-Flowfile-Client": clientId } : {}),
          },
          data: {
            flow_id: flowId,
            node_id: 1,
            flow_reference: { registration_id: 0 },
            input_slots: inputSlots,
            output_slots: outputSlots,
          },
        });
        expect(saved.ok(), await saved.text()).toBe(true);
      };
      // The interface cache is saved through the real settings endpoint. Child-flow
      // discovery/execution is outside this clipboard regression's contract.
      await saveInterface(["Orders"], ["Result"]);
      if (multi) {
        const companion = await authPost(
          request,
          `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=2&node_type=${outgoing ? "select" : "manual_input"}&pos_x=950&pos_y=500`,
          authToken,
        );
        expect(companion.ok(), await companion.text()).toBe(true);
        const connected = await request.post(`${API_URL}/editor/connect_node/?flow_id=${flowId}`, {
          headers: { Authorization: `Bearer ${authToken}` },
          data: {
            output_connection: { node_id: outgoing ? 1 : 2, connection_class: "output-0" },
            input_connection: { node_id: outgoing ? 2 : 1, connection_class: outgoing ? "input-0" : "input-1" },
          },
        });
        expect(connected.ok(), await connected.text()).toBe(true);
      }
      await openFlow(page, authToken, flowId);
      if (outgoing) {
        await page.evaluate(async () => {
          const modulePath = "/app/stores/editor-store.ts";
          const { useEditorStore } = await import(/* @vite-ignore */ modulePath);
          useEditorStore().showEdgeLabels = true;
        });
      }
      const source = page.locator('.vue-flow__node[data-id="1"]');
      await expect(source.locator('.handle-input .vue-flow__handle')).toHaveCount(1);
      await expect(source.locator('.handle-output .vue-flow__handle')).toHaveCount(1);
      await source.click();
      if (multi) {
        await page.locator('.vue-flow__node[data-id="2"]').click({ modifiers: ["Alt"] });
        await expect(page.locator('.vue-flow__node.selected')).toHaveCount(2);
      }
      await dispatchClipboardEvent(page, '.vue-flow__node[data-id="1"]', "copy", "");
      const copiedBuffer = await page.evaluate(() =>
        JSON.parse(localStorage.getItem("flowfileNodeClipboard")!),
      );
      if (multi) {
        expect(copiedBuffer.single).toBeUndefined();
        expect(copiedBuffer.multi.nodes).toHaveLength(2);
        expect(copiedBuffer.multi.nodes.map((node: { nodeIdToCopyFrom: number }) => node.nodeIdToCopyFrom).sort()).toEqual([1, 2]);
        expect(copiedBuffer.multi.edges).toHaveLength(1);
      }
      const runFlowSnapshot = multi
        ? copiedBuffer.multi.nodes.find((node: { nodeIdToCopyFrom: number }) => node.nodeIdToCopyFrom === 1)
        : copiedBuffer.single;
      expect(runFlowSnapshot.typeSnakeCase).toBe("run_flow");
      expect(runFlowSnapshot.inputHandles).toHaveLength(1);
      expect(runFlowSnapshot.outputHandles).toHaveLength(1);

      // Model a same-client saved edit without updating the canvas. An untagged
      // API write is a foreign edit and legitimately schedules a sync reload.
      const clientId = await page.evaluate(() => sessionStorage.getItem("flowfile.client_id.v1"));
      expect(clientId).toBeTruthy();
      await saveInterface(["Orders", "Customers"], [outgoing ? "Renamed" : "Result", "Rejected"], clientId!);
      // Deliberately leave both the source canvas and clipboard stale.
      await expect(source.locator('.handle-input .vue-flow__handle')).toHaveCount(1);
      await expect(source.locator('.handle-output .vue-flow__handle')).toHaveCount(1);
      expect(await page.evaluate(() => localStorage.getItem("flowfileNodeClipboard"))).toBe(
        JSON.stringify(copiedBuffer),
      );
      const reloadsBefore = await reloadCounter(page);
      const navigations: string[] = [];
      page.on("framenavigated", (frame) => {
        if (frame === page.mainFrame()) navigations.push(frame.url());
      });
      const graphResponses: import("@playwright/test").Response[] = [];
      let pasteResponded = false;
      page.on("response", (response) => {
        const url = new URL(response.url());
        if (pasteResponded && /\/flow_data\/v2\/?$/.test(url.pathname) &&
            url.searchParams.get("flow_id") === String(flowId)) graphResponses.push(response);
      });
      const copyResponse = page.waitForResponse((response) => {
        const url = new URL(response.url());
        if (response.request().method() !== "POST") return false;
        const expectedPath = multi ? /\/editor\/apply_operations\/?$/ : /\/editor\/copy_node\/?$/;
        if (!expectedPath.test(url.pathname)) return false;
        const payload = response.request().postDataJSON();
        const matches = payload.flow_id === flowId && (multi
          ? payload.operations.some((operation: { op: string; node_id_to_copy_from?: number }) =>
              operation.op === "copy_node" && operation.node_id_to_copy_from === 1)
          : url.searchParams.get("node_id_to_copy_from") === "1");
        if (matches) pasteResponded = true;
        return matches;
      });
      await dispatchClipboardEvent(
        page,
        ".vue-flow__pane",
        "paste",
        `Flowfile nodes · ${multi ? 2 : 1} · ${copiedBuffer.sentinelId}`,
      );
      const response = await copyResponse;
      expect(response.ok(), await response.text()).toBe(true);
      const payload = response.request().postDataJSON();
      const copiedId: number = multi
        ? payload.operations.find((operation: { op: string; node_id_to_copy_from?: number }) =>
            operation.op === "copy_node" && operation.node_id_to_copy_from === 1).node_promise.node_id
        : payload.node_id;
      expect(copiedId).not.toBe(1);
      if (multi) {
        expect(payload.operations.filter((operation: { op: string }) => operation.op === "copy_node")).toHaveLength(2);
        await expect(page.locator('.vue-flow__node')).toHaveCount(4);
      }
      // Read back the exact copied instance, not the source or just a node count.
      const graph = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
        headers: { Authorization: `Bearer ${authToken}` },
      });
      expect(graph.ok(), await graph.text()).toBe(true);
      const graphData = await graph.json();
      const copiedNode = graphData.node_inputs.find((node: { id: number }) => node.id === copiedId);
      expect(copiedNode).toBeDefined();
      expect(copiedNode.outputs).toEqual([
        { id: "output-0", name: outgoing ? "Renamed" : "Result" },
        { id: "output-1", name: "Rejected" },
      ]);
      expect(copiedNode.inputs).toEqual([
        { id: "input-0", name: "", kind: "parameter", hidden: true, required: false, multiple: false },
        { id: "input-1", name: "Orders", kind: "data", hidden: false, required: false, multiple: false },
        { id: "input-2", name: "Customers", kind: "data", hidden: false, required: false, multiple: false },
      ]);
      const pasted = page.locator(`.vue-flow__node[data-id="${copiedId}"]`);
      await expect(pasted.locator('.handle-input .vue-flow__handle')).toHaveCount(2);
      await expect(pasted.locator('.handle-output .vue-flow__handle')).toHaveCount(2);
      await expect(pasted.locator('.vue-flow__handle[data-handleid="input-0"]')).toHaveCount(0);
      await expect(pasted.locator('.handle-input .vue-flow__handle').nth(0)).toHaveAttribute("data-handleid", "input-1");
      await expect(pasted.locator('.handle-input .vue-flow__handle').nth(1)).toHaveAttribute("data-handleid", "input-2");
      await expect(pasted.locator('.handle-output .vue-flow__handle').nth(0)).toHaveAttribute("data-handleid", "output-0");
      await expect(pasted.locator('.handle-output .vue-flow__handle').nth(1)).toHaveAttribute("data-handleid", "output-1");
      await expect(pasted.locator('.handle-input .vue-flow__handle').nth(0)).toHaveAttribute("title", "Orders");
      await expect(pasted.locator('.handle-input .vue-flow__handle').nth(1)).toHaveAttribute("title", "Customers");
      await expect(pasted.locator('.handle-output .vue-flow__handle').nth(0)).toHaveAttribute("title", outgoing ? "Renamed" : "Result");
      await expect(pasted.locator('.handle-output .vue-flow__handle').nth(1)).toHaveAttribute("title", "Rejected");
      const reloadStacks = await page.evaluate(() => (window as unknown as { clipboardReloadStacks: string[] }).clipboardReloadStacks);
      expect(await reloadCounter(page), JSON.stringify(reloadStacks, null, 2)).toBe(reloadsBefore);
      expect(navigations).toEqual([]);
      expect(graphResponses).toHaveLength(1);
      expect(graphResponses[0].ok()).toBe(true);
      const fetched = await graphResponses[0].json();
      expect(fetched.node_inputs.find((node: { id: number }) => node.id === copiedId)).toEqual(copiedNode);
      if (multi) {
        const companionId = payload.operations.find((operation: { op: string; node_id_to_copy_from?: number }) =>
          operation.op === "copy_node" && operation.node_id_to_copy_from === 2).node_promise.node_id;
        const copiedEdges = graphData.node_edges.filter((edge: { source: string; target: string }) =>
          edge.source === String(outgoing ? copiedId : companionId) && edge.target === String(outgoing ? companionId : copiedId));
        expect(copiedEdges).toHaveLength(1);
        expect(copiedEdges[0]).toMatchObject({
          source: String(outgoing ? copiedId : companionId), target: String(outgoing ? companionId : copiedId),
          sourceHandle: "output-0", targetHandle: outgoing ? "input-0" : "input-1",
        });
        await expect(page.locator(`.vue-flow__edge[data-id="${copiedEdges[0].id}"]`)).toHaveCount(1);
        if (outgoing) {
          const label = await page.evaluate(async (id) => {
            const modulePath = "/app/stores/flow-store.ts";
            const { useFlowStore } = await import(/* @vite-ignore */ modulePath);
            return useFlowStore().vueFlowInstance?.getEdges.value.find((edge: { id: string }) => edge.id === id)?.label;
          }, copiedEdges[0].id);
          expect(label).toBe("Renamed");
        }
        await expect(page.locator(`.vue-flow__node[data-id="${companionId}"] .vue-flow__handle[data-handleid="output-0"]`)).toHaveCount(1);
        expect(payload.operations.filter((operation: { op: string }) => operation.op === "connect")).toHaveLength(1);
      }
    });
  }

  test("saving Run Flow settings preserves its summary output connection", async ({ page, request }) => {
    const created = await authPost(request,
      `${API_URL}/editor/create_flow/?name=Summary_Save_E2E&register_in_catalog=false`, authToken);
    expect(created.ok()).toBe(true);
    const flowId = await created.json();
    for (const [id, type, x] of [[1, "run_flow", 550], [2, "select", 950]]) {
      const added = await authPost(request,
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=${id}&node_type=${type}&pos_x=${x}&pos_y=400`, authToken);
      expect(added.ok()).toBe(true);
    }
    const configured = await request.post(`${API_URL}/update_settings/?node_type=run_flow`, {
      headers: { Authorization: `Bearer ${authToken}` },
      data: { flow_id: flowId, node_id: 1, flow_reference: { registration_id: 0 },
        input_slots: [], output_slots: [], parameter_specs: [], parameter_bindings: [] },
    });
    expect(configured.ok(), await configured.text()).toBe(true);
    const connected = await request.post(`${API_URL}/editor/connect_node/?flow_id=${flowId}`, {
      headers: { Authorization: `Bearer ${authToken}` },
      data: { flow_id: flowId, output_connection: { node_id: 1, connection_class: "output-0" },
        input_connection: { node_id: 2, connection_class: "input-0" } },
    });
    expect(connected.ok(), await connected.text()).toBe(true);
    await openFlow(page, authToken, flowId);
    await expect(page.locator('.vue-flow__node[data-id="1"] .handle-output .vue-flow__handle')).toHaveAttribute("title", "Run summary");
    await page.locator('.vue-flow__node[data-id="1"]').dblclick();
    await page.getByRole("tab", { name: "General Settings", exact: true }).click();
    await page.getByPlaceholder("Add a description for this node...").fill("Summary edge survives settings save");
    await page.getByPlaceholder("Add a description for this node...").blur();
    const saved = page.waitForResponse((response) => response.request().method() === "POST" &&
      (response.url().includes("/update_settings/") || response.url().includes("/apply_operations/")));
    await page.getByRole("tab", { name: /Schema/ }).click();
    expect((await saved).ok()).toBe(true);
    const graph = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
      headers: { Authorization: `Bearer ${authToken}` },
    });
    const data = await graph.json();
    expect(data.node_edges).toHaveLength(1);
    expect(data.node_edges[0]).toMatchObject({ source: "1", target: "2", sourceHandle: "output-0" });
    await expect(page.locator(`.vue-flow__edge[data-id="${data.node_edges[0].id}"]`)).toHaveCount(1);
    expect(data.node_inputs.find((node: { id: number }) => node.id === 1).description).toBe("Summary edge survives settings save");
  });

  for (const multi of [false, true]) {
    for (const undo of [false, true]) {
      test(`late paste projection respects ${undo ? "undo" : "flow switching"} (${multi ? "multi" : "single"})`, async ({ page, request }) => {
        const sourceFlowId = await createFlowWithNode(request, authToken);
        const destinationFlowId = await createFlowWithNode(request, authToken);
        if (multi) {
          const added = await authPost(request,
            `${API_URL}/editor/add_node/?flow_id=${sourceFlowId}&node_id=2&node_type=manual_input&pos_x=700&pos_y=400`, authToken);
          expect(added.ok()).toBe(true);
        }
        await openFlow(page, authToken, sourceFlowId);
        let sentinel = SENTINEL_TEXT;
        if (multi) {
          await page.evaluate(async () => {
            const path = "/app/stores/flow-store.ts";
            const { useFlowStore } = await import(/* @vite-ignore */ path);
            for (const node of useFlowStore().vueFlowInstance.getNodes.value) node.selected = true;
          });
          await dispatchClipboardEvent(page, ".vue-flow__pane", "copy", "");
          sentinel = await page.evaluate(() => {
            const buffer = JSON.parse(localStorage.getItem("flowfileNodeClipboard")!);
            return `Flowfile nodes · 2 · ${buffer.sentinelId}`;
          });
        }
        let release!: () => void;
        let held!: () => void;
        const gate = new Promise<void>((resolve) => { release = resolve; });
        const captured = new Promise<void>((resolve) => { held = resolve; });
        let holdNext = true;
        test.setTimeout(45000);
        await page.route("**/flow_data/v2?**", async (route) => {
          if (new URL(route.request().url()).searchParams.get("flow_id") !== String(sourceFlowId)) return route.continue();
          if (!holdNext) return route.continue();
          holdNext = false;
          const response = await route.fetch();
          held();
          await gate;
          await route.fulfill({ response });
        });
        await dispatchClipboardEvent(page, ".vue-flow__pane", "paste", sentinel);
        await test.step("capture post-paste graph", async () => { await captured; });
        const switched = page.waitForResponse((response) =>
          response.url().includes(`/flow_data/v2?flow_id=${undo ? sourceFlowId : destinationFlowId}`));
        if (undo) {
          await page.keyboard.press("ControlOrMeta+z");
        } else {
          await page.evaluate(async (id) => {
            const modulePath = "/app/stores/flow-store.ts";
            const { useFlowStore } = await import(/* @vite-ignore */ modulePath);
            useFlowStore().setFlowId(id);
          }, destinationFlowId);
        }
        await switched;
        await expect(page.locator(".vue-flow__node")).toHaveCount(undo && multi ? 2 : 1);
        const completed = page.waitForResponse((response) =>
          response.url().includes(`/flow_data/v2?flow_id=${sourceFlowId}`));
        release();
        await completed;
        // Allow the resumed promise and Vue's render queue to finish.
        await page.evaluate(() => new Promise<void>((resolve) =>
          requestAnimationFrame(() => requestAnimationFrame(() => resolve()))));
        await expect(page.locator(".vue-flow__node")).toHaveCount(undo && multi ? 2 : 1);
        await expect(page.locator(`.vue-flow__node[data-id="${multi ? 3 : 2}"]`)).toHaveCount(0);
        for (const [id, count] of [[sourceFlowId, (multi ? 2 : 1) * (undo ? 1 : 2)], [destinationFlowId, 1]]) {
          const graph = await request.get(`${API_URL}/flow_data/v2?flow_id=${id}`, {
            headers: { Authorization: `Bearer ${authToken}` },
          });
          expect(graph.ok()).toBe(true);
          expect((await graph.json()).node_inputs).toHaveLength(count);
        }
      });
    }
  }

  test("paste aimed at overlay-panel chrome never touches the canvas", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    // The palette ("Data actions") is a DraggableItem — same data-canvas-overlay
    // ancestry as the node-settings drawer the bug report targeted.
    await expectIgnoredPaste(page, request, authToken, flowId, await paletteChrome(page), SENTINEL_TEXT);
  });

  test("non-tabular text on the pane does NOT resurrect the stale node buffer", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    await expectIgnoredPaste(page, request, authToken, flowId, ".vue-flow__pane", "[price] * 1.21");
  });

  test("sentinel with a mismatched uuid does nothing", async ({ page, request }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    await expectIgnoredPaste(page, request, authToken, flowId, ".vue-flow__pane", "Flowfile nodes · 1 · some-other-uuid");
  });

  test("matching sentinel pastes the buffered node onto the canvas", async ({ page, request }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    const before = await nodeCount(page);

    await dispatchClipboardEvent(page, ".vue-flow__pane", "paste", SENTINEL_TEXT);
    await expect(page.locator(".vue-flow__node")).toHaveCount(before + 1, { timeout: 10000 });
  });

  test("tabular text on the pane creates a Manual Input node (not the buffered node)", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    const before = await nodeCount(page);

    const copyRequests: string[] = [];
    page.on("request", (req) => {
      if (req.url().includes("copy_node")) copyRequests.push(req.url());
    });

    await dispatchClipboardEvent(page, ".vue-flow__pane", "paste", "name\tage\nAlice\t30\nBob\t25");
    await expect(page.locator(".vue-flow__node")).toHaveCount(before + 1, { timeout: 10000 });
    expect(copyRequests).toEqual([]);
  });

  test("selection guard leaves copy unhandled when getSelection reports text (stubbed guard test)", async ({
    page,
    request,
  }) => {
    // Node placed in open canvas (the default 120,140 sits under the palette
    // overlay and is unclickable), with a description so the selectable
    // <pre class="description-text"> actually renders.
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=Clipboard_E2E_Sel&register_in_catalog=false`,
      authToken,
    );
    const flowId = await createResponse.json();
    await authPost(
      request,
      `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=formula&pos_x=700&pos_y=400`,
      authToken,
    );
    await request.post(`${API_URL}/node/description/?flow_id=${flowId}&node_id=1`, {
      headers: { Authorization: `Bearer ${authToken}`, "Content-Type": "application/json" },
      data: JSON.stringify("joins revenue per region"),
    });
    await openFlow(page, authToken, flowId);

    // Canvas node text computes user-select:none, so a real mouse selection
    // over it cannot exist (Chromium's selection.toString() is "" for such
    // content even with a programmatic range) — the guard is parity/defense
    // for any selectable canvas text. Exercise its wiring by stubbing
    // getSelection the way genuinely selectable text would behave, then run
    // an unstubbed control proving the same dispatch arms the buffer.
    await page.locator(".vue-flow__node").first().click();
    await page.waitForSelector("pre.description-text", { timeout: 10000 });
    const bufferBefore = await page.evaluate(() => localStorage.getItem("flowfileNodeClipboard"));

    const withSelection = await page.evaluate(() => {
      const pre = document.querySelector("pre.description-text")!;
      const original = window.getSelection.bind(window);
      try {
        (window as { getSelection: unknown }).getSelection = () => ({
          toString: () => pre.textContent ?? "",
        });
        const event = new ClipboardEvent("copy", {
          clipboardData: new DataTransfer(),
          bubbles: true,
          cancelable: true,
        });
        pre.dispatchEvent(event);
        return { prevented: event.defaultPrevented };
      } finally {
        (window as { getSelection: unknown }).getSelection = original;
      }
    });
    const bufferAfterGuarded = await page.evaluate(() =>
      localStorage.getItem("flowfileNodeClipboard"),
    );
    expect(withSelection.prevented).toBe(false);
    expect(bufferAfterGuarded).toBe(bufferBefore);

    const control = await page.evaluate(() => {
      const pre = document.querySelector("pre.description-text")!;
      const event = new ClipboardEvent("copy", {
        clipboardData: new DataTransfer(),
        bubbles: true,
        cancelable: true,
      });
      pre.dispatchEvent(event);
      return { prevented: event.defaultPrevented };
    });
    const bufferAfterControl = await page.evaluate(() =>
      localStorage.getItem("flowfileNodeClipboard"),
    );
    expect(control.prevented).toBe(true);
    expect(bufferAfterControl).not.toBe(bufferBefore);
  });

  test("copy aimed at overlay-panel chrome leaves the node buffer untouched", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);

    const bufferBefore = await page.evaluate(() => localStorage.getItem("flowfileNodeClipboard"));
    const dispatched = await dispatchClipboardEvent(page, await paletteChrome(page), "copy", "");
    expect(dispatched.prevented).toBe(false);
    const bufferAfter = await page.evaluate(() => localStorage.getItem("flowfileNodeClipboard"));

    expect(bufferAfter).toBe(bufferBefore);
  });
});
