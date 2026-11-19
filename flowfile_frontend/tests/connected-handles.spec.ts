/// <reference types="node" />
import { test, expect } from "@playwright/test";
import { readFileSync } from "node:fs";

// Run against npm run dev:web. This mounts real VueFlow and NodeWrapper;
// only description fetching is replaced, since no backend flow is needed.
const BASE_URL = process.env.TEST_URL || "http://localhost:8080";

for (const initiallyUnavailable of [false, true]) {
  test(`saved edges remain visible with unavailable ports (initial=${initiallyUnavailable})`, async ({
    page,
  }) => {
    await page.route("**/connected-handles-fixture", (route) =>
      route.fulfill({
        contentType: "text/html",
        body: '<html><body><div id="fixture" style="width:900px;height:600px"></div></body></html>',
      }),
    );
    await page.goto(`${BASE_URL}/connected-handles-fixture`);
    await page.addStyleTag({
      content: ["style.css", "theme-default.css"]
        .map((file) => readFileSync(`node_modules/@vue-flow/core/dist/${file}`, "utf8"))
        .join("\n"),
    });

    await page.route("**/node_list", (route) =>
      route.fulfill({ contentType: "application/json", body: "[]" }),
    );
    await page.evaluate(async (initiallyUnavailable) => {
      const load = (path: string) => import(/* @vite-ignore */ path);
      const { createApp, h, ref, nextTick } = await load("/@id/vue");
      const { createPinia } = await load("/@id/pinia");
      const { VueFlow, Position, useVueFlow } = await load("/@id/@vue-flow/core");
      const { connectionPortError } = await load("/app/utils/connectionPorts.ts");
      const { default: useDragAndDrop } = await load("/app/composables/useDragAndDrop.ts");

      const { default: NodeWrapper } = await load("/app/components/nodes/NodeWrapper.vue");
      const { useNodeStore } = await load("/app/stores/column-store.ts");
      const pinia = createPinia();
      useNodeStore(pinia).getNodeDescription = async () => "";
      const nodes = ref([
        {
          id: "1",
          type: "custom-node",
          position: { x: 50, y: 100 },
          data: {
            id: 1,
            label: "Source",
            inputs: [],
            outputs: initiallyUnavailable ? [] : [{ id: "output-1", position: Position.Right }],
          },
        },
        {
          id: "2",
          type: "custom-node",
          position: { x: 450, y: 250 },
          data: {
            id: 2,
            label: "Target",
            inputs: initiallyUnavailable ? [] : [{ id: "input-0", position: Position.Left }],
            outputs: [],
          },
        },
      ]);
      const edges = ref([
        {
          id: "saved",
          source: "1",
          target: "2",
          sourceHandle: "output-1",
          targetHandle: "input-0",
        },
      ]);
      let instance: ReturnType<typeof useVueFlow>;
      let projection: ReturnType<typeof useDragAndDrop>;
      const app = createApp({
        setup() {
          instance = useVueFlow();
          projection = useDragAndDrop();
          return () =>
            h(
              VueFlow,
              {
                nodes: nodes.value,
                edges: edges.value,
                // Match Canvas's boundary: full saved edges render, while new
                // gestures still validate against the declared instance ports.
                isValidConnection: (connection: { source: string; target: string }) =>
                  "id" in connection ||
                  connectionPortError(
                    connection,
                    nodes.value.find((node: { id: string }) => node.id === connection.source)?.data,
                    nodes.value.find((node: { id: string }) => node.id === connection.target)?.data,
                    edges.value,
                  ) === null,
              },
              {
                "node-custom-node": (props: { data: unknown }) =>
                  h(NodeWrapper, { data: props.data }),
              },
            );
        },
      });
      app.use(pinia).mount("#fixture");
      await nextTick();
      const savedEdges = [...edges.value];
      instance!.setEdges([]);
      if (initiallyUnavailable) {
        // Real saved-flow projection: missing-template fallback plus addEdges.
        await projection!.importFlow({
          node_inputs: nodes.value.map(
            (node: { id: string; position: { x: number; y: number } }) => ({
              id: Number(node.id),
              item: "uninstalled_custom_node",
              name: "Unavailable custom node",
              pos_x: node.position.x,
              pos_y: node.position.y,
              input: 0,
              output: 0,
              inputs: [],
              outputs: [],
            }),
          ),
          node_edges: savedEdges,
        });
        if (!instance!.findNode("1")?.data.nodeTemplate.custom_node) {
          throw new Error("Missing template did not use the custom-node placeholder");
        }
      } else {
        instance!.addEdges(savedEdges);
      }
      Object.assign(window, {
        retirePorts: () => {
          instance!.findNode("1")!.data.outputs = [];
          instance!.findNode("2")!.data.inputs = [];
        },
        removeSavedEdge: () => {
          instance!.setEdges([]);
          edges.value = [];
        },
      });
    }, initiallyUnavailable);
    const edge = page.locator('.vue-flow__edge[data-id="saved"] .vue-flow__edge-path');
    await expect(edge).toBeVisible();
    await page.evaluate(() => (window as unknown as { retirePorts(): void }).retirePorts());
    const source = page.locator('[data-nodeid="1"][data-handleid="output-1"]');
    const target = page.locator('[data-nodeid="2"][data-handleid="input-0"]');
    await expect(source).toHaveAttribute("title", "Saved connection (port unavailable)");
    await expect(target).toHaveAttribute("title", "Saved connection (port unavailable)");
    await expect(source).not.toHaveClass(/connectablestart/);
    await expect(target).not.toHaveClass(/connectableend/);
    await expect(edge).toBeVisible();
    await expect(edge).toHaveAttribute("d", /^M/);
    await page.evaluate(() => (window as unknown as { removeSavedEdge(): void }).removeSavedEdge());
    await expect(edge).toHaveCount(0);
    await expect(source).toHaveCount(0);
    await expect(target).toHaveCount(0);
  });
}
