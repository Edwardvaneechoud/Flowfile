import { test, expect } from "@playwright/test";
import { closeFlow, createFlow, flowEdges, getAuthToken, nodeIdsByType } from "./helpers/api";
import { minimizePalette, openFlow } from "./helpers/canvas";
import { postEditor } from "./helpers/flows";

for (const key of ["Delete", "Backspace"]) {
  test(`${key} removes a selected connection without deleting its nodes`, async ({
    page,
    request,
  }) => {
    const token = await getAuthToken(request);
    const name = `delete_key_${key}_${Date.now()}`;
    const flowId = await createFlow(request, token, name);
    try {
      for (const [id, type, x] of [
        [1, "manual_input", 250],
        [2, "select", 600],
      ] as const) {
        await postEditor(request, token, "/editor/add_node/", {
          flow_id: flowId,
          node_id: id,
          node_type: type,
          pos_x: x,
          pos_y: 200,
        });
      }
      await openFlow(page, token, flowId, name);
      await minimizePalette(page);
      const nodes = page.locator(".vue-flow__node");
      await expect(nodes).toHaveCount(2);
      const source = page.locator('.vue-flow__node[data-id="1"] .vue-flow__handle.source').first();
      const target = page.locator('.vue-flow__node[data-id="2"] .vue-flow__handle.target').first();
      const a = await source.boundingBox();
      const b = await target.boundingBox();
      expect(a).not.toBeNull();
      expect(b).not.toBeNull();
      await page.mouse.move(a!.x + a!.width / 2, a!.y + a!.height / 2);
      await page.mouse.down();
      await page.mouse.move(b!.x + b!.width / 2, b!.y + b!.height / 2, { steps: 20 });
      await page.mouse.up();
      await expect.poll(() => flowEdges(request, token, flowId)).toEqual(["1->2"]);
      const edge = page.locator(".vue-flow__edge");
      // Avoid the midpoint, where hovering reveals the delete button.
      const point = await edge
        .locator("path")
        .first()
        .evaluate((element) => {
          const path = element as SVGPathElement;
          const p = path.getPointAtLength(path.getTotalLength() / 4);
          const screen = new DOMPoint(p.x, p.y).matrixTransform(path.getScreenCTM()!);
          return { x: screen.x, y: screen.y };
        });
      await page.mouse.click(point.x, point.y);
      await expect(edge).toHaveClass(/selected/);
      await page.keyboard.press(key);
      await expect(edge).toHaveCount(0);
      await expect.poll(() => flowEdges(request, token, flowId)).toEqual([]);
      expect(await nodeIdsByType(request, token, flowId)).toEqual({
        manual_input: ["1"],
        select: ["2"],
      });
      await page.reload();
      await expect(nodes).toHaveCount(2);
      await expect(edge).toHaveCount(0);
    } finally {
      await closeFlow(request, token, flowId);
    }
  });
}
