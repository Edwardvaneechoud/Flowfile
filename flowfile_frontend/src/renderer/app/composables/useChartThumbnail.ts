import { createApp, h, ref, type Ref } from "vue";
import type VueGraphicWalker from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import VueGraphicRenderer from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicRenderer.vue";
import type { IRow } from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/interfaces";
import type { IChart } from "@kanaries/graphic-walker/interfaces";

// Mirrors flowfile_core/flowfile_core/catalog/service.py:_THUMBNAIL_MAX_BYTES.
export const THUMBNAIL_MAX_BYTES = 500_000;

type GwRef = Ref<InstanceType<typeof VueGraphicWalker> | null>;

async function downscale(dataUrl: string, maxDim = 800): Promise<string> {
  const img = new Image();
  await new Promise<void>((res, rej) => {
    img.onload = () => res();
    img.onerror = rej;
    img.src = dataUrl;
  });
  const scale = Math.min(maxDim / img.width, maxDim / img.height, 1);
  const w = Math.round(img.width * scale);
  const h = Math.round(img.height * scale);
  const canvas = document.createElement("canvas");
  canvas.width = w;
  canvas.height = h;
  canvas.getContext("2d")!.drawImage(img, 0, 0, w, h);
  return canvas.toDataURL("image/png");
}

type Computation = (payload: any) => Promise<IRow[]>;

const CAPTURE_SIZE = { width: 800, height: 500 };
const CAPTURE_TIMEOUT_MS = 8000;
const CAPTURE_POLL_MS = 200;
// How long the chart's queries must have been quiet before it counts as drawn.
const CAPTURE_SETTLE_MS = 400;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Draw one chart off screen in the light theme and return its picture.
 *
 * The chart builder shows a chart in the app's current theme, so a picture taken from it is dark
 * whenever the app is, and the library shows every thumbnail on a light card. The copy drawn here
 * is always light. Null when the chart did not draw in time.
 */
async function captureLight(chart: IChart, computation: Computation): Promise<string | null> {
  const host = document.createElement("div");
  host.style.cssText =
    `position:fixed;left:-10000px;top:0;width:${CAPTURE_SIZE.width}px;` +
    `height:${CAPTURE_SIZE.height}px;background:#ffffff;pointer-events:none;`;
  document.body.appendChild(host);

  let started = 0;
  let pending = 0;
  let settledAt = 0;
  const tracked: Computation = async (payload) => {
    started += 1;
    pending += 1;
    try {
      return await computation(payload);
    } finally {
      pending -= 1;
      settledAt = Date.now();
    }
  };

  const renderer = ref<{ exportImage: () => Promise<string | null> } | null>(null);
  const app = createApp({
    render: () =>
      h(VueGraphicRenderer, { ref: renderer, chart, computation: tracked, appearance: "light" }),
  });
  app.mount(host);
  try {
    const deadline = Date.now() + CAPTURE_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await sleep(CAPTURE_POLL_MS);
      if (!started || pending || Date.now() - settledAt < CAPTURE_SETTLE_MS) continue;
      const image = await renderer.value?.exportImage();
      if (image) return image;
    }
    return null;
  } finally {
    app.unmount();
    host.remove();
  }
}

/**
 * A thumbnail of the chart open in the builder. With the builder's `computation` it is drawn
 * again in the light theme; without one, or when that fails, it is the builder's own picture.
 */
export async function captureThumbnail(
  gwRef: GwRef,
  computation?: Computation,
): Promise<string | null> {
  if (!gwRef.value || typeof gwRef.value.exportImage !== "function") return null;
  try {
    let raw: string | null = null;
    if (computation) {
      const chart = (await gwRef.value.exportCode())?.[0];
      if (chart) raw = await captureLight(chart, computation).catch(() => null);
    }
    raw ??= await gwRef.value.exportImage();
    if (!raw) return null;
    const dataUrl = await downscale(raw, 800);
    if (dataUrl.length > THUMBNAIL_MAX_BYTES) {
      console.warn(`[viz] thumbnail too large after downscale (${dataUrl.length} bytes)`);
      return null;
    }
    return dataUrl;
  } catch (err) {
    console.warn("[viz] thumbnail capture failed:", err);
    return null;
  }
}
