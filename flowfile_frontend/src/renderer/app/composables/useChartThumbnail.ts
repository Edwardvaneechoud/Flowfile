import type { Ref } from "vue";
import type VueGraphicWalker from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";

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

export async function captureThumbnail(gwRef: GwRef): Promise<string | null> {
  if (!gwRef.value || typeof gwRef.value.exportImage !== "function") return null;
  try {
    const raw = await gwRef.value.exportImage();
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
