import type { Ref } from "vue";
import type VueGraphicWalker from "../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";

// Mirrors flowfile_core/flowfile_core/catalog/service.py:_THUMBNAIL_MAX_BYTES.
export const THUMBNAIL_MAX_BYTES = 200_000;

type GwRef = Ref<InstanceType<typeof VueGraphicWalker> | null>;

export async function captureThumbnail(gwRef: GwRef): Promise<string | null> {
  if (!gwRef.value || typeof gwRef.value.exportImage !== "function") return null;
  try {
    const dataUrl = await gwRef.value.exportImage();
    if (!dataUrl) return null;
    if (dataUrl.length > THUMBNAIL_MAX_BYTES) {
      console.warn(`[viz] thumbnail too large (${dataUrl.length} bytes), skipping`);
      return null;
    }
    return dataUrl;
  } catch (err) {
    console.warn("[viz] thumbnail capture failed:", err);
    return null;
  }
}
