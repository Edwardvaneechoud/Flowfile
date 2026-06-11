/**
 * Capture the current GraphicWalker chart as a small base64 PNG for visual
 * library cards. Ported from flowfile_frontend's useChartThumbnail, but with a
 * much tighter cap: thumbnails live in localStorage alongside the specs, so a
 * 240px / <60KB ceiling keeps many visuals well inside the ~5MB quota.
 */

import type { Ref } from 'vue'
import type VueGraphicWalker from '../components/nodes/exploreData/VueGraphicWalker.vue'

export const THUMBNAIL_MAX_BYTES = 60_000
const THUMBNAIL_MAX_DIM = 240

type GwRef = Ref<InstanceType<typeof VueGraphicWalker> | null>

async function downscale(dataUrl: string, maxDim = THUMBNAIL_MAX_DIM): Promise<string> {
  const img = new Image()
  await new Promise<void>((res, rej) => {
    img.onload = () => res()
    img.onerror = rej
    img.src = dataUrl
  })
  const scale = Math.min(maxDim / img.width, maxDim / img.height, 1)
  const w = Math.round(img.width * scale)
  const h = Math.round(img.height * scale)
  const canvas = document.createElement('canvas')
  canvas.width = w
  canvas.height = h
  canvas.getContext('2d')!.drawImage(img, 0, 0, w, h)
  return canvas.toDataURL('image/png')
}

export async function captureThumbnail(gwRef: GwRef): Promise<string | null> {
  if (!gwRef.value || typeof gwRef.value.exportImage !== 'function') return null
  try {
    const raw = await gwRef.value.exportImage()
    if (!raw) return null
    const dataUrl = await downscale(raw, THUMBNAIL_MAX_DIM)
    if (dataUrl.length > THUMBNAIL_MAX_BYTES) {
      console.warn(`[viz] thumbnail too large after downscale (${dataUrl.length} bytes); skipping`)
      return null
    }
    return dataUrl
  } catch (err) {
    console.warn('[viz] thumbnail capture failed:', err)
    return null
  }
}
