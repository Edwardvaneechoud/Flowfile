import {
  IMAGE_MAX_BYTES,
  IMAGES_TOTAL_MAX_BYTES,
  dataUrlInfo,
  dataUrlWithMime,
  encodeLadder,
  imageBudgetError,
  imageMimeOf,
  imageTooLargeError,
  storedAsIs,
  type EncodeStep,
} from "./imageTile";

/** Why a file cannot become a tile's image; its message is for the user. */
export class ImageFileError extends Error {}

const readAsDataUrl = (file: File): Promise<string> =>
  new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.onerror = () => reject(new ImageFileError(`${file.name} could not be read.`));
    reader.readAsDataURL(file);
  });

const loadImage = (src: string, name: string): Promise<HTMLImageElement> =>
  new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = () => reject(new ImageFileError(`${name} could not be read as an image.`));
    img.src = src;
  });

const encodeOnCanvas = (img: HTMLImageElement, step: EncodeStep): string => {
  const canvas = document.createElement("canvas");
  canvas.width = step.width;
  canvas.height = step.height;
  const ctx = canvas.getContext("2d");
  if (!ctx) throw new ImageFileError("This browser cannot scale the image down.");
  ctx.imageSmoothingQuality = "high";
  ctx.drawImage(img, 0, 0, step.width, step.height);
  return canvas.toDataURL(step.type, step.quality);
};

const sizeOf = (url: string): number => dataUrlInfo(url)?.bytes ?? url.length;

/**
 * The data URL a tile stores for `file`: read and decoded once to check it is an image, kept as
 * it is when `storedAsIs` says so, else re-encoded down `encodeLadder` until a step fits both
 * the per-image cap and what the dashboard's `otherBytes` of images leave. Throws
 * `ImageFileError` with the reason and what to do.
 */
export async function readImageFile(file: File, otherBytes: number): Promise<string> {
  const mime = imageMimeOf(file);
  if (!mime) {
    throw new ImageFileError(
      `${file.name} is not an image a dashboard can show (PNG, JPEG, WebP, GIF or SVG).`,
    );
  }
  const limit = Math.min(IMAGE_MAX_BYTES, Math.max(0, IMAGES_TOTAL_MAX_BYTES - otherBytes));
  const original = dataUrlWithMime(await readAsDataUrl(file), mime);
  const img = await loadImage(original, file.name);
  let url = original;
  let last: EncodeStep | null = null;
  if (!storedAsIs(mime, img.naturalWidth, img.naturalHeight, sizeOf(original), limit)) {
    for (const step of encodeLadder(mime, img.naturalWidth, img.naturalHeight)) {
      url = encodeOnCanvas(img, step);
      last = step;
      if (sizeOf(url) <= limit) return url;
    }
  }
  const bytes = sizeOf(url);
  if (bytes > IMAGE_MAX_BYTES) throw new ImageFileError(imageTooLargeError(mime, bytes, last));
  const budget = imageBudgetError(otherBytes, bytes);
  if (budget) throw new ImageFileError(budget);
  return url;
}
