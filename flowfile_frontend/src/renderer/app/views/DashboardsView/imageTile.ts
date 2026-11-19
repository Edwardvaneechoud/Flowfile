import type { DashboardImageFit, DashboardTile } from "../../types";

/** Core's caps (`catalog_schema.py`): image bytes per image and per dashboard. */
export const IMAGE_MAX_BYTES = 300 * 1024;
export const IMAGES_TOTAL_MAX_BYTES = 2 * 1024 * 1024;
export const IMAGE_ALT_MAX_LENGTH = 300;
/** A raster longer than this on its long side is scaled down before it is stored. */
export const IMAGE_MAX_SIDE_PX = 1600;
/** A new image tile: a sixth of the 48-column grid, three rows. */
export const IMAGE_DEFAULT_SIZE = { w: 8, h: 3 };

export const IMAGE_MIME_TYPES = [
  "image/png",
  "image/jpeg",
  "image/webp",
  "image/gif",
  "image/svg+xml",
] as const;
export type ImageMime = (typeof IMAGE_MIME_TYPES)[number];

/** The file input's `accept`. */
export const IMAGE_ACCEPT = IMAGE_MIME_TYPES.join(",");

export const NOT_AN_IMAGE_MESSAGE =
  "Only an image file (PNG, JPEG, WebP, GIF or SVG) can go on a dashboard.";

const EXTENSION_MIME: Record<string, ImageMime> = {
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  webp: "image/webp",
  gif: "image/gif",
  svg: "image/svg+xml",
};

const isImageMime = (type: string): type is ImageMime =>
  (IMAGE_MIME_TYPES as readonly string[]).includes(type);

/** The image type of a file, by its type or, when the system gives none, its extension; else null. */
export const imageMimeOf = (file: { type: string; name: string }): ImageMime | null => {
  const type = file.type.toLowerCase();
  if (isImageMime(type)) return type;
  if (type && type !== "application/octet-stream") return null;
  const ext = /\.([a-z0-9]+)$/i.exec(file.name)?.[1]?.toLowerCase();
  return (ext && EXTENSION_MIME[ext]) || null;
};

/** Whether a drag carries files, and whether one of them looks like an image (types only, as a dragover sees them). */
export const draggedFiles = (
  types: readonly string[],
  items: readonly { kind: string; type: string }[],
): "none" | "image" | "other" => {
  if (!types.includes("Files")) return "none";
  return items.some((it) => it.kind === "file" && isImageMime(it.type.toLowerCase()))
    ? "image"
    : "other";
};

export interface DataUrlInfo {
  mime: string;
  /** The bytes the base64 payload decodes to. */
  bytes: number;
}

/** A base64 data URL's media type and decoded size, or null when it is not one. */
export const dataUrlInfo = (url: string): DataUrlInfo | null => {
  const comma = url.indexOf(",");
  if (comma < 0) return null;
  const header = /^data:([\w.+-]+\/[\w.+-]+);base64$/i.exec(url.slice(0, comma));
  if (!header) return null;
  const payloadLength = url.length - comma - 1;
  const padding = url.endsWith("==") ? 2 : url.endsWith("=") ? 1 : 0;
  return {
    mime: header[1].toLowerCase(),
    bytes: Math.max(0, Math.floor((payloadLength * 3) / 4) - padding),
  };
};

/** A data URL with its media type set to `mime` (a file without a type reads as octet-stream). */
export const dataUrlWithMime = (url: string, mime: string): string => {
  const comma = url.indexOf(",");
  return comma < 0 ? url : `data:${mime};base64${url.slice(comma)}`;
};

/** A byte count as core words it, rounded up so a size just over a cap never reads as the cap. */
export const formatImageBytes = (bytes: number): string => {
  const mb = 1024 * 1024;
  if (bytes > mb) return `${Math.ceil((bytes * 10) / mb) / 10} MB`;
  if (bytes === mb) return "1 MB";
  return `${Math.max(1, Math.ceil(bytes / 1024))} KB`;
};

/** The long sides (px) a raster is re-encoded at, largest first, until it fits. */
export const IMAGE_LADDER_SIDES_PX = [IMAGE_MAX_SIDE_PX, 1280, 1024, 800, 640] as const;
/** The qualities a JPEG or WebP is tried at on each side; PNG has none (it is lossless). */
export const IMAGE_LADDER_QUALITIES = [0.9, 0.8, 0.7] as const;

/** One re-encoding: the raster drawn at this size, in its own format. */
export interface EncodeStep {
  width: number;
  height: number;
  type: ImageMime;
  quality?: number;
}

const keptAsIs = (mime: ImageMime) => mime === "image/gif" || mime === "image/svg+xml";

/**
 * Whether an image is stored exactly as read: a GIF (it may be animated) or an SVG always, a
 * raster when it is within `IMAGE_MAX_SIDE_PX` on its long side and within `limit` bytes.
 */
export const storedAsIs = (
  mime: ImageMime,
  width: number,
  height: number,
  bytes: number,
  limit: number,
): boolean => keptAsIs(mime) || (Math.max(width, height) <= IMAGE_MAX_SIDE_PX && bytes <= limit);

/**
 * The re-encodings to try, in order, for a raster not stored as is; the first within the limit
 * wins. Each long side of `IMAGE_LADDER_SIDES_PX` below the image's own (the first capped at
 * 1600 px, so a smaller image starts at its own size), and on each side JPEG and WebP step
 * through `IMAGE_LADDER_QUALITIES`. The format never changes, so PNG keeps transparency.
 */
export const encodeLadder = (mime: ImageMime, width: number, height: number): EncodeStep[] => {
  if (keptAsIs(mime)) return [];
  const long = Math.max(width, height, 1);
  const first = Math.min(long, IMAGE_MAX_SIDE_PX);
  const sides = [first, ...IMAGE_LADDER_SIDES_PX.filter((side) => side < first)];
  const qualities = mime === "image/png" ? [undefined] : IMAGE_LADDER_QUALITIES;
  return sides.flatMap((side) =>
    qualities.map((quality) => ({
      width: Math.max(1, Math.round((width * side) / long)),
      height: Math.max(1, Math.round((height * side) / long)),
      type: mime,
      ...(quality === undefined ? {} : { quality }),
    })),
  );
};

/**
 * Why an image of `bytes` is refused, saying what to do: a GIF or SVG kept as it is, or a
 * raster still over the cap at `last`, the smallest step of its ladder.
 */
export const imageTooLargeError = (
  mime: ImageMime,
  bytes: number,
  last: EncodeStep | null,
): string => {
  const cap = formatImageBytes(IMAGE_MAX_BYTES);
  if (keptAsIs(mime) || !last) {
    const kind = mime === "image/gif" ? "GIF" : mime === "image/svg+xml" ? "SVG" : "image";
    return (
      `This ${kind} is ${formatImageBytes(bytes)}; a GIF or SVG is kept as it is and may be at ` +
      `most ${cap}. Save it as a PNG or JPEG, or make it smaller, and try again.`
    );
  }
  const advice =
    mime === "image/png"
      ? "Save it as a JPEG, or crop it, and try again."
      : "Crop it and try again.";
  return (
    `This image is still ${formatImageBytes(bytes)} at ${Math.max(last.width, last.height)} px; ` +
    `an image may be at most ${cap}. ${advice}`
  );
};

/** The image bytes the tiles hold, leaving out the tile `exceptTileId` (the one being replaced). */
export const imageBytesOf = (
  tiles: readonly DashboardTile[],
  exceptTileId: string | null = null,
): number =>
  tiles.reduce((sum, t) => {
    if (t.id === exceptTileId || !t.image?.data_url) return sum;
    return sum + (dataUrlInfo(t.image.data_url)?.bytes ?? t.image.data_url.length);
  }, 0);

/** Why `bytes` more of images cannot join `otherBytes` already on the dashboard, or null. */
export const imageBudgetError = (otherBytes: number, bytes: number): string | null =>
  otherBytes + bytes > IMAGES_TOTAL_MAX_BYTES
    ? `The images on this dashboard would take ${formatImageBytes(otherBytes + bytes)}; ` +
      `a dashboard holds at most ${formatImageBytes(IMAGES_TOTAL_MAX_BYTES)} of images.`
    : null;

/** Whether a tile is an image tile with a picture (an empty one is blank to viewers). */
export const imageTileShows = (tile: DashboardTile): boolean =>
  tile.type === "image" && !!tile.image?.data_url;

/** The tile showing `dataUrl`, keeping its alt text and fit. */
export const withImage = (tile: DashboardTile, dataUrl: string): DashboardTile => ({
  ...tile,
  image: {
    data_url: dataUrl,
    alt: tile.image?.alt ?? null,
    fit: tile.image?.fit ?? "contain",
  },
});

/** The tile with another fit, or null when it has no image or the fit is unchanged. */
export const withImageFit = (tile: DashboardTile, fit: DashboardImageFit): DashboardTile | null => {
  if (!tile.image || (tile.image.fit ?? "contain") === fit) return null;
  return { ...tile, image: { ...tile.image, fit } };
};

/** The tile with the draft as its alt text (blank removes it), or null when nothing changes. */
export const withImageAlt = (tile: DashboardTile, draft: string): DashboardTile | null => {
  if (!tile.image) return null;
  const alt = draft.trim().slice(0, IMAGE_ALT_MAX_LENGTH) || null;
  if (alt === (tile.image.alt ?? null)) return null;
  return { ...tile, image: { ...tile.image, alt } };
};
