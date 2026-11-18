import { describe, expect, it } from "vitest";
import type { DashboardTile } from "../../types";
import {
  IMAGE_MAX_BYTES,
  IMAGES_TOTAL_MAX_BYTES,
  dataUrlInfo,
  dataUrlWithMime,
  draggedFiles,
  encodeLadder,
  formatImageBytes,
  imageBudgetError,
  imageBytesOf,
  imageMimeOf,
  imageTileShows,
  imageTooLargeError,
  storedAsIs,
  withImage,
  withImageAlt,
  withImageFit,
  type EncodeStep,
} from "./imageTile";

const urlOf = (bytes: number, mime = "image/png") => {
  const payload = "A".repeat(Math.ceil(bytes / 3) * 4);
  const padding = (3 - (bytes % 3)) % 3;
  return `data:${mime};base64,${payload.slice(0, payload.length - padding)}${"=".repeat(padding)}`;
};

const tile = (over: Partial<DashboardTile> = {}): DashboardTile => ({
  id: "img",
  type: "image",
  viz_id: null,
  chart_index: 0,
  x: 0,
  y: 0,
  w: 8,
  h: 3,
  ...over,
});

describe("imageMimeOf", () => {
  it("takes the five image types by their type", () => {
    expect(imageMimeOf({ type: "image/png", name: "a.png" })).toBe("image/png");
    expect(imageMimeOf({ type: "image/jpeg", name: "a" })).toBe("image/jpeg");
    expect(imageMimeOf({ type: "image/webp", name: "a" })).toBe("image/webp");
    expect(imageMimeOf({ type: "image/gif", name: "a" })).toBe("image/gif");
    expect(imageMimeOf({ type: "image/svg+xml", name: "a" })).toBe("image/svg+xml");
  });

  it("falls back on the extension only when the system names no type", () => {
    expect(imageMimeOf({ type: "", name: "Logo.SVG" })).toBe("image/svg+xml");
    expect(imageMimeOf({ type: "application/octet-stream", name: "photo.jpg" })).toBe("image/jpeg");
    expect(imageMimeOf({ type: "", name: "notes.txt" })).toBeNull();
    expect(imageMimeOf({ type: "text/plain", name: "fake.png" })).toBeNull();
  });

  it("refuses other images", () => {
    expect(imageMimeOf({ type: "image/tiff", name: "scan.tiff" })).toBeNull();
    expect(imageMimeOf({ type: "image/bmp", name: "a.bmp" })).toBeNull();
  });
});

describe("draggedFiles", () => {
  it("tells a file drag from ours and an image from other files", () => {
    expect(draggedFiles(["application/flowfile-kpi"], [])).toBe("none");
    expect(draggedFiles(["Files"], [{ kind: "file", type: "image/png" }])).toBe("image");
    expect(draggedFiles(["Files"], [{ kind: "file", type: "text/csv" }])).toBe("other");
    expect(draggedFiles(["Files"], [])).toBe("other");
  });
});

describe("dataUrlInfo", () => {
  it("reads the type and the decoded size from the length", () => {
    expect(dataUrlInfo("data:image/png;base64,AAAA")).toEqual({ mime: "image/png", bytes: 3 });
    expect(dataUrlInfo("data:image/png;base64,AAA=")).toEqual({ mime: "image/png", bytes: 2 });
    expect(dataUrlInfo("data:image/svg+xml;base64,AA==")).toEqual({
      mime: "image/svg+xml",
      bytes: 1,
    });
    expect(dataUrlInfo(urlOf(IMAGE_MAX_BYTES))?.bytes).toBe(IMAGE_MAX_BYTES);
  });

  it("is null for anything but a base64 data URL", () => {
    expect(dataUrlInfo("https://example.com/a.png")).toBeNull();
    expect(dataUrlInfo("data:image/svg+xml,<svg/>")).toBeNull();
    expect(dataUrlInfo("data:image/png;base64")).toBeNull();
  });
});

describe("dataUrlWithMime", () => {
  it("names the type a file without one read as", () => {
    expect(dataUrlWithMime("data:application/octet-stream;base64,AAAA", "image/png")).toBe(
      "data:image/png;base64,AAAA",
    );
  });
});

describe("formatImageBytes", () => {
  it("rounds up, so a size just over a cap never reads as the cap", () => {
    expect(formatImageBytes(300 * 1024)).toBe("300 KB");
    expect(formatImageBytes(300 * 1024 + 1)).toBe("301 KB");
    expect(formatImageBytes(10)).toBe("1 KB");
    expect(formatImageBytes(1024 * 1024)).toBe("1 MB");
    expect(formatImageBytes(2 * 1024 * 1024)).toBe("2 MB");
    expect(formatImageBytes(2 * 1024 * 1024 + 1)).toBe("2.1 MB");
  });
});

describe("storedAsIs", () => {
  it("keeps a GIF or SVG whatever its size", () => {
    expect(storedAsIs("image/gif", 4000, 3000, 5_000_000, IMAGE_MAX_BYTES)).toBe(true);
    expect(storedAsIs("image/svg+xml", 0, 0, 5_000_000, IMAGE_MAX_BYTES)).toBe(true);
  });

  it("never re-encodes a raster that already fits, and re-encodes one too long or too large", () => {
    expect(storedAsIs("image/png", 1600, 900, IMAGE_MAX_BYTES, IMAGE_MAX_BYTES)).toBe(true);
    expect(storedAsIs("image/jpeg", 1601, 900, 1000, IMAGE_MAX_BYTES)).toBe(false);
    expect(storedAsIs("image/jpeg", 1200, 900, IMAGE_MAX_BYTES + 1, IMAGE_MAX_BYTES)).toBe(false);
    expect(storedAsIs("image/png", 400, 300, 200_000, 100_000)).toBe(false);
  });
});

describe("encodeLadder", () => {
  const sides = (steps: EncodeStep[]) => steps.map((s) => Math.max(s.width, s.height));

  it("is empty for a GIF or SVG, which are kept as they are", () => {
    expect(encodeLadder("image/gif", 4000, 3000)).toEqual([]);
    expect(encodeLadder("image/svg+xml", 4000, 3000)).toEqual([]);
  });

  it("steps a large PNG down 1600, 1280, 1024, 800 and 640 px, keeping PNG and its shape", () => {
    const steps = encodeLadder("image/png", 3200, 1000);
    expect(sides(steps)).toEqual([1600, 1280, 1024, 800, 640]);
    expect(steps[0]).toEqual({ width: 1600, height: 500, type: "image/png" });
    expect(steps.every((s) => s.type === "image/png" && s.quality === undefined)).toBe(true);
    expect(steps[4]).toEqual({ width: 640, height: 200, type: "image/png" });
  });

  it("tries a JPEG or WebP at quality 0.9, 0.8 and 0.7 on each side", () => {
    const steps = encodeLadder("image/jpeg", 1000, 4000);
    expect(steps).toHaveLength(15);
    expect(steps.slice(0, 4)).toEqual([
      { width: 400, height: 1600, type: "image/jpeg", quality: 0.9 },
      { width: 400, height: 1600, type: "image/jpeg", quality: 0.8 },
      { width: 400, height: 1600, type: "image/jpeg", quality: 0.7 },
      { width: 320, height: 1280, type: "image/jpeg", quality: 0.9 },
    ]);
    expect(steps.at(-1)).toEqual({ width: 160, height: 640, type: "image/jpeg", quality: 0.7 });
    expect(encodeLadder("image/webp", 5000, 5000)[0]).toMatchObject({ type: "image/webp" });
  });

  it("starts a smaller image at its own size and never scales it up", () => {
    expect(sides(encodeLadder("image/png", 1100, 700))).toEqual([1100, 1024, 800, 640]);
    expect(sides(encodeLadder("image/png", 500, 300))).toEqual([500]);
  });

  it("never makes a side zero", () => {
    expect(encodeLadder("image/png", 20000, 2).at(-1)).toMatchObject({ width: 640, height: 1 });
  });
});

describe("imageTooLargeError", () => {
  it("says a GIF or SVG is kept as it is and what to do", () => {
    expect(imageTooLargeError("image/gif", 1.2 * 1024 * 1024, null)).toBe(
      "This GIF is 1.2 MB; a GIF or SVG is kept as it is and may be at most 300 KB. " +
        "Save it as a PNG or JPEG, or make it smaller, and try again.",
    );
    expect(imageTooLargeError("image/svg+xml", 412 * 1024, null)).toMatch(/^This SVG is 412 KB;/);
  });

  it("names the size at the last step and suggests JPEG for a PNG", () => {
    const last = { width: 640, height: 480, type: "image/png" as const };
    expect(imageTooLargeError("image/png", 412 * 1024, last)).toBe(
      "This image is still 412 KB at 640 px; an image may be at most 300 KB. " +
        "Save it as a JPEG, or crop it, and try again.",
    );
    const jpeg = { width: 480, height: 640, type: "image/jpeg" as const, quality: 0.7 };
    expect(imageTooLargeError("image/jpeg", 310 * 1024, jpeg)).toBe(
      "This image is still 310 KB at 640 px; an image may be at most 300 KB. Crop it and try again.",
    );
  });
});

describe("imageBytesOf and imageBudgetError", () => {
  const tiles = [
    tile({ id: "a", image: { data_url: urlOf(1000) } }),
    tile({ id: "b", image: { data_url: urlOf(2000) } }),
    tile({ id: "c", image: null }),
    tile({ id: "d", type: "viz", viz_id: 1 }),
  ];

  it("adds up every image, leaving out the one being replaced", () => {
    expect(imageBytesOf(tiles)).toBe(3000);
    expect(imageBytesOf(tiles, "a")).toBe(2000);
  });

  it("refuses what would take the dashboard over its total", () => {
    expect(imageBudgetError(IMAGES_TOTAL_MAX_BYTES - 10, 10)).toBeNull();
    expect(imageBudgetError(1900 * 1024, 300 * 1024)).toBe(
      "The images on this dashboard would take 2.2 MB; a dashboard holds at most 2 MB of images.",
    );
  });
});

describe("image tile edits", () => {
  const url = "data:image/png;base64,AAAA";

  it("shows only with a picture", () => {
    expect(imageTileShows(tile())).toBe(false);
    expect(imageTileShows(tile({ image: { data_url: url } }))).toBe(true);
    expect(imageTileShows(tile({ type: "text", image: { data_url: url } }))).toBe(false);
  });

  it("a new picture keeps the alt text and fit", () => {
    expect(withImage(tile(), url).image).toEqual({ data_url: url, alt: null, fit: "contain" });
    const set = tile({
      image: { data_url: "data:image/gif;base64,AA==", alt: "Logo", fit: "cover" },
    });
    expect(withImage(set, url).image).toEqual({ data_url: url, alt: "Logo", fit: "cover" });
  });

  it("a fit or alt text that changes nothing is no edit", () => {
    const set = tile({ image: { data_url: url, alt: "Logo", fit: "contain" } });
    expect(withImageFit(set, "contain")).toBeNull();
    expect(withImageFit(tile(), "cover")).toBeNull();
    expect(withImageFit(set, "cover")?.image?.fit).toBe("cover");
    expect(withImageAlt(set, "  Logo ")).toBeNull();
    expect(withImageAlt(set, "  ")?.image?.alt).toBeNull();
    expect(withImageAlt(set, "Acme logo")?.image).toEqual({
      data_url: url,
      alt: "Acme logo",
      fit: "contain",
    });
    expect(withImageAlt(set, "x".repeat(400))?.image?.alt).toHaveLength(300);
  });
});
