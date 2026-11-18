// @vitest-environment happy-dom
// Core accepts SVG images, so a stored picture may carry script: it is harmless only as an <img> source.
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { mountIn } from "../../__fixtures__/mount";
import type { DashboardTile } from "../../types";

vi.mock("element-plus", () => ({ ElMessage: { warning: vi.fn(), info: vi.fn() } }));
vi.mock("@element-plus/icons-vue", () => ({ Picture: { render: () => null } }));

import ImageTile from "./ImageTile.vue";

const hostileSvg = `data:image/svg+xml;base64,${btoa(
  '<svg xmlns="http://www.w3.org/2000/svg" onload="alert(1)"><script>alert(2)</script></svg>',
)}`;

const tile: DashboardTile = {
  id: "img-1",
  type: "image",
  viz_id: null,
  chart_index: 0,
  image: { data_url: hostileSvg, alt: "Logo", fit: "contain" },
  x: 0,
  y: 0,
  w: 8,
  h: 3,
} as DashboardTile;

// Vitest runs from flowfile_frontend; under happy-dom import.meta.url is not a file URL.
const APP_DIR = resolve(process.cwd(), "src/renderer/app");
const HTML_SINKS = /v-html|innerHTML|outerHTML|insertAdjacentHTML|<object|<embed|<iframe|srcdoc/;
const IMAGE_BYTES = /data_url/;

const sources = (dir: string): string[] =>
  readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__fixtures__" ? [] : sources(path);
    return /\.(vue|ts)$/.test(name) && !name.endsWith(".test.ts") ? [path] : [];
  });

describe("an image tile's picture", () => {
  it("is drawn as an <img> source, never inlined", async () => {
    const host = await mountIn(ImageTile, { props: { tile, mode: "view" } });

    const imgs = host.querySelectorAll("img");
    expect(imgs).toHaveLength(1);
    expect(imgs[0].getAttribute("src")).toBe(hostileSvg);
    expect(host.querySelector("svg, script, object, embed, iframe")).toBeNull();
  });

  it("never reaches an HTML sink in a file that reads it", () => {
    const files = sources(APP_DIR);
    expect(files.some((path) => path.endsWith("ImageTile.vue"))).toBe(true);
    const offenders = files
      .filter((path) => {
        const text = readFileSync(path, "utf8");
        return IMAGE_BYTES.test(text) && HTML_SINKS.test(text);
      })
      .map((path) => relative(APP_DIR, path));
    expect(offenders).toEqual([]);
  });
});
