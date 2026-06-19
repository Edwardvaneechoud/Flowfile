# Node Icon Styleguide (for Claude)

This is the design system for Flowfile's **node icons** (the glyphs shown in the
node palette and on canvas nodes). Read this before adding or editing any icon so
the set stays unified. The system was established in 2026-06; every icon in this
directory follows it.

## Design language (the "why")

Derived from the Flowfile **logo** — a sphere with a cyan→teal→blue→navy flowing
gradient. The icon system mirrors that: a **saturated, gradient-filled circle**
(the sphere) carrying a **flat white line-art glyph**. Modern, smooth, rounded,
friendly for non-technical users.

Two principles that make it work:
1. **Color = group.** Each node group has one hue. The flow narrative runs from
   the logo's bright cyan (Input) down to its deep navy (Output).
2. **Background-agnostic.** A saturated circle + white glyph reads correctly on
   BOTH the light palette panel and the dark canvas, so icons do **not** need
   `prefers-color-scheme` dark variants and never wash out. (This is why we
   abandoned the older pale-bg `--ff-icon-*` CSS-variable pattern that two icons
   — `window_functions`, `dynamic_rename` — originally used. Don't reintroduce it.)

## Two treatments

| Treatment | Used for | Background | Glyph |
|---|---|---|---|
| **Native** (default) | every first-party operation | saturated **group gradient** circle `r=48` | white line-art glyph |
| **Brand** | real third-party marks (Python, Polars, Google Analytics) | **same as native** — group gradient circle `r=48` | the brand's **mark recolored white** (the logo's silhouette/shape, no brand colors) |

Brand icons use the **identical** treatment to native — a group-gradient circle —
so they fully unify with the set; only the glyph shape differs (it's the brand's
mark instead of a drawn metaphor). The brand mark is rendered in **white** (so it's
"in the category color" = the circle is the category color). This was a deliberate
choice (2026-06): an earlier white-chip + colored-ring treatment was rejected for
not unifying. Don't reintroduce white chips or rings.

Kafka is **not** a real brand mark here (it was a gray placeholder) — it uses a
native teal **glyph** (a broadcast/stream motif). Treat any connector without a
clean logo as a native glyph.

## Per-group palette (the "Flow Spectrum")

Native circles use a vertical `linearGradient` (top lighter → bottom deeper):

| Group | Hue | gradient TOP | gradient BOTTOM | solid ref hue |
|---|---|---|---|---|
| **input** | Teal | `#15B6C9` | `#0C8FA0` | `#0EA5B7` |
| **transform** | Blue | `#3D9BF2` | `#2376D8` | `#2E8FE6` |
| **aggregate** | Indigo | `#6E7DF7` | `#4B58DC` | `#5B6CF0` |
| **combine** | Violet | `#9A6FF8` | `#7A45E6` | `#8B5CF6` |
| **output** | Navy | `#3A4FA6` | `#243A82` | `#2A3F8F` |
| **ml** | Amber | `#F0A52A` | `#D9810A` | `#D9810A` |

(Group membership per node lives in `flowfile_core/.../configs/node_store/nodes.py`
as `node_group=...`. Match the icon's hue to the node's group.)

## Native icon template (copy this)

```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100">
  <defs><linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="#3D9BF2"/><stop offset="1" stop-color="#2376D8"/></linearGradient></defs>
  <circle cx="50" cy="50" r="48" fill="url(#bg)"/>
  <g fill="none" stroke="#FFFFFF" stroke-width="4.5" stroke-linejoin="round" stroke-linecap="round">
    <!-- glyph here, white line-art, inside ~x:22-78 y:22-78 -->
  </g>
</svg>
```

## Brand icon template (copy this)

Same as the native template (group-gradient circle) but the glyph is the brand mark
recolored white.

**Vector logo** (Python, Google Analytics) — set every `fill` to `#FFFFFF`, centered:
```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" width="100" height="100">
  <defs><linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="#3D9BF2"/><stop offset="1" stop-color="#2376D8"/></linearGradient></defs>
  <circle cx="50" cy="50" r="48" fill="url(#bg)"/>
  <g transform="translate(24,24) scale(1.625)"><!-- logo paths, all fill="#FFFFFF" --></g>
</svg>
```

Prefer the **official vector** mark — source a clean single-path SVG (e.g. from
simple-icons / Streamline) and set its `fill="#FFFFFF"`, centered via a `transform`.
The current Polars bear is exactly this (the official "barcode bear" vector, white-on-blue
— crisp at 40px). Do NOT tint the raster Polars PNG: its bars become noisy static when
recolored.

**Raster fallback** (last resort, no vector available): if you must tint a raster whose
background is OPAQUE white, a plain alpha tint floods the whole circle — use a
**luminance-keyed tint** filter (dark pixels → white, white bg → transparent), clipped to
the circle:
```svg
<defs>
  <clipPath id="cl"><circle cx="50" cy="50" r="46"/></clipPath>
  <filter id="t" color-interpolation-filters="sRGB">
    <feColorMatrix type="matrix" values="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 -1 -1 -1 0 1" result="m"/>
    <feFlood flood-color="#FFFFFF"/><feComposite operator="in" in2="m"/>
  </filter>
</defs>
<image href="data:image/png;base64,…" x="-8" y="2" width="116" height="96"
       clip-path="url(#cl)" filter="url(#t)" preserveAspectRatio="xMidYMid meet"/>
```
(`feFlood flood-color` is the output color — `#FFFFFF` for white-on-category. The
`feColorMatrix` alpha row `-1 -1 -1 0 1` makes alpha = `1 − R − G − B`, i.e. dark→opaque,
white→transparent.)

## Hard rules (a glyph that breaks these looks off)

- `viewBox="0 0 100 100"`. Background circle `r=48` (native and brand alike).
- Glyph is **white only** (`#FFFFFF`), **line-art** (`fill="none"` strokes).
  Enclosed shapes get a faint glass fill: `fill="#FFFFFF" fill-opacity="0.16"`.
  Ghost/secondary elements: `stroke-opacity` or `fill-opacity` ~`0.35–0.4`.
- Stroke weights: **4.5** primary, **3.5** dense detail, **5** a single bold
  element. Always `stroke-linecap="round"` and `stroke-linejoin="round"`.
- **No text/letters baked in** (the old icons had "CATALOG"/"SQL SOURCE"/"RECORD"
  — gone). **No gradients on the glyph. No drop shadows. No `<style>` blocks, no
  CSS variables, no `@media` queries** — fully self-contained, ≤ ~6 glyph elements,
  legible at **24px** (palette) and **40px** (canvas).
- Keep one metaphor per node (don't reuse a metaphor across nodes). Input nodes
  share a downward "into the flow" arrow where it helps (`read`, `database_reader`,
  `cloud_storage_reader`, `external_source`).

## Metaphor catalog (current set — keep recognizable)

- **input (teal):** read=table+↓ · manual_input=pen on rows · external_source=globe+↓ ·
  cloud_storage_reader=cloud+↓ · database_reader=cylinder+↓ · catalog_reader=open book ·
  rest_api_reader={ } braces · kafka_source=broadcast arcs · google_analytics=GA bars (brand)
- **transform (blue):** filter=funnel · select=columns+✓ · formula=fx · sort=descending bars+↓ ·
  record_id=new ID column (sequential markers) + data rows · sample=bracketed subset over faint rows · unique=rows w/ duplicate ×'d ·
  text_to_rows=cell splitting · dynamic_rename=old→new · sql_query=cylinder+magnifier ·
  python_code=Python logo (brand) · polars_code=Polars bear (brand)
- **aggregate (indigo):** group_by=rows→1 · pivot=grid+rotate · unpivot=cols→stack ·
  record_count=brace+# · window_functions=bars in a window frame
- **combine (violet):** join=venn · cross_join=3×3 dot grid · union=stacks merge ·
  fuzzy_match=two nodes + ≈ · graph_solver=node graph · wait_for=hourglass
- **output (navy):** output=tray+↓ · cloud_storage_writer=cloud+↑ · explore_data=bars+magnifier ·
  api_response={ }+→ · database_writer=cylinder+↑ · catalog_writer=book+write
- **ml (amber):** train_model=graduation cap · apply_model=model box→row · evaluate_model=gauge ·
  random_split=forking arrows

## Wiring: three independent consumers (none share files)

Changing/adding an icon basename touches up to three places:

1. **Desktop/web** (primary):
   - File: this directory (`flowfile_frontend/.../designer/assets/icons/<name>.svg`).
   - `flowfile_core/flowfile_core/configs/node_store/nodes.py` → the node's `image="<name>.svg"`.
   - `flowfile_frontend/.../designer/utils.ts` → add `"<name>.svg"` to the `BUILTIN_ICONS` set
     (**exact-match**; a missing/mismatched entry silently falls through to the custom-icon
     backend URL → 404/broken image, with **no build error**).
2. **WASM** (`flowfile_wasm/`, only for the ~21 nodes it ships): copy the SVG into
   `src/assets/icons/`, then update `src/utils/iconUrls.ts` (import + map key),
   `src/components/Canvas.vue` (`nodeCategories` `icon:`), and
   `src/components/nodes/FlowNode.vue` (`iconMap`). Keep the `view.png` fallback.
3. **Docs** (optional, `docs/assets/images/nodes/` + the `nodes/*.md` markdown) — can lag.

Already-`.svg` basenames are **content-only** swaps (drop the file, no nodes.py/BUILTIN/WASM
edit). PNG→SVG basenames need the full tuple above.

### Audit (run after any change — catches the silent-404 risk)
For every `image="…"` in nodes.py, confirm the file exists in this dir AND is in
`BUILTIN_ICONS`. A quick Python check:
```python
import re, os
np="flowfile_core/flowfile_core/configs/node_store/nodes.py"
ut="flowfile_frontend/src/renderer/app/features/designer/utils.ts"
d="flowfile_frontend/src/renderer/app/features/designer/assets/icons"
imgs=[m for m in re.findall(r'image="([^"]+)"', open(np).read()) if m]
builtin=set(re.findall(r'"([^"]+)"', open(ut).read().split("BUILTIN_ICONS")[1].split("]")[0]))
for i in imgs:
    assert os.path.exists(f"{d}/{i}") and i in builtin, i
print("ok", len(imgs))
```

## Visual QA harness (IMPORTANT)

**`rsvg-convert`/librsvg renders these as solid black** if you ever reintroduce
CSS-var `<style>` icons — but even for the current self-contained icons, prefer
**Chromium** for QA (it's what the app uses, and it can emulate dark mode). Write
this to `/tmp` and run with the repo's Playwright (`flowfile_frontend/node_modules/playwright`):

```js
// render.mjs — node render.mjs <icons_dir> <out_prefix> [file.svg ...]
import pw from "/ABS/PATH/flowfile_frontend/node_modules/playwright/index.js";
const { chromium } = pw; import fs from "node:fs"; import path from "node:path";
const dir=process.argv[2], out=process.argv[3];
let files=process.argv.slice(4); if(!files.length) files=fs.readdirSync(dir).filter(f=>f.endsWith(".svg")).sort();
const cells=files.map(f=>{const b64=fs.readFileSync(path.join(dir,f)).toString("base64");
  return `<div class=c><div class=btn><img src="data:image/svg+xml;base64,${b64}" width=64 height=64></div><div class=l>${f}</div></div>`;}).join("");
const html=s=>`<style>body{margin:0;padding:20px;background:${s==="dark"?"#1b1f2a":"#f5f6f8"};font-family:sans-serif}
 .g{display:grid;grid-template-columns:repeat(7,1fr);gap:14px}.c{display:flex;flex-direction:column;align-items:center;gap:6px}
 .btn{width:76px;height:76px;border-radius:12px;background:#dedede;display:flex;align-items:center;justify-content:center}
 .l{font-size:10px;color:${s==="dark"?"#cbd3e1":"#444"};text-align:center}</style><div class=g>${cells}</div>`;
const b=await chromium.launch();
for(const s of ["light","dark"]){const c=await b.newContext({colorScheme:s,deviceScaleFactor:2});const p=await c.newPage();
  await p.setContent(html(s),{waitUntil:"networkidle"});await (await p.$(".g")).screenshot({path:`${out}_${s}.png`});await c.close();}
await b.close();
```
Then view `${out}_light.png` and `${out}_dark.png`. The `#dedede` button matches the
real `nodeButton.vue` background. Icons render at 40px in-app, 24px in the palette —
verify legibility at small sizes.

## Adding a brand-new node icon — checklist
1. Pick the node's group → use that gradient (native) or white-chip+ring (brand).
2. Author the SVG from the template; keep it ≤6 elements, legible at 24px.
3. Render via the harness (light+dark) and eyeball it next to siblings.
4. Wire all three consumers (or just desktop if the node isn't in WASM/docs).
5. Run the audit. Run `cd flowfile_frontend && npm run build:web` (compiles, but does
   NOT catch a missing icon — the audit + a Network-tab check for 404s does).
