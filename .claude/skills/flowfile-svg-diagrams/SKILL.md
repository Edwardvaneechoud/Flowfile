---
name: flowfile-svg-diagrams
description: How to author, wire, and verify the hand-drawn brand SVG diagrams on the Flowfile docs site — the exact file contract (viewBox, role + aria-label, defs→style→content), the shipped palette and typography ladders with exact hex values, the reusable component library (node chips, preview tables, arrow markers, decision diamonds, storage substrates, greyed insets), the concept-vs-technical register split and the one warm exception, the dark-mode technique against the two page canvases, the labeled-placeholder pattern for screenshots, and the redraw lessons the maintainer has already enforced. Use when creating or editing any SVG under docs/assets/images/, when a docs page needs a new concept or architecture diagram, when replacing or adding an IMAGE-PLACEHOLDER, when a diagram reads badly in dark mode, or when asked to draw, redraw, or fix any docs illustration.
---

# Flowfile docs SVG diagrams — authoring, wiring, verification

## When NOT to use this skill

- **Screenshots, gifs, raster captures** — never create or edit those (`flowfile-docs-review` §1.11); ship a labeled placeholder (§8) and track the capture in `DOCS_IMAGE_TODO.md`.
- Page prose, alt-text register rules, and fact-checking doctrine → `flowfile-docs-review` (this skill owns the image file; that one owns the page around it).
- Site build mechanics beyond the §9 verification recipe → `flowfile-docs-and-writing`.

## 0. What these are

Hand-authored, fully self-contained SVGs — no external refs (an SVG loaded via `![]()` cannot fetch anything), no icon fonts, no rasters, no drop shadows. Directories:

| Directory | Register | Examples |
|---|---|---|
| `docs/assets/images/concepts/` | concept (persona / what-is pages) | `flow-assembly-line`, `analyst-loop`, `catalog-ecosystem-loop` |
| `docs/assets/images/architecture/` + `guides/catalog/` | technical (for-developers pages) | `process-map`, `access-resolution`, `trigger-cascade` |
| `docs/assets/images/nodes/` | app node glyphs (source material, don't restyle) | `filter.svg`, `input_data.svg` — 31 files |
| `docs/assets/images/guides/sales_dashboard/` | labeled placeholders awaiting screenshots | `dashboard_overview.svg` |

Healthy size is **5–17 KB**. The one 82 KB outlier (`concepts/positioning-spectrum.svg`) carries a ~44 KB base64 PNG of the logo in a 60×64 slot — a known anti-pattern, not a license. That file (and `recipe-to-flow`) also predates the house root-element pattern; **new files follow §2, not those two.**

## 1. Design doctrine — decide before drawing

- **Color hierarchy is the message.** In the concept register, brand identity is carried by the **single gradient hero** and the cyan accent — supporting cards stay white with grey strokes even when they depict Flowfile-owned things; grey-vs-brand contrast is reserved for the argument (the old painful way vs the Flowfile way). In the technical register, Flowfile boxes take `#1D76D6` strokes and neighbors/bypasses go grey.
- **One cyan path carries the eye** — the hero edge, the refresh loop, the publish arrow. A symmetric fan of same-meaning spokes counts as *one* path and may be all-cyan (`catalog-fan-out`); when the diagram also has a distinct action edge, that edge takes the cyan and the fan stays grey (`connection-store`, the rotate-credential arrow). Never two competing accents.
- **Contrast of complexity sells the argument.** The "before/painful/many copies" side: small, muted, tangled. The Flowfile side: clean, gradient, single. Left→right reading.
- **Label budget**: concept diagrams ship at **25–55 words** of visible text; technical diagrams run **~60–160** (`process-map` is the ceiling at ~160). Per element: 1–3-word bold title, ≤4-word grey subtitle, 1–3-word arrow labels. Fake data in preview tables is **dash lines, never words** (§5).
- **Restraint.** Resist the fifth icon, the extra source logo, the second legend row. Every redraw in the history (§10) was a *removal*.
- **Pick a composition pattern** from the shipped catalog rather than inventing one:

| Pattern | Use when | Shipped example |
|---|---|---|
| Left-vs-right contrast | before/after, bad/good of the same job | `export-vs-publish`, `sheet-vs-flow`, `vlookup-to-join` |
| Hub-and-spoke fan-out | one producer, N consumers — the spokes *are* the message | `catalog-fan-out`, `system-boundary` (undirected ring) |
| Center hub with mirrored wings | one shared object referenced from two audience sides, plus one action accent | `connection-store` |
| Loop (cycle with a return arrow) | a self-sustaining cycle (refresh, publish→consume→refresh) | `catalog-ecosystem-loop`, `analyst-loop` |
| Layered bands / left→right pipeline | staged movement through processes or stations | `sync-architecture`, `process-map`, `trigger-cascade` |
| Twin panels + shared center object | two equal representations of one thing | `code-canvas-duality` |
| Stacked dashed panels (stack vs state) | runtime processes vs persistent volumes | `team-deployment-architecture` |
| Vertical layers (router → services → storage) | what-calls-what above what-is-stored | `architecture-overview` |

## 2. The file contract

Root element, exactly this shape — nothing more:

```xml
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 W H" role="img" aria-label="...">
```

- **W:** 880–980 (typically 940) for concept diagrams; 940–1340 for technical (wider = denser). **H:** whatever the content needs (shipped range 330–900). **Never** set `width`, `height`, or `preserveAspectRatio`; no `<title>`/`<desc>`.
- **`aria-label`** is one long declarative sentence that *argues the diagram* — entities, relationships, takeaway — mirroring the markdown alt text (§9). Not a description of shapes.
- **Order:** `<defs>` (gradients, markers) → `<style>` (class system) → content. Section banner comments split the body: `<!-- ============ CATALOG HUB ============ -->`; inline comments annotate semantics (`<!-- struck duplicate row -->`).
- One `<g>` per card/node/section; icons as nested `<g class="ic" transform="translate(x,y)">` groups drawn on a ~24-unit grid; ids short lowercase (`brand`, `ah`, `ahc`); classes 1–5 lowercase letters (`.box .chip .ic .t .s .arw .arwc .albl`).
- **Arrows are drawn before cards** (they run behind), labels last within each group.

## 3. Palette — exact values

The brand gradient, declared identically in every post-redraw file that uses a gradient (19 of 22 — `sheet-vs-flow`, `system-boundary`, and `node-palette-annotated` have none or per-node ones only):

```xml
<linearGradient id="brand" x1="0" y1="0" x2="1" y2="1">
  <stop offset="0" stop-color="#2DD5D3"/><stop offset="0.55" stop-color="#26A8E0"/><stop offset="1" stop-color="#1D76D6"/>
</linearGradient>
```

Navy `#233588` is **ink**, not a gradient stop: titles, storage styling, dark accents. (The 4-stop cyan→navy gradient belongs to the landing page hero in `docs/index.html`; `extra.css` defines the four brand hexes as variables plus a 2-stop card-border gradient. Diagrams use the 3-stop version above.)

| Role | Values |
|---|---|
| Titles / ink | `#233588` |
| Body / subtitle grey | `#6b7280` · tertiary `#8a93a3` · icon grey `#5b6470` |
| Card stroke, concept register (ALL non-hero cards, Flowfile-owned included) | `#9aa0a6`, width 1.4–1.5 |
| Flowfile box stroke (technical register only) | `#1D76D6`, width 1.5–1.7 (hub emphasis 2.3); neutral actors keep `#9aa0a6` |
| Ordinary edges | stroke `#b3bcc9` w 1.7–1.9, arrowhead `#9aa8b6` |
| Hero/accent edge (one per diagram) | stroke `#2DD5D3` w 3 (or 2.6), arrowhead `#2DD5D3` |
| Chips | grey `#eef0f2` = concept-register default · blue `#eaf3fc` = technical-register default (and table-header/highlight tint) · navy `#dfe6ff` = storage boxes · on-gradient `rgba(255,255,255,0.16)` |
| Storage substrate (technical) | fill `#eef2ff`/`#eef4fb`, stroke `#233588`/`#8fb0d6`, navy text `#4a5578` |
| Dimmed/skipped node | fill `#e9edf1`, stroke `#cdd4dc`, label `#a9b0ba`, dashed edge `#c7cdd5` |
| Greyed inset ("the manual way") | `fill:rgba(120,131,146,0.13); stroke:#8a93a3; stroke-dasharray:6 5` |
| Cards / surfaces | `#ffffff` |

**The two canvases.** Transparent SVGs render on light `#fff` and slate ≈`#1e2129` (Material `hsla(225,15%,14%,1)`; `extra.css` does not override the background). The default technique is **opaque surfaces + mid-tone floating text**: put text on white cards or on the gradient (`#ffffff` / `#eaf6ff`), and keep the few free-floating labels in mid-greys `#6b7280`/`#8a93a3`, which pass on both canvases. When a diagram carries a lot of floating grey text (kickers, captions, dimmed labels), add the paired dark-mode CSS inside `<style>` — media query **plus** the Material theme attribute, both required because the toggle sets `data-md-color-scheme` independently of the OS:

```css
/* .kick = 12.5px grey kicker · .cap = 11px grey caption · .hd = 12.5px w600 NAVY floating heading */
@media (prefers-color-scheme: dark) {
  .kick, .cap { fill:#aeb6c0; }
  .hd { fill:#c7cfd8; }
}
[data-md-color-scheme="slate"] .kick, [data-md-color-scheme="slate"] .cap { fill:#aeb6c0; }
[data-md-color-scheme="slate"] .hd { fill:#c7cfd8; }
```

Rule: any **floating navy text** (a heading not sitting on a card) must carry an `.hd`-style dark override — navy ink vanishes on slate. (Shipped in `flow-assembly-line.svg` and `node-palette-annotated.svg`, each with its own class list; never `var(--md-...)`.)

## 4. Typography

Stack: `font-family:Roboto,Arial,sans-serif` on text classes (never on the root). The ladder:

| Size / weight | Role | Fill |
|---|---|---|
| 17–19px / 700 | Hub title on gradient | `#ffffff` (subtitle 11.5px `#eaf6ff`) |
| 17–18px / 600–700 | Diagram title (technical pages) | `#26A8E0` |
| 12.5–15px / 600 | Card/node title `.t` (14–15 in concept files; dense technical files shrink to 12.5–13.5) | `#233588` |
| 15px / 700 | Gradient decision-gate text `.dt` | `#ffffff` |
| 12–12.5px | Kicker / caption / overline | `#6b7280` |
| 10.5–11px | Subtitles `.s`, arrow labels `.albl` | `#6b7280` / `#8a93a3` |
| 9.5–10px | Fine print `.ss`, footnotes, node names | `#5b6470` / `#8a93a3` |
| 9px / 700 + `letter-spacing:.6–1px` | ALL-CAPS region tags and kickers only | `#8a93a3` / `#d3f7f6` |

Monospace appears where the *content* is code or ports, in either register: `process-map.svg` has `.mono { font-family:'Roboto Mono',Menlo,Consolas,monospace; font-size:9px; fill:#5b6470; }` for env vars and CLI strings, and the concept files `code-canvas-duality.svg` (`.code`, and a 12px/700 `#1D76D6` monospace `.albl`) and `team-deployment-architecture.svg` (`.port`) use `ui-monospace`-first stacks. Short code identifiers (`spawn_flow_run`, `GET /api/data/{slug}`) usually stay in the proportional face. Beware: `vlookup-to-join.svg` has a class *named* `.mono` that is actually proportional. Separate label fragments with `&#183;` (·).

## 5. Component library — copy from the shipped files, don't reinvent

**Arrow markers** (identical geometry everywhere; recolored clones for variants — `ahb` `#1D76D6`, `ahn` `#233588`; caution: the id `ahm` collides across files — dimmed `#c7cdd5` in `dev-vs-performance` but manage-royal `#1D76D6` in `sharing-model`, so check the target file's `<defs>` before reusing it):

```xml
<marker id="ah" markerWidth="10" markerHeight="10" refX="7.5" refY="4.5" orient="auto"><path d="M1,1 L8,4.5 L1,8 Z" fill="#9aa8b6"/></marker>
<marker id="ahc" markerWidth="11" markerHeight="11" refX="7.8" refY="5" orient="auto"><path d="M1,1.2 L8.6,5 L1,8.8 Z" fill="#2DD5D3"/></marker>
```

**The canonical concept-register `<style>` block** (shared by the shipped concept files — start from this, don't reconstruct it from the tables):

```css
.box  { fill:#ffffff; stroke:#9aa0a6; stroke-width:1.4; }
.chip { fill:#eef0f2; }
.ic   { fill:none; stroke:#5b6470; stroke-width:1.7; stroke-linecap:round; stroke-linejoin:round; }
.icw  { fill:none; stroke:#ffffff; stroke-width:1.8; stroke-linecap:round; stroke-linejoin:round; }
.t    { font-family:Roboto,Arial,sans-serif; font-weight:600; font-size:14px; fill:#233588; }
.s    { font-family:Roboto,Arial,sans-serif; font-size:10.5px; fill:#6b7280; }
.kick { font-family:Roboto,Arial,sans-serif; font-size:12.5px; fill:#6b7280; }
.albl { font-family:Roboto,Arial,sans-serif; font-size:11px; fill:#6b7280; }
.arw  { fill:none; stroke:#b3bcc9; stroke-width:1.8; }
.arwc { fill:none; stroke:#2DD5D3; stroke-width:3; }
```

**Card with icon chip** — the universal building block (white rect rx 10–12 + chip square rx 7–10 + stroke icon + `.t`/`.s` text):

```xml
<g>
  <rect class="box" x="70" y="95" width="165" height="84" rx="11"/>
  <rect class="chip" x="83" y="117" width="40" height="40" rx="8"/>
  <g class="ic" transform="translate(91,125)">
    <path d="M4,15 L9,9 L14,13 L20,6"/>
    <circle cx="4" cy="15" r="1.7" fill="#5b6470"/><circle cx="9" cy="9" r="1.7" fill="#5b6470"/>
    <circle cx="14" cy="13" r="1.7" fill="#5b6470"/><circle cx="20" cy="6" r="1.7" fill="#5b6470"/>
  </g>
  <text class="t" x="132" y="133">Flows</text>
  <text class="s" x="132" y="150">shape the data</text>
</g>
```

Icons are **bespoke ~24×24 stroke drawings** (`fill:none; stroke-width:1.7; round caps/joins`) — never icon-font glyphs, never filled MDI path data (that exists only in the legacy `positioning-spectrum.svg`).

- **Canvas flow node**: `<circle r="21" fill="url(#brand)"/>` + white icon (`.icw`, stroke 1.8) + 9.5px label below; mini 44×26 preview beneath. Or scale a 100-unit master node (`sheet-vs-flow` uses `scale(0.46)`).
- **Mini preview table**: white rect + tinted header strip (`#eaf3fc`/`#eef0f2`) via the double-rect trick; **data is dash lines** (`#9cc4ea` headers, `#a9b2bd`/`#c4cad2` cells), with semantics drawn in: struck duplicate row, cyan-tinted new column + `fx`, highlighted join column `#eaf3fc`+`#7fb2e6`, Σ totals band.
- **Hero emphasis**: heavier outline (`stroke-width:2.3`) or gradient fill + white text/icon + translucent chip; optional all-caps `.tag` kicker (`9px w700 letter-spacing:1px fill:#d3f7f6`).

**Technical-register devices** (do not use in concept diagrams):

- **Cluster container**: `fill:#eef5fd; stroke:#1D76D6; stroke-width:1.7; rx 16` + 14px w700 title ("Core process · FastAPI :63578").
- **Decision diamonds**: raw 4-point `<polygon>`. Plain: white fill, `#1D76D6` stroke 1.5, navy 11px question text. Hero gate: `fill="url(#brand)"` + white 15px w700 text, grey 10px condition fine-print below, `yes`/`no` as `.albl` at the exits.
- **Notes/callouts**: dashed white rects `stroke:#b9c2d0; stroke-dasharray:5 4; rx 8` with 10.5px navy title + 10px grey body. Dashed rect stroke = annotation/exception/soft grouping.
- **Dash vocabulary for edges**: dashed = *data movement or background machinery* (`5 5` Arrow IPC/Delta, `6 6` shared-volume, dotted `1.5 5` return callback), solid grey = control calls, solid cyan = the one primary path. Legend top-right: 32–34px sample strokes + 11–12px `#6b7280` labels.
- **Numbered execution paths**: Unicode circled digits `&#9312;`–`&#9316;` (①–⑤) inline in bold 11.5px labels (`#2489cf`, cyan variant `#12a8c2`) — not drawn circles. Drawn `r="12"` gradient badges are for table markers (X/Y); pill counters (`rx 8.5`, `#eef0f2`) for bounds ("≤ 5").

## 6. The two registers, and the one warm exception

| | Concept | Technical |
|---|---|---|
| Audience / pages | personas, what-is | `for-developers/` |
| Density | ~2 large panels, 25–55 words | 8–12 boxes, ports, env vars, legends |
| Gradients | `brand`; per-node hues ONLY when the scene depicts canvas nodes/glyphs (§7) — a flow as an entity is a neutral card | exactly one (`brand`) |
| Devices | metaphor scenes, insets | diamonds, clusters, substrates, `.mono`, ①–⑤ |
| Shared DNA | identical markers, Roboto ladder, `#233588` ink, grey ramp, white cards, rx 8–16 | same |

**The warm exception is `recipe-to-flow.svg` and only it** — the plain-language page is the one place a hand-drawn sketch is on-brand. Its techniques (don't reuse without maintainer direction): `feTurbulence`+`feDisplacementMap` "rough" filter on sketch strokes only (never text), `rotate(-2)` tilt, warm paper `#F4EDDE`, Caveat handwriting, flat brand-step node fills, open-chevron marker.

## 7. Real node glyphs

When a diagram depicts an actual Flowfile canvas, embed the real glyphs from `docs/assets/images/nodes/*.svg` (100×100 `circle r="48"` on a per-node vertical gradient + white strokes at 4.5): copy the glyph group verbatim, wrap in `<g transform="translate(…) scale(0.44)">`, re-declare its gradient locally under a renamed id. **Keep the app's node colors** (`input_data` `#15B6C9→#0C8FA0`, `filter` `#3D9BF2→#2376D8`, `group_by` `#6E7DF7→#4B58DC`, `join` `#9A6FF8→#7A45E6`…) — they are the app's palette, not the brand ramp; re-tinting them breaks "matches the app". Node *names* drawn as text must match the palette (`nodes.py` `name=` — it's "Filter data", not "Filter"; see §10).

## 8. The labeled-placeholder pattern (screenshot stand-ins)

A real file at the referenced path keeps `mkdocs build` warning-free while the capture is pending:

```xml
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 340" role="img" aria-label="Placeholder: Full dashboard on the canvas">
  <rect x="5" y="5" width="790" height="330" rx="14" fill="none" stroke="#8a8f98" stroke-width="2" stroke-dasharray="10 8"/>
  <text x="400" y="158" text-anchor="middle" font-family="system-ui, -apple-system, sans-serif" font-size="22" font-weight="600" fill="#8a8f98">Screenshot placeholder</text>
  <text x="400" y="196" text-anchor="middle" font-family="system-ui, -apple-system, sans-serif" font-size="18" fill="#8a8f98">Full dashboard on the canvas</text>
</svg>
```

Grey `#8a8f98` reads on both canvases. Pair it with an `<!-- IMAGE-PLACEHOLDER-TO-CHANGE: … -->` comment at the spot, keep the markdown alt short (a screenshot will replace it), and add a `DOCS_IMAGE_TODO.md` row. In tutorials, images sit in `<details markdown="1">` fold-outs (hero `open`); the swap is drop-in — overwrite the file, keep the basename.

## 9. Wire, fact-check, verify

1. **Wire inline**: plain `![alt](relative/path.svg)` immediately after the prose paragraph the diagram argues — no `<figure>`, no HTML `<img>`, and concept/technical diagrams are never in fold-outs (fold-outs are for tutorial screenshots).
2. **Alt text = the argument**, one long sentence with the exact names and values (ports, symbols, bounds), mean ~330 chars shipped. It must state the load-bearing semantics — the virtual-table alt spells out "both paths return a LazyFrame that stays lazy". Keep alt and `aria-label` telling the same story.
3. **Fact-check edges like prose claims** (`flowfile-docs-review` §3): every labeled edge is a claim about code. Precedent: `architecture-overview.svg` shipped a wrong "Delta write-back" kernel edge and was corrected against `kernel_runtime/kernel_runtime/flowfile_client.py` to `metadata → core` / `Delta → volume`.
4. **Build**: `FLOWFILE_SKIP_STARTUP_MIGRATION=1 poetry run mkdocs build` — a missing image is only a WARNING and the build still exits 0, so grep the output; a green build is not proof.
5. **Theme check — manual, no tooling exists**: `FLOWFILE_SKIP_STARTUP_MIGRATION=1 poetry run mkdocs serve`, open the page, click the Material header theme toggle, eyeball the SVG on `#fff` and on slate ≈`#1e2129`. Check every free-floating label.
6. **Track and stop**: a placeholder swap updates its existing `DOCS_IMAGE_TODO.md` row; a brand-new diagram gets added to that file's §2a diagram inventory (page + file). Leave everything uncommitted (standing agreement — the maintainer reviews via `git diff`).

## 10. Maintainer-rejected patterns — don't re-ship these

Each of these was shipped once and redrawn after feedback (commit `172c6ab0`, 2026-07-07):

- **Metaphor not literal** — `flow-assembly-line` v1 drew the belt as a strip *behind* the stations with previews dangling below; the fix made stations *sit on* one continuous belt with previews inside the cards. If the metaphor is an assembly line, the geometry must behave like one.
- **Clutter over legibility** — `sheet-vs-flow` v1: six data rows, five crossing arrows, a pivot overlapping the grid. Fix: five rows, three crossing arrows routed clear of the `=` marks, pivot moved out and enlarged. Fewer, clearer.
- **Structure must match the mental model** — `architecture-overview` v1 drew a ring of services around the substrates; the maintainer wanted *what's stored* visually distinct from *how you interact*: two layers, services above storage.
- **No duplicate edges for one route** — `process-map` had two lines between SqlService and Worker (SQL editor + notebook cells); merged into one line labeled with both.
- **Icons must match the house style** — `recipe-to-flow` v1 used generic icons; fixed by inlining the real node glyphs (§7).
- **Diagram claims are code claims** — the `architecture-overview` kernel-edge fix (§9.3).
- **Exact UI names in drawn text** — `node-palette-annotated.svg` shipped with "Filter" and six idealized category headers while the palette renders "Filter data" and seven (`NodeList.vue`); realigned 2026-07-07. Drawn node names come from `nodes.py` `name=`, category headers from `NodeList.vue` — same exact-names rule as prose.
- **No embedded rasters** — the 44 KB logo PNG inside `positioning-spectrum.svg` (§0).

## Provenance

Distilled 2026-07-07 from: the original illustration style guide (git `27cb76b9:DOCS_IMAGE_TODO.md`), full-file extraction of the 24 shipped diagrams (concept + technical + placeholder sets), `docs/stylesheets/extra.css` + `mkdocs.yml` (canvas colors), the `172c6ab0` redraw diff, and the maintainer feedback record (`docs-feedback.md`). Exact values quoted from the files; re-verify against a shipped sibling before departing from them.
