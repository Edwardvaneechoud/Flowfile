# Agent prompt — produce all SVG-able docs diagrams

Paste the block below to kick off an ultracode / Opus run that creates every remaining
Flowfile docs diagram that can be an SVG (the 18 "diagrams to design"), and wires each
into its page. Screenshots are out of scope (they need the running app). The full
per-diagram brief lives in [`DOCS_IMAGE_TODO.md`](DOCS_IMAGE_TODO.md) §2.

Three exemplars are already shipped and define the target style:
`docs/assets/images/concepts/{positioning-spectrum,system-boundary,catalog-ecosystem-loop}.svg`.

---

```
Ultracode task: produce every Flowfile docs diagram that can be an SVG.

## Scope
Create the 18 "diagrams to design" listed in DOCS_IMAGE_TODO.md §2 as hand-authored,
self-contained SVGs, and wire each into its docs page. Do NOT touch the 10 "screenshots
to capture" (those need the running app). Do NOT redo the 3 already-done SVGs
(positioning-spectrum, system-boundary, catalog-ecosystem-loop).

The full brief for each diagram — target doc page, destination path, "what to create",
and an "Icons & illustration" note — is in DOCS_IMAGE_TODO.md §2, tables
"Diagrams to design — concept (13)" and "Diagrams to design — developer (draw.io) (5)".
Read that file first; it is the source of truth for content per diagram.

Worklist (18):
Concept (13): recipe-to-flow, flow-assembly-line, dev-vs-performance, sheet-vs-flow,
vlookup-to-join, analyst-loop, export-vs-publish, catalog-fan-out, connection-store,
sync-architecture, code-canvas-duality, team-deployment-architecture, sharing-model.
Developer draw.io (5): architecture-overview, access-resolution, virtual-table-resolution,
trigger-cascade, notebook-anatomy (all under docs/assets/images/guides/catalog/).

## Style — match the exemplars exactly
Study these three already-shipped SVGs and copy their conventions:
  docs/assets/images/concepts/system-boundary.svg
  docs/assets/images/concepts/catalog-ecosystem-loop.svg
  docs/assets/images/concepts/positioning-spectrum.svg
Rules (also in DOCS_IMAGE_TODO.md "Illustration style guide"):
- Brand palette ONLY: cyan #2DD5D3, cerulean #26A8E0, royal #1D76D6, navy #233588,
  neutral grey #9aa0a6, subtitle grey #6b7280, arrow grey #b3bcc9. NO orange.
- Color hierarchy = the message: the Flowfile "hero" element in a cyan→royal
  linearGradient; the single accent that points (a loop/highlight arrow) in cyan
  #2DD5D3; everything being replaced/contrasted (spreadsheets, copies, neighbors,
  bypass paths) in neutral grey.
- Cards: white fill, grey (or brand) border, rounded; titles navy #233588 (600),
  subtitles #6b7280 (~11px); icons in a light chip.
- Hand-draw ALL icons as simple line glyphs (fill:none, stroke, round caps/joins,
  stroke-width ~1.7). Do NOT depend on Material icon fonts or any external/remote
  resource — every SVG must be fully self-contained (no <image> to a URL, no CDN,
  no external CSS/fonts). font-family: Roboto, Arial, sans-serif.
- Arrowheads via <marker>; reuse the <defs> gradient+marker pattern from
  catalog-ecosystem-loop.svg.
- viewBox only, no fixed width/height (responsive). Transparent canvas so white cards
  read on both light and dark site themes.
- recipe-to-flow is the ONE warm/hand-drawn (Excalidraw-style, warm paper tone)
  exception; every other diagram is flat and cool. The 5 developer ones are technical
  flowcharts: orthogonal edges, decision diamonds, brand for the happy path, grey for
  bypasses/neighbors.

## Per-diagram procedure (do all of this for each)
1. Read its DOCS_IMAGE_TODO.md row + open the target doc page for surrounding context/voice.
2. Author the SVG at the suggested path but with a .svg extension (create
   docs/assets/images/guides/catalog/ if missing for the dev diagrams).
3. Validate XML:  python3 -c "import xml.dom.minidom; xml.dom.minidom.parse('PATH')"
4. Render and VISUALLY VERIFY both themes (rsvg-convert is installed):
     rsvg-convert -w 940 -b white  PATH -o /tmp/prev_light.png
     rsvg-convert -w 620 -b '#1e1e1e' PATH -o /tmp/prev_dark.png
   Read BOTH PNGs. Check: icons are recognizable, no overlapping shapes/text, text
   readable in light AND dark, arrowheads land on card edges, alignment is clean.
   Iterate until it looks professional and matches the exemplars' polish.
5. Wire it in: replace the matching <!-- IMAGE-PLACEHOLDER-TO-CHANGE ... --> comment on
   that page with  ![descriptive alt text](RELATIVE/PATH.svg)  (concept pages use
   assets/images/concepts/... ; catalog-architecture.md uses ../assets/images/guides/catalog/...).
6. Do NOT edit DOCS_IMAGE_TODO.md from parallel workers (shared-file conflict).

## Orchestration
Each diagram edits a different doc page, so fan out — ideally one subagent per diagram in
parallel. After all diagrams are done, run a single serial finalization:
- Update DOCS_IMAGE_TODO.md: mark all 18 done, remove their rows, fix the counts/status.
- Build once and confirm exit 0 with no new warnings about missing images/snippets:
    FLOWFILE_SKIP_STARTUP_MIGRATION=1 DISABLE_MKDOCS_2_WARNING=true poetry run mkdocs build
- Report: each SVG path + one-line description, and confirm the build is clean.

## Definition of done
18 self-contained SVGs created, each visually verified in light+dark, each wired into its
page (placeholder comment removed), DOCS_IMAGE_TODO.md updated, mkdocs build green.
Quality bar: indistinguishable in style/polish from system-boundary.svg and
catalog-ecosystem-loop.svg. When an icon reads ambiguously, redraw it.
```

---

## Notes

- **The spec lives in `DOCS_IMAGE_TODO.md`**, not duplicated here — that file has the per-diagram "what to create" + icon guidance and the shared style guide, so this prompt only carries conventions and process.
- **The render-and-eyeball loop (light + dark) is the load-bearing step.** An agent that skips it will produce plausible-but-broken SVGs (misaligned text, unrecognizable icons). It's a hard requirement in the prompt.
- **Concurrency is safe** because each diagram edits a different doc page; the one shared file (`DOCS_IMAGE_TODO.md`) and the final `mkdocs build` are deferred to a single serial step.
- **Self-contained SVG is emphasized** because embedding Material icon fonts or remote refs breaks under the docs CSP and in offline builds.
- **The 5 developer draw.io flowcharts are the hard part** (decision diamonds, cascade sequences). If the run is time-boxed, do the 13 concept diagrams first and the 5 flowcharts as a second pass.
