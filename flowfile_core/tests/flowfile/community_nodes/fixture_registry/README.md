# Community-nodes fixture registry

A tiny, self-contained community registry (`index.json` + two node folders under
`nodes/`, with computed sha256 pins) used both by the `community_nodes` pytest suite and
for manual, offline frontend testing before the real `flowfile-community-nodes` repo
exists. `mood_emoji` carries an icon, screenshot, README and popularity entry;
`uppercase_text` is minimal (no media, no settings, no popularity) so the media-absent and
popularity-absent code paths are exercised.

**Manual testing:** restart core with
`FLOWFILE_COMMUNITY_INDEX_URL=<abs-path-to-this-dir>/index.json` (a local filesystem path
switches the client into fixture mode) to browse and install these nodes in the UI with no
network access.
