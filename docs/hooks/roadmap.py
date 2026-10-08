"""Render ``docs/roadmap.md`` from the repository's ``ROADMAP.md`` so the two never drift.

The root file is written for GitHub, where images live under ``docs/assets/``; the docs page
lives inside ``docs/``, so those paths lose their ``docs/`` prefix here.
"""

from pathlib import Path


def on_page_markdown(markdown: str, page, config, files) -> str:
    if page.file.src_uri != "roadmap.md":
        return markdown
    root = Path(config.config_file_path).parent
    text = (root / "ROADMAP.md").read_text(encoding="utf-8")
    return text.replace("](docs/assets/", "](assets/")
