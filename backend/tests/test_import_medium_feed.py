"""The Medium feed import (``scripts/import_medium.py --feed``).

The importer's converters (markdownify, beautifulsoup4) are not app
dependencies, so this module skips without them; CI runs it in its own step
with both, and so does the daily ``import-medium-feed`` workflow before it
publishes anything. Offline: the feed is a fixture and no image is fetched.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("markdownify")
pytest.importorskip("bs4")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import import_medium  # noqa: E402

FEED = (Path(__file__).parent / "fixtures" / "medium" / "feed.xml").read_text(encoding="utf-8")

HELD = """---
title: "An older post"
medium_id: "bbbbbbbbbbbb"
---

Edited here.
"""


@pytest.fixture
def out(tmp_path, monkeypatch) -> Path:
    (tmp_path / "posts").mkdir()
    (tmp_path / "posts" / "an-older-post.md").write_text(HELD, encoding="utf-8")
    monkeypatch.setattr(import_medium, "OUT", tmp_path)
    return tmp_path


def _post(posts, slug):
    return next(p for p in posts if p["slug"] == slug)


def test_the_subtitle_is_read_in_each_of_the_three_opening_shapes() -> None:
    posts = import_medium.parse_feed(FEED)
    assert (
        _post(posts, "a-new-post-with-a-linked-subtitle")["subtitle"]
        == "How OpenCheck reads a feed"
    )
    assert (
        _post(posts, "a-post-that-opens-with-a-figure")["subtitle"]
        == "The subtitle, after the figure."
    )
    assert (
        _post(posts, "a-post-that-opens-with-its-title")["subtitle"]
        == "Medium shows this first paragraph as the subtitle"
    )


def test_a_feed_post_has_the_fields_an_export_post_has() -> None:
    post = _post(import_medium.parse_feed(FEED), "a-new-post-with-a-linked-subtitle")
    assert post["id"] == "aaaaaaaaaaaa"
    assert post["published"] == "2026-11-19T09:30:00.000Z"
    assert post["canonical"] == (
        "https://medium.com/@stephenabbottpugh/a-new-post-with-a-linked-subtitle-aaaaaaaaaaaa"
    )
    assert post["tag_slugs"] == ["opencheck", "beneficial-ownership", "mcp-server"]
    assert post["tags"] == ["Opencheck", "Beneficial Ownership", "Mcp Server"]


def test_only_new_opencheck_posts_are_written(out: Path) -> None:
    written = import_medium.import_feed(FEED, fetch=False)
    assert sorted(p.name for p in written) == [
        "a-new-post-with-a-linked-subtitle.md",
        "a-post-that-opens-with-a-figure.md",
        "a-post-that-opens-with-its-title.md",
    ]
    assert not (out / "posts" / "a-post-not-tagged-opencheck.md").exists()
    # A held post is never rewritten: the edit made here survives.
    assert (out / "posts" / "an-older-post.md").read_text(encoding="utf-8") == HELD
    # A second run finds nothing new.
    assert import_medium.import_feed(FEED, fetch=False) == []


def test_a_written_post_reads_like_an_imported_one(out: Path) -> None:
    import_medium.import_feed(FEED, fetch=False)
    text = (out / "posts" / "a-new-post-with-a-linked-subtitle.md").read_text(encoding="utf-8")
    assert 'subtitle: "How OpenCheck reads a feed"' in text
    assert 'medium_id: "aaaaaaaaaaaa"' in text
    assert '  - "MCP"' in text
    assert 'image: "/blog/media/a-new-post-with-a-linked-subtitle/01.webp"' in text
    # The subtitle is not repeated as the body's first paragraph.
    body = text.split("---", 2)[2]
    assert "reads a feed" not in body
    # A link to a post the blog holds points here, and the tracking pixel is gone.
    assert "(/blog/an-older-post)" in body
    assert "medium.com/_/stat" not in text
    assert "## A heading" in body

    titled = (out / "posts" / "a-post-that-opens-with-its-title.md").read_text(encoding="utf-8")
    assert "and more" not in titled
    assert "Medium shows this first paragraph" not in titled.split("---", 2)[2]
