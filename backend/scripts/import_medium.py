"""Import Stephen's OpenCheck posts from a Medium export into the blog.

Reads the ``posts/*.html`` files of Medium's "Download your information" zip
(the only source of every post: the author RSS feed holds the latest ten and
story pages answer 403 to scripts) and writes one Markdown file per post to
``data/blog/posts/<slug>.md``, with its images in ``data/blog/media/<slug>/``.
The blog (``opencheck/blog.py``) serves those files at ``/blog``.

A post is imported when it carries Medium's ``opencheck`` tag, or when its
Medium id is named with ``--id``. Drafts (``draft_*.html``) are never read.

Run it from ``backend/`` with the two converters it needs, which are not
dependencies of the app (Pillow, which writes the images as WebP, is)::

    uv run --with markdownify --with beautifulsoup4 \\
        python scripts/import_medium.py ~/Downloads/medium-export.zip

Re-running replaces every imported post, so a post edited on Medium can be
re-imported. A post edited here should then be edited here only: the
importer would overwrite the change. ``--keep-existing`` skips any slug that
already has a file.

The export zip holds personal data (sessions, IP addresses, e-mail
settings); never commit it.

``--feed`` reads Medium's author RSS feed instead of an export (Stephen,
20 Nov 2026). The feed carries the latest ten posts in full, so it is how a
new post reaches the blog without an export: the daily workflow
``import-medium-feed.yml`` runs it. It imports only OpenCheck-tagged posts
the blog does not already hold (matched on the Medium id), and never
rewrites one, so an edit made here is never overwritten::

    uv run --with markdownify --with beautifulsoup4 \\
        python scripts/import_medium.py --feed
"""

from __future__ import annotations

import argparse
import email.utils
import hashlib
import io
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup, Tag
from markdownify import MarkdownConverter

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "blog"

#: Medium's id is the trailing 12 hex characters of a story URL.
_ID_RE = re.compile(r"-([0-9a-f]{10,16})(?:[?#].*)?$")
#: A Medium story URL on any of its hosts.
_MEDIUM_HOSTS = ("medium.com", "stephenabbottpugh.medium.com")
#: The tag that marks an OpenCheck post (Stephen, 20 Nov 2026).
OPENCHECK_TAG = "opencheck"
#: Medium title-cases tag names ("Mcp Server"); the blog shows these instead.
_TOPIC_LABELS = {"Mcp Server": "MCP", "Ai": "AI"}
#: Image width asked of Medium's CDN: twice the 690px prose column.
_IMAGE_WIDTH = 1600
_USER_AGENT = "OpenCheck blog importer (+https://opencheck.world/blog)"
#: The author feed: the latest ten posts, in full, with their tags.
FEED_URL = "https://medium.com/feed/@stephenabbottpugh"
#: The address every imported post's ``medium_url`` uses, whichever host the
#: feed or export linked from.
_CANONICAL_BASE = "https://medium.com/@stephenabbottpugh/"
_CONTENT_NS = "{http://purl.org/rss/1.0/modules/content/}encoded"
#: Medium's CDN image id: the last path segment of a ``cdn-images-1`` URL.
_CDN_RE = re.compile(r"^https://cdn-images-1\.medium\.com/(?:max/\d+/)?(?P<id>[^/?#]+)$")


def medium_id(url: str) -> str | None:
    match = _ID_RE.search(urlparse(url).path)
    return match.group(1) if match else None


def slug_of(canonical: str) -> str:
    last = urlparse(canonical).path.rstrip("/").rsplit("/", 1)[-1]
    return _ID_RE.sub("", last)


def _is_medium_story(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host in _MEDIUM_HOSTS and medium_id(url) is not None


class _Converter(MarkdownConverter):
    """Headings as ATX, Medium's h3/h4 moved up one level (the page's h1 is
    the title), images with their caption as the Markdown title."""

    def convert_h3(self, el, text, parent_tags):  # type: ignore[no-untyped-def]
        return f"\n\n## {text.strip()}\n\n"

    def convert_h4(self, el, text, parent_tags):  # type: ignore[no-untyped-def]
        return f"\n\n### {text.strip()}\n\n"


def _yaml_str(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _download(url: str, dest: Path) -> None:
    """Fetch an image from Medium's CDN and store it as WebP (quality 85).

    Medium serves screenshots as PNG; the twelve posts' 33 images are 12.8 MB
    as PNG and 2.1 MB as WebP at this quality, with text still sharp.
    """
    from PIL import Image

    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:  # follows 301
        data = response.read()
    if not data:
        raise RuntimeError(f"empty image body from {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(io.BytesIO(data)) as image:
        image.save(dest, "WEBP", quality=85, method=6)


def _image_url(img: Tag) -> str:
    image_id = img.get("data-image-id")
    if image_id:
        return f"https://cdn-images-1.medium.com/max/{_IMAGE_WIDTH}/{image_id}"
    return str(img.get("src"))


def _local_image(url: str, slug: str, n: int, *, fetch: bool) -> str:
    name = f"{n:02d}.webp"
    dest = OUT / "media" / slug / name
    if fetch and not dest.exists():
        _download(url, dest)
    return f"/blog/media/{slug}/{name}"


def parse_post(raw: str) -> dict:
    soup = BeautifulSoup(raw, "html.parser")
    canonical_el = soup.select_one("a.p-canonical")
    time_el = soup.select_one("time.dt-published")
    if canonical_el is None or time_el is None:
        raise ValueError("not a published post (no canonical link or date)")
    canonical = str(canonical_el["href"])
    tags = [a.get_text(strip=True) for a in soup.select("a.p-tag")]
    tag_slugs = [str(a["href"]).rstrip("/").rsplit("/", 1)[-1] for a in soup.select("a.p-tag")]
    subtitle_el = soup.select_one('section[data-field="subtitle"]')
    return {
        "soup": soup,
        "title": soup.select_one("h1.p-name").get_text(" ", strip=True),
        "subtitle": subtitle_el.get_text(" ", strip=True) if subtitle_el else "",
        "published": str(time_el["datetime"]),
        "canonical": canonical,
        "id": medium_id(canonical),
        "slug": slug_of(canonical),
        "tags": tags,
        "tag_slugs": tag_slugs,
    }


def convert(post: dict, slugs_by_id: dict[str, str], *, fetch: bool = True) -> str:
    """The post as Markdown with YAML frontmatter."""
    soup: BeautifulSoup = post["soup"]
    body = soup.select_one('section[data-field="body"]')
    slug = post["slug"]

    for hr in body.select("hr"):
        hr.decompose()
    for sel in ("h3.graf--title", "h4.graf--subtitle"):
        for el in body.select(sel)[:1]:
            el.decompose()
    first_p = body.select_one("p")
    if (
        first_p is not None
        and post["subtitle"]
        and first_p.get_text(" ", strip=True).rstrip(".") == post["subtitle"].rstrip(".")
    ):
        first_p.decompose()

    # Link cards ("mixtape" embeds) → one link to the page they show.
    for embed in body.select("div.graf--mixtapeEmbed"):
        anchor = embed.find("a")
        strong = embed.find("strong")
        p = soup.new_tag("p")
        if anchor is not None:
            link = soup.new_tag("a", href=anchor.get("href"))
            link.string = (strong or anchor).get_text(" ", strip=True)
            p.append(link)
        embed.replace_with(p)

    # Pull quotes are emphasis, not quotations from someone else.
    for quote in body.select("blockquote.graf--pullquote"):
        quote.name = "blockquote"

    # Images: the first is the hero, the rest stay in place.
    hero = None
    n = 0
    for figure in body.select("figure"):
        img = figure.find("img")
        if img is None:
            figure.decompose()
            continue
        n += 1
        caption_el = figure.find("figcaption")
        caption = caption_el.get_text(" ", strip=True) if caption_el else ""
        local = _local_image(_image_url(img), slug, n, fetch=fetch)
        if hero is None:
            hero = {"src": local, "caption": caption}
            figure.decompose()
            continue
        p = soup.new_tag("p")
        new_img = soup.new_tag("img", src=local, alt=caption)
        if caption:
            new_img["title"] = caption
        p.append(new_img)
        figure.replace_with(p)

    # Links: a post imported here points here; Medium's tracking attributes go.
    for a in body.select("a[href]"):
        href = str(a["href"])
        if _is_medium_story(href):
            target = slugs_by_id.get(medium_id(href) or "")
            if target:
                a["href"] = f"/blog/{target}"
            else:
                a["href"] = href.split("?", 1)[0]
        for attr in ("data-href", "class", "rel", "target", "title"):
            if attr in a.attrs:
                del a[attr]

    markdown = _Converter(heading_style="ATX", bullets="-", strip=["span"]).convert_soup(body)
    markdown = re.sub(r"\n{3,}", "\n\n", markdown).strip() + "\n"
    markdown = markdown.replace(" ", " ")

    topics = [
        _TOPIC_LABELS.get(t, t)
        for t, s in zip(post["tags"], post["tag_slugs"], strict=True)
        if s != OPENCHECK_TAG
    ]
    lines = [
        "---",
        f"title: {_yaml_str(post['title'])}",
        f"subtitle: {_yaml_str(post['subtitle'])}",
        f"published: {_yaml_str(post['published'])}",
        f"medium_url: {_yaml_str(post['canonical'])}",
        f"medium_id: {_yaml_str(post['id'] or '')}",
        "topics:",
        *[f"  - {_yaml_str(t)}" for t in topics],
    ]
    if hero:
        lines.append(f"image: {_yaml_str(hero['src'])}")
        lines.append(f"image_alt: {_yaml_str(hero['caption'])}")
    lines.append("---")
    return "\n".join(lines) + "\n\n" + markdown


def _tag_name(slug: str) -> str:
    """A feed category (``mcp-server``) in the export's spelling (``Mcp Server``)."""
    return " ".join(word.capitalize() for word in slug.split("-"))


def _all_emphasis(p: Tag) -> bool:
    """Whether every word of a paragraph is inside ``<em>`` (links included)."""
    words = [t for t in p.find_all(string=True) if t.strip()]
    return bool(words) and all(t.find_parent("em") is not None for t in words)


def _take_subtitle(body: BeautifulSoup) -> str:
    """Remove the feed body's opening title and subtitle; return the subtitle.

    A feed body opens in one of three ways: the subtitle as an all-emphasis
    paragraph (its links kept inside the emphasis); a figure, then that
    paragraph; or an ``<h3>`` restating the title (the zip export's
    ``graf--title``, dropped as that is), where Medium shows the first
    paragraph as the subtitle, as the zip export's subtitle field holds it.
    """
    after_title = False
    for el in [c for c in body.children if isinstance(c, Tag)]:
        if el.name == "figure":
            continue
        if el.name == "h3" and not after_title:
            el.decompose()
            after_title = True
            continue
        if el.name == "p" and (after_title or _all_emphasis(el)):
            text = re.sub(r"\s+", " ", el.get_text("")).strip()
            el.decompose()
            return text.rstrip(".") if after_title else text
        return ""
    return ""


def parse_feed(xml_text: str) -> list[dict]:
    """Every post in Medium's RSS feed, in the shape ``parse_post`` returns.

    The feed's ``content:encoded`` is the body alone: the subtitle is its
    first paragraph when that paragraph is all emphasis, images are plain
    ``<figure><img>`` with a ``cdn-images-1`` source, and a tracking pixel
    (``medium.com/_/stat``) closes it.
    """
    root = ET.fromstring(xml_text)
    posts = []
    for item in root.iter("item"):
        link = (item.findtext("link") or "").split("?", 1)[0]
        content = item.findtext(_CONTENT_NS) or ""
        date = item.findtext("pubDate")
        if not link or not content or not date or medium_id(link) is None:
            continue
        body = BeautifulSoup(content, "html.parser")
        for img in body.select("img"):
            src = str(img.get("src") or "")
            if "medium.com/_/stat" in src:
                img.decompose()
                continue
            match = _CDN_RE.match(src)
            if match:
                img["data-image-id"] = match.group("id")
        subtitle = _take_subtitle(body)
        soup = BeautifulSoup('<section data-field="body"></section>', "html.parser")
        section = soup.section
        for el in list(body.contents):
            section.append(el)
        published = email.utils.parsedate_to_datetime(date)
        slugs = [c.text.strip() for c in item.findall("category") if c.text]
        last = urlparse(link).path.rstrip("/").rsplit("/", 1)[-1]
        posts.append(
            {
                "soup": soup,
                "title": (item.findtext("title") or "").strip(),
                "subtitle": subtitle,
                "published": published.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "canonical": _CANONICAL_BASE + last,
                "id": medium_id(link),
                "slug": slug_of(link),
                "tags": [_tag_name(t) for t in slugs],
                "tag_slugs": slugs,
            }
        )
    return posts


def _held_ids() -> set[str]:
    """The Medium ids of the posts the blog already holds."""
    held = set()
    for path in (OUT / "posts").glob("*.md"):
        match = re.search(r'^medium_id: "([0-9a-f]+)"$', path.read_text(encoding="utf-8"), re.M)
        if match:
            held.add(match.group(1))
    return held


def _held_slugs_by_id() -> dict[str, str]:
    out = {}
    for path in (OUT / "posts").glob("*.md"):
        match = re.search(r'^medium_id: "([0-9a-f]+)"$', path.read_text(encoding="utf-8"), re.M)
        if match:
            out[match.group(1)] = path.stem
    return out


def import_feed(xml_text: str, *, fetch: bool = True) -> list[Path]:
    """Write every OpenCheck post in the feed the blog does not hold yet.

    Never rewrites a held post (matched on the Medium id, or on a file of the
    same slug), so a post corrected here keeps its correction. Returns the
    files written.
    """
    posts = [p for p in parse_feed(xml_text) if OPENCHECK_TAG in p["tag_slugs"]]
    held = _held_ids()
    slugs_by_id = {**_held_slugs_by_id(), **{p["id"]: p["slug"] for p in posts}}
    written = []
    (OUT / "posts").mkdir(parents=True, exist_ok=True)
    for post in sorted(posts, key=lambda p: p["published"]):
        dest = OUT / "posts" / f"{post['slug']}.md"
        if post["id"] in held or dest.exists():
            continue
        dest.write_text(convert(post, slugs_by_id, fetch=fetch), encoding="utf-8")
        written.append(dest)
    return written


def _fetch_feed(url: str = FEED_URL) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read().decode("utf-8")


def _read_export(path: Path) -> dict[str, str]:
    """``{filename: html}`` for every published post in an export zip or dir."""
    files: dict[str, str] = {}
    if path.is_dir():
        for f in sorted((path / "posts").glob("*.html")):
            files[f.name] = f.read_text(encoding="utf-8")
    else:
        with zipfile.ZipFile(path) as zf:
            for name in zf.namelist():
                if name.startswith("posts/") and name.endswith(".html"):
                    files[name.split("/", 1)[1]] = zf.read(name).decode("utf-8")
    return {k: v for k, v in files.items() if not k.startswith("draft_")}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument(
        "export", type=Path, nargs="?", help="Medium export zip, or the unzipped folder"
    )
    ap.add_argument(
        "--feed", action="store_true", help="import new OpenCheck posts from the RSS feed"
    )
    ap.add_argument("--id", action="append", default=[], help="also import this Medium id")
    ap.add_argument("--keep-existing", action="store_true")
    ap.add_argument("--no-images", action="store_true", help="do not download images")
    args = ap.parse_args(argv)

    if args.feed:
        written = import_feed(_fetch_feed(), fetch=not args.no_images)
        for path in written:
            print(f"imported {path.name}")
        if not written:
            print("No new OpenCheck posts in the feed.")
        return 0
    if args.export is None:
        ap.error("give an export zip or folder, or --feed")

    posts = []
    for raw in _read_export(args.export).values():
        try:
            post = parse_post(raw)
        except ValueError:
            continue
        if OPENCHECK_TAG in post["tag_slugs"] or post["id"] in args.id:
            posts.append(post)
    if not posts:
        print("No OpenCheck posts found in the export.", file=sys.stderr)
        return 1

    slugs_by_id = {p["id"]: p["slug"] for p in posts if p["id"]}
    (OUT / "posts").mkdir(parents=True, exist_ok=True)
    for post in sorted(posts, key=lambda p: p["published"]):
        dest = OUT / "posts" / f"{post['slug']}.md"
        if args.keep_existing and dest.exists():
            print(f"kept     {dest.name}")
            continue
        text = convert(post, slugs_by_id, fetch=not args.no_images)
        dest.write_text(text, encoding="utf-8")
        day = datetime.fromisoformat(post["published"].replace("Z", "+00:00")).date()
        digest = hashlib.sha256(text.encode()).hexdigest()[:8]
        print(f"imported {day} {dest.name} ({digest})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
