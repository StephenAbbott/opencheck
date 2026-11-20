# ruff: noqa: E501 — the stylesheet and the page templates are long lines of CSS and HTML.
"""The OpenCheck blog: Markdown posts in ``data/blog/posts``, rendered here.

Posts start life on Medium and are imported by ``scripts/import_medium.py``
(one ``<slug>.md`` per post, YAML frontmatter, images in
``data/blog/media/<slug>/``). This module reads them, renders the Markdown to
HTML with markdown-it-py (CommonMark; raw HTML in a post is escaped, never
passed through) and draws the pages in the "Ledger" look Stephen chose on
20 Nov 2026: a dated timeline for the index, and a post page whose
publication date is stated twice — in words, and as ``publicationDate``.

opencheck.world is the original copy (Stephen, 20 Nov 2026): every post's
``rel=canonical`` names its own ``/blog/<slug>`` URL, and the Medium story is
credited as where it first appeared. Medium's own canonical link is changed
by hand in Medium's story settings.

Nothing here calls a source or the network; pages are a read of local files,
cached while the files do not change.
"""

from __future__ import annotations

import hashlib
import html
import json
import math
import re
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from email.utils import format_datetime
from functools import lru_cache
from pathlib import Path

import yaml  # type: ignore[import-untyped]
from markdown_it import MarkdownIt

from .cache import data_root
from .entity_pages import FONTS_STYLESHEET, _goatcounter_snippet

#: Bump when the rendered HTML changes materially (part of every ETag).
TEMPLATE_VERSION = "3"

AUTHOR = "Stephen Abbott Pugh"
AUTHOR_URL = "https://www.beneficialownership.co.uk/"
BLOG_TITLE = "OpenCheck blog"
BLOG_DESCRIPTION = (
    "Notes on building OpenCheck: open registers, the Beneficial Ownership "
    "Data Standard, and what the data can and cannot tell you."
)
#: Words a minute for the reading-time estimate.
_WORDS_PER_MINUTE = 230

_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_MEDIA_RE = re.compile(r"^[0-9]{2}\.(?:webp|png|jpe?g|gif)$")


def blog_dir() -> Path:
    return data_root() / "blog"


def posts_dir() -> Path:
    return blog_dir() / "posts"


def media_path(slug: str, name: str) -> Path | None:
    """The file behind ``/blog/media/<slug>/<name>``, or None when the name is
    not one the importer writes (so no path outside the folder is reachable)."""
    if not _SLUG_RE.match(slug) or not _MEDIA_RE.match(name):
        return None
    path = blog_dir() / "media" / slug / name
    return path if path.is_file() else None


def topic_slug(topic: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", topic.lower()).strip("-")


def long_date(day: date) -> str:
    """``2 November 2026`` — the house date form."""
    return f"{day.day} {day.strftime('%B %Y')}"


@dataclass(frozen=True)
class Heading:
    id: str
    text: str


@dataclass(frozen=True)
class Post:
    slug: str
    title: str
    subtitle: str
    published: datetime
    medium_url: str
    topics: tuple[str, ...]
    image: str
    image_alt: str
    body_html: str
    headings: tuple[Heading, ...]
    words: int
    updated: datetime | None = None
    source_digest: str = field(default="", compare=False)

    @property
    def day(self) -> date:
        return self.published.date()

    @property
    def path(self) -> str:
        return f"/blog/{self.slug}"

    @property
    def reading_minutes(self) -> int:
        return max(1, math.ceil(self.words / _WORDS_PER_MINUTE))

    @property
    def description(self) -> str:
        return self.subtitle or _first_sentence(self.body_html)


# ---------------------------------------------------------------------------
# Markdown
# ---------------------------------------------------------------------------

_MD = MarkdownIt("commonmark", {"html": False, "linkify": False, "typographer": False})


def _heading_id(text: str, seen: set[str]) -> str:
    base = topic_slug(text) or "section"
    candidate, n = base, 2
    while candidate in seen:
        candidate, n = f"{base}-{n}", n + 1
    seen.add(candidate)
    return candidate


def _figure(match: re.Match[str]) -> str:
    attrs = match.group(1)
    src = re.search(r'src="([^"]*)"', attrs)
    alt = re.search(r'alt="([^"]*)"', attrs)
    title = re.search(r'title="([^"]*)"', attrs)
    src_v = src.group(1) if src else ""
    alt_v = alt.group(1) if alt else ""
    size = _image_size(src_v)
    dims = f' width="{size[0]}" height="{size[1]}"' if size else ""
    caption = f"<figcaption>{title.group(1)}</figcaption>" if title else ""
    return (
        f'<figure><img src="{src_v}" alt="{alt_v}"{dims} loading="lazy" '
        f'decoding="async">{caption}</figure>'
    )


def render_markdown(text: str) -> tuple[str, tuple[Heading, ...]]:
    """Markdown → HTML, with ids on the ``h2`` headings (the Contents list)
    and a lone image paragraph drawn as a figure with its caption."""
    tokens = _MD.parse(text)
    seen: set[str] = set()
    headings: list[Heading] = []
    for i, token in enumerate(tokens):
        if token.type == "heading_open" and token.tag in ("h2", "h3"):
            inline = tokens[i + 1]
            label = "".join(
                child.content
                for child in (inline.children or [])
                if child.type in ("text", "code_inline")
            )
            hid = _heading_id(label, seen)
            token.attrSet("id", hid)
            if token.tag == "h2":
                headings.append(Heading(hid, label))
    body = _MD.renderer.render(tokens, _MD.options, {})
    body = re.sub(r"<p><img ([^>]*?) ?/?></p>", _figure, body)
    return body, tuple(headings)


def _first_sentence(body_html: str) -> str:
    text = re.sub(r"<[^>]+>", "", body_html.split("</p>", 1)[0])
    text = html.unescape(text).strip()
    if len(text) <= 200:
        return text
    cut = text[:200]
    end = max(cut.rfind(". "), cut.rfind("? "))
    return cut[: end + 1] if end > 60 else cut.rsplit(" ", 1)[0] + "…"


def _image_size(src: str) -> tuple[int, int] | None:
    """Width and height of a local ``/blog/media/…`` image, for layout."""
    parts = src.split("/")
    if len(parts) != 5 or parts[1:3] != ["blog", "media"]:
        return None
    path = media_path(parts[3], parts[4])
    if path is None:
        return None
    try:
        from PIL import Image

        with Image.open(path) as image:
            return image.size
    except Exception:  # noqa: BLE001 — a size is a nicety, never a failure
        return None


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, str) and value.strip():
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def parse_post(slug: str, text: str) -> Post:
    """One Markdown file → a Post. Raises ValueError on a malformed file."""
    if not text.startswith("---\n"):
        raise ValueError(f"{slug}: no frontmatter")
    _, front, body = text.split("---\n", 2)
    meta = yaml.safe_load(front) or {}
    title = str(meta.get("title") or "").strip()
    published = _parse_datetime(meta.get("published"))
    if not title or published is None:
        raise ValueError(f"{slug}: a post needs a title and a published date")
    body_html, headings = render_markdown(body)
    words = len(re.findall(r"\w+", re.sub(r"<[^>]+>", " ", body_html)))

    return Post(
        slug=slug,
        title=title,
        subtitle=str(meta.get("subtitle") or "").strip(),
        published=published.astimezone(UTC),
        medium_url=str(meta.get("medium_url") or ""),
        topics=tuple(str(t) for t in (meta.get("topics") or [])),
        image=str(meta.get("image") or ""),
        image_alt=str(meta.get("image_alt") or ""),
        body_html=body_html,
        headings=headings,
        words=words,
        updated=_parse_datetime(meta.get("updated")),
        source_digest=hashlib.sha256(text.encode()).hexdigest()[:16],
    )


def _signature() -> tuple[tuple[str, int, int], ...]:
    folder = posts_dir()
    if not folder.is_dir():
        return ()
    return tuple(
        (f.name, f.stat().st_mtime_ns, f.stat().st_size) for f in sorted(folder.glob("*.md"))
    )


@lru_cache(maxsize=4)
def _load(folder: str, signature: tuple[tuple[str, int, int], ...]) -> tuple[Post, ...]:
    posts = []
    for name, _, _ in signature:
        slug = name[: -len(".md")]
        if not _SLUG_RE.match(slug):
            continue
        posts.append(parse_post(slug, (Path(folder) / name).read_text(encoding="utf-8")))
    return tuple(sorted(posts, key=lambda p: p.published, reverse=True))


def all_posts() -> tuple[Post, ...]:
    """Every post, newest first. Re-read when a file changes."""
    return _load(str(posts_dir()), _signature())


def get_post(slug: str) -> Post | None:
    return next((p for p in all_posts() if p.slug == slug), None)


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

_LOGO = (
    '<svg viewBox="-11 -11 98 94" width="26" height="26" aria-hidden="true">'
    '<path d="M38 0 L0 72 M38 0 L76 72 M0 72 L76 72" stroke="#93c5fd" stroke-width="5"/>'
    '<circle cx="38" cy="0" r="11" fill="#22c55e"/>'
    '<circle cx="0" cy="72" r="11" fill="#3b82f6"/>'
    '<circle cx="76" cy="72" r="11" fill="#7c3aed"/></svg>'
)

#: Colours are the design system's tokens (frontend/tailwind.config.js):
#: oo.navy, oo.blue, oo.soft / oo.softBorder, oo.ink, oo.muted, oo.mark.line.
_CSS = """
:root{--navy:#191d23;--blue:#3d30d4;--soft:#eef1fb;--soft-border:#cfd6f5;
--ink:#191d23;--body:#2a2f37;--muted:#5b6170;--rule:#e3e4ea;--bg:#f3f3f5;
--line:#93c5fd}
*{box-sizing:border-box}html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--bg);color:var(--ink);
font:16px/1.55 "DM Sans",system-ui,"Segoe UI",sans-serif}
a{color:var(--blue)}img{max-width:100%;height:auto;display:block}
.skip{position:absolute;left:-999px;top:0;background:#fff;padding:8px 12px;z-index:9}
.skip:focus{left:8px;top:8px}
header.site{background:var(--navy);color:#fff;padding:12px 16px}
header.site .in{max-width:1100px;margin:0 auto;display:flex;justify-content:space-between;
align-items:center;gap:4px 16px;flex-wrap:wrap}
.brand{display:flex;align-items:center;gap:9px;color:#fff;text-decoration:none;
font:700 20px Bitter,Georgia,serif}.brand b{color:var(--line);font-weight:700}
header.site nav{display:flex;gap:18px;font-size:14px;flex-wrap:wrap}
header.site nav a{color:rgba(255,255,255,.8);text-decoration:none;display:inline-flex;
align-items:center;min-height:44px}
header.site nav a:hover{color:#fff}
header.site nav a[aria-current]{color:#fff;font-weight:500;box-shadow:inset 0 -2px var(--line)}
.wrap{max-width:1100px;margin:0 auto;padding:36px 16px 8px}
.head{display:flex;justify-content:space-between;align-items:end;gap:16px;flex-wrap:wrap;
padding-bottom:18px;border-bottom:1px solid #dcdde3;margin-bottom:8px}
.head h1{font:700 30px/1.2 Bitter,Georgia,serif;margin:0}
.head p{margin:6px 0 0;color:var(--muted);font-size:15px;max-width:560px;line-height:1.55}
.month{display:grid;grid-template-columns:150px 1fr;gap:0 28px;margin-top:22px}
.month h2{font:700 15px Bitter,Georgia,serif;margin:14px 0 0;position:sticky;top:12px;align-self:start}
.month h2 small{display:block;font:400 12px "DM Sans",sans-serif;color:var(--muted);margin-top:2px}
.rail{border-left:2px solid var(--soft-border);padding-left:24px;list-style:none;margin:0}
.card{position:relative;background:#fff;border:1px solid var(--rule);border-radius:10px;
padding:16px 18px;margin:12px 0;box-shadow:0 1px 2px rgba(61,48,212,.05)}
.card::before{content:"";position:absolute;left:-32px;top:22px;width:12px;height:12px;
border-radius:50%;background:#fff;border:3px solid var(--blue)}
.stamp{font:12px "DM Mono",ui-monospace,monospace;color:var(--blue);margin-bottom:6px}
.stamp span{color:var(--muted)}
.card h3{font:700 19px/1.3 Bitter,Georgia,serif;margin:0 0 6px}
.card h3 a{color:var(--ink);text-decoration:none}.card h3 a:hover{color:var(--blue);text-decoration:underline}
.card p{margin:0 0 10px;font-size:14px;line-height:1.6;color:var(--muted)}
.card.has-thumb{display:grid;grid-template-columns:minmax(0,1fr) 240px;gap:20px;align-items:start}
.thumb{display:block}.thumb img{width:100%;aspect-ratio:40/21;object-fit:cover;object-position:left top;
border-radius:8px;border:1px solid var(--rule)}
.row{display:flex;gap:6px;flex-wrap:wrap;align-items:center;font-size:12px;color:var(--muted)}
.empty{padding:28px 0;color:var(--muted)}
.post{max-width:1100px;margin:0 auto;padding:30px 16px 8px;display:grid;
grid-template-columns:260px minmax(0,1fr);gap:44px}
.post aside{position:sticky;top:16px;align-self:start;background:#fff;border:1px solid var(--rule);
border-radius:10px;padding:18px;font-size:13px}
.post aside h2{font:600 11px "DM Sans",sans-serif;letter-spacing:.14em;text-transform:uppercase;
color:var(--blue);margin:0 0 10px}
.post dl{margin:0;display:grid;grid-template-columns:auto 1fr;gap:8px 12px}
.post dt{color:var(--muted)}.post dd{margin:0;font-weight:500}
.toc{margin:16px 0 0;padding:14px 0 0;border-top:1px solid var(--rule)}
.toc ol{margin:0;padding-left:18px;line-height:1.7}.toc a{color:var(--ink);text-decoration:none}
.toc a:hover{color:var(--blue);text-decoration:underline}
.eyebrow{font:600 12px "DM Sans",sans-serif;letter-spacing:.14em;text-transform:uppercase}
.eyebrow a{text-decoration:none}
.post article h1{font:700 38px/1.15 Bitter,Georgia,serif;margin:6px 0 12px}
.dek{font-size:19px;line-height:1.5;color:var(--muted);margin:0 0 18px}
.pub{display:inline-flex;align-items:center;flex-wrap:wrap;gap:4px 10px;background:var(--soft);
border:1px solid var(--soft-border);border-radius:10px;padding:8px 12px;font-size:14px;margin-bottom:26px}
.pub time{font:700 15px Bitter,Georgia,serif}.pub code{font:12px "DM Mono",ui-monospace,monospace;color:var(--blue)}
.hero{margin:0 0 28px}.hero img{border-radius:10px;border:1px solid var(--rule)}
.prose{max-width:690px}
.prose h2{font:700 22px/1.3 Bitter,Georgia,serif;margin:2em 0 .5em;scroll-margin-top:16px}
.prose h3{font:700 18px/1.35 Bitter,Georgia,serif;margin:1.6em 0 .4em}
.prose p,.prose li{font-size:17px;line-height:1.72;color:var(--body)}.prose p{margin:0 0 1.15em}
.prose ul,.prose ol{padding-left:1.2em;margin:0 0 1.2em}.prose li{margin:.3em 0}
.prose a{text-underline-offset:2px}
.prose code{font:15px "DM Mono",ui-monospace,monospace;background:var(--soft);padding:1px 5px;border-radius:4px}
.prose blockquote{margin:1.6em 0;padding:4px 0 4px 18px;border-left:3px solid var(--soft-border)}
.prose blockquote p{font:600 19px/1.5 Bitter,Georgia,serif;color:var(--ink)}
.prose figure{margin:1.6em 0}.prose figure img{border-radius:10px;border:1px solid var(--rule)}
.prose figcaption{font-size:13px;color:var(--muted);margin-top:8px}
.endnote{max-width:690px;margin:36px 0 0;padding:20px 22px;background:#fff;border:1px solid var(--rule);
border-radius:10px;font-size:14px;line-height:1.6;color:var(--muted)}.endnote b{color:var(--ink)}
footer.site{background:var(--navy);color:rgba(255,255,255,.85);font-size:13px;padding:18px 16px;margin-top:48px}
footer.site div{max-width:1100px;margin:0 auto;display:flex;gap:6px 18px;flex-wrap:wrap}
footer.site a{color:rgba(255,255,255,.85)}
@media(max-width:860px){.post{grid-template-columns:1fr;gap:18px}.post aside{position:static;order:2}
.month{grid-template-columns:1fr}.month h2{position:static}.post article h1{font-size:28px}.dek{font-size:17px}}
@media(max-width:640px){.card.has-thumb{grid-template-columns:1fr}.thumb{order:-1}.prose p,.prose li{font-size:16px}.head h1{font-size:26px}}
"""


def _head(
    *,
    title: str,
    description: str,
    canonical: str,
    frontend: str,
    og_type: str = "website",
    image: str = "",
    extra: str = "",
    noindex: bool = False,
    analytics_path: str = "/blog",
) -> str:
    e = html.escape
    og_image = image or f"{frontend}/og-image.png"
    robots = '<meta name="robots" content="noindex,follow">' if noindex else ""
    return f"""<!doctype html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{e(title)}</title>
<meta name="description" content="{e(description)}">
<link rel="canonical" href="{e(canonical)}">
<link rel="alternate" type="application/rss+xml" title="{e(BLOG_TITLE)}" href="{e(frontend)}/blog/feed.xml">
<meta name="theme-color" content="#191d23">
<meta name="author" content="{e(AUTHOR)}">
<meta property="og:type" content="{og_type}">
<meta property="og:site_name" content="OpenCheck">
<meta property="og:locale" content="en_GB">
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(description)}">
<meta property="og:url" content="{e(canonical)}">
<meta property="og:image" content="{e(og_image)}">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{e(title)}">
<meta name="twitter:description" content="{e(description)}">
<meta name="twitter:image" content="{e(og_image)}">
{robots}{extra}
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="stylesheet" href="{FONTS_STYLESHEET}">
<style>{_CSS}</style>
{_goatcounter_snippet(analytics_path)}
</head>"""


def _header(frontend: str) -> str:
    f = html.escape(frontend)
    return (
        '<a class="skip" href="#main">Skip to content</a>'
        f'<header class="site"><div class="in"><a class="brand" href="{f}/">{_LOGO}'
        "<span>Open<b>Check</b></span></a>"
        '<nav aria-label="Site navigation">'
        f'<a href="{f}/sources">Sources</a><a href="{f}/api">API</a>'
        f'<a href="{f}/features">Features</a>'
        '<a href="/blog" aria-current="page">Blog</a></nav></div></header>'
    )


def _footer(frontend: str) -> str:
    f = html.escape(frontend)
    return (
        '<footer class="site"><div>'
        f'<a href="{f}/">OpenCheck</a>'
        '<a href="/blog/feed.xml">RSS feed</a>'
        f'<a href="{f}/about">About OpenCheck</a>'
        '<a href="https://github.com/StephenAbbott/opencheck">Source code (MIT)</a>'
        "<span>Posts by "
        f'<a href="{AUTHOR_URL}">{AUTHOR}</a></span>'
        "</div></footer>"
    )


def _absolute(url: str, frontend: str) -> str:
    return f"{frontend}{url}" if url.startswith("/") else url


def _thumb(post: Post) -> str:
    """The post's lead image beside its card on /blog. Decorative: the title
    beside it is the link, so the image is hidden from screen readers and
    taken out of the tab order."""
    if not post.image:
        return ""
    size = _image_size(post.image)
    dims = f' width="{size[0]}" height="{size[1]}"' if size else ""
    return (
        f'<a class="thumb" href="{post.path}" tabindex="-1" aria-hidden="true">'
        f'<img src="{html.escape(post.image)}" alt=""{dims} loading="lazy" decoding="async"></a>'
    )


def render_index(posts: tuple[Post, ...], *, frontend: str) -> str:
    """The /blog page: posts grouped by month on a dated rail."""
    e = html.escape
    months: dict[str, list[Post]] = {}
    for post in posts:
        months.setdefault(post.day.strftime("%Y-%m"), []).append(post)
    sections = []
    for items in months.values():
        heading = items[0].day.strftime("%B %Y")
        count = f"{len(items)} post" + ("" if len(items) == 1 else "s")
        cards = "".join(
            f'<li class="card{" has-thumb" if p.image else ""}"><div class="text">'
            f'<div class="stamp">publicationDate <span>=</span> '
            f'<time datetime="{p.published.isoformat()}">{p.day.isoformat()}</time></div>'
            f'<h3><a href="{p.path}">{e(p.title)}</a></h3>'
            f"<p>{e(p.description)}</p>"
            f'<div class="row"><span>{p.reading_minutes} min read</span></div></div>'
            f"{_thumb(p)}</li>"
            for p in items
        )
        sections.append(
            f'<section class="month" aria-label="{heading}"><h2>{heading}<small>{count}</small></h2>'
            f'<ol class="rail">{cards}</ol></section>'
        )
    body = "".join(sections) or '<p class="empty">No posts yet.</p>'

    head = _head(
        title=f"{BLOG_TITLE} - OpenCheck",
        description=BLOG_DESCRIPTION,
        canonical=f"{frontend}/blog",
        frontend=frontend,
    )
    intro = (
        "Each post is dated the way OpenCheck dates a statement: when it was "
        "published, where it first appeared, and what it is about."
    )
    return f"""{head}
<body>
{_header(frontend)}
<main id="main" class="wrap">
<div class="head"><div><h1>{BLOG_TITLE}</h1><p>{intro}</p></div></div>
{body}
</main>
{_footer(frontend)}
</body>
</html>"""


def json_ld(post: Post, frontend: str) -> str:
    data = {
        "@context": "https://schema.org",
        "@type": "BlogPosting",
        "headline": post.title,
        "description": post.description,
        "datePublished": post.published.isoformat(),
        "dateModified": (post.updated or post.published).isoformat(),
        "author": {"@type": "Person", "name": AUTHOR, "url": AUTHOR_URL},
        "publisher": {"@type": "Organization", "name": "OpenCheck", "url": f"{frontend}/"},
        "mainEntityOfPage": f"{frontend}{post.path}",
        "url": f"{frontend}{post.path}",
        "inLanguage": "en-GB",
        "keywords": list(post.topics),
        "wordCount": post.words,
    }
    if post.image:
        data["image"] = _absolute(post.image, frontend)
    if post.medium_url:
        data["sameAs"] = post.medium_url
    # "</" cannot appear inside the script element.
    return json.dumps(data, ensure_ascii=False).replace("</", "<\\/")


def render_post(post: Post, *, frontend: str) -> str:
    e = html.escape
    published_iso = post.published.isoformat()
    meta = (
        f'<meta property="article:published_time" content="{published_iso}">'
        + (
            f'<meta property="article:modified_time" content="{post.updated.isoformat()}">'
            if post.updated
            else ""
        )
        + f'<meta property="article:author" content="{e(AUTHOR)}">'
        + "".join(f'<meta property="article:tag" content="{e(t)}">' for t in post.topics)
        + f'<script type="application/ld+json">{json_ld(post, frontend)}</script>'
    )
    head = _head(
        title=f"{post.title} - OpenCheck",
        description=post.description,
        canonical=f"{frontend}{post.path}",
        frontend=frontend,
        og_type="article",
        image=_absolute(post.image, frontend) if post.image else "",
        extra=meta,
        analytics_path=post.path,
    )
    medium = (
        f'<dt>First on</dt><dd><a href="{e(post.medium_url)}">Medium</a></dd>'
        if post.medium_url
        else ""
    )
    updated = (
        f'<dt>Updated</dt><dd><time datetime="{post.updated.isoformat()}">'
        f"{long_date(post.updated.date())}</time></dd>"
        if post.updated
        else ""
    )
    toc = (
        '<nav class="toc" aria-label="Contents"><h2>Contents</h2><ol>'
        + "".join(f'<li><a href="#{h.id}">{e(h.text)}</a></li>' for h in post.headings)
        + "</ol></nav>"
        if post.headings
        else ""
    )
    hero = ""
    if post.image:
        size = _image_size(post.image)
        dims = f' width="{size[0]}" height="{size[1]}"' if size else ""
        hero = (
            f'<figure class="hero"><img src="{e(post.image)}" alt="{e(post.image_alt)}"{dims}'
            ' fetchpriority="high"></figure>'
        )
    dek = f'<p class="dek">{e(post.subtitle)}</p>' if post.subtitle else ""
    medium_note = (
        f' It first appeared on <a href="{e(post.medium_url)}">Medium</a>.'
        if post.medium_url
        else ""
    )
    return f"""{head}
<body>
{_header(frontend)}
<div class="post">
<aside aria-label="About this post"><h2>About this post</h2><dl>
<dt>Published</dt><dd><time datetime="{published_iso}">{long_date(post.day)}</time></dd>
{updated}<dt>Reading</dt><dd>{post.reading_minutes} minutes</dd>
{medium}<dt>Author</dt><dd>{e(AUTHOR)}</dd></dl>{toc}</aside>
<main id="main"><article>
<div class="eyebrow"><a href="/blog">← All posts</a></div>
<h1>{e(post.title)}</h1>{dek}
<p class="pub">Published <time datetime="{published_iso}">{long_date(post.day)}</time>
<code>publicationDate {post.day.isoformat()}</code></p>
{hero}<div class="prose">{post.body_html}</div>
<p class="endnote"><b>About OpenCheck.</b> OpenCheck shows who owns and controls a
company, from open registers and open standards, anchored on its Legal Entity
Identifier. <a href="{e(frontend)}/">Run a check</a> · <a href="/blog">More posts</a>.{medium_note}</p>
</article></main>
</div>
{_footer(frontend)}
</body>
</html>"""


def render_not_found(frontend: str) -> str:
    head = _head(
        title=f"Post not found - {BLOG_TITLE}",
        description=BLOG_DESCRIPTION,
        canonical=f"{frontend}/blog",
        frontend=frontend,
        noindex=True,
    )
    return f"""{head}
<body>
{_header(frontend)}
<main id="main" class="wrap"><h1 style="font:700 30px Bitter,Georgia,serif">Post not found</h1>
<p>There is no post at this address. <a href="/blog">See every post</a>.</p></main>
{_footer(frontend)}
</body>
</html>"""


# ---------------------------------------------------------------------------
# Feed and sitemap
# ---------------------------------------------------------------------------


def _absolute_html(body: str, frontend: str) -> str:
    return re.sub(r'(src|href)="/', lambda m: f'{m.group(1)}="{frontend}/', body)


def _cdata(text: str) -> str:
    return "<![CDATA[" + text.replace("]]>", "]]]]><![CDATA[>") + "]]>"


def render_feed(posts: tuple[Post, ...], *, frontend: str) -> str:
    """RSS 2.0 with each post's full text, newest first."""
    e = html.escape
    last = posts[0].published if posts else datetime(2026, 1, 1, tzinfo=UTC)
    items = []
    for post in posts:
        url = f"{frontend}{post.path}"
        hero = (
            f'<p><img src="{_absolute(post.image, frontend)}" alt="{e(post.image_alt)}"></p>'
            if post.image
            else ""
        )
        content = hero + _absolute_html(post.body_html, frontend)
        items.append(
            "<item>"
            f"<title>{e(post.title)}</title>"
            f"<link>{url}</link>"
            f'<guid isPermaLink="true">{url}</guid>'
            f"<pubDate>{format_datetime(post.published)}</pubDate>"
            f"<dc:creator>{e(AUTHOR)}</dc:creator>"
            + "".join(f"<category>{e(t)}</category>" for t in post.topics)
            + f"<description>{e(post.description)}</description>"
            f"<content:encoded>{_cdata(content)}</content:encoded>"
            "</item>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:atom="http://www.w3.org/2005/Atom">'
        "<channel>"
        f"<title>{BLOG_TITLE}</title>"
        f"<link>{frontend}/blog</link>"
        f'<atom:link href="{frontend}/blog/feed.xml" rel="self" type="application/rss+xml"/>'
        f"<description>{e(BLOG_DESCRIPTION)}</description>"
        "<language>en-gb</language>"
        f"<lastBuildDate>{format_datetime(last)}</lastBuildDate>"
        + "".join(items)
        + "</channel></rss>"
    )


def render_sitemap(posts: tuple[Post, ...], *, frontend: str) -> str:
    def lastmod(p: Post) -> str:
        return (p.updated or p.published).date().isoformat()

    newest = max((lastmod(p) for p in posts), default="")
    urls = [
        f"<url><loc>{frontend}/blog</loc>"
        + (f"<lastmod>{newest}</lastmod>" if newest else "")
        + "</url>"
    ]
    urls += [
        f"<url><loc>{frontend}{p.path}</loc><lastmod>{lastmod(p)}</lastmod></url>" for p in posts
    ]
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + "".join(urls) + "</urlset>"
    )
