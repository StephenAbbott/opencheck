"""The blog at /blog (opencheck/blog.py, routers/blog.py).

Reads the posts committed under data/blog/ — the suite's data root is seeded
from the tracked files, so these tests see exactly what production serves.
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from datetime import date

import pytest
from fastapi.testclient import TestClient

from opencheck import blog
from opencheck.app import app

SITE = "https://opencheck.world"


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(scope="module")
def posts() -> tuple[blog.Post, ...]:
    found = blog.all_posts()
    assert found, "no posts under data/blog/posts"
    return found


# -- the files ---------------------------------------------------------------


def test_every_post_has_a_title_a_date_and_a_medium_origin(posts) -> None:
    for post in posts:
        assert post.title and post.published.tzinfo is not None
        assert post.medium_url.startswith("https://medium.com/@stephenabbottpugh/")
        assert post.words > 100


def test_posts_are_newest_first(posts) -> None:
    assert list(posts) == sorted(posts, key=lambda p: p.published, reverse=True)


def test_every_image_a_post_names_is_committed(posts) -> None:
    for post in posts:
        srcs = re.findall(r'src="(/blog/media/[^"]+)"', post.body_html)
        srcs += [post.image] if post.image else []
        for src in srcs:
            _, _, _, slug, name = src.split("/")
            assert blog.media_path(slug, name) is not None, src


def test_every_link_to_another_post_resolves(posts) -> None:
    slugs = {p.slug for p in posts}
    for post in posts:
        for target in re.findall(r'href="/blog/([^"#?]+)"', post.body_html):
            assert target in slugs, (post.slug, target)


def test_links_to_imported_posts_point_here_not_at_medium(posts) -> None:
    ids = {p.medium_url.rsplit("-", 1)[-1] for p in posts}
    for post in posts:
        for href in re.findall(r'href="(https://[^"]*medium\.com[^"]*)"', post.body_html):
            assert href.rsplit("-", 1)[-1] not in ids, (post.slug, href)


# -- rendering ----------------------------------------------------------------


def test_raw_html_in_a_post_is_escaped_not_run() -> None:
    post = blog.parse_post(
        "x",
        '---\ntitle: "T"\npublished: "2026-01-02T03:04:05Z"\n---\n\n'
        "Hello <script>alert(1)</script>\n\n## A heading\n",
    )
    assert "<script>" not in post.body_html
    assert "&lt;script&gt;" in post.body_html
    assert post.headings == (blog.Heading("a-heading", "A heading"),)
    assert post.day == date(2026, 1, 2)


def test_a_lone_image_is_a_figure_with_its_caption() -> None:
    body, _ = blog.render_markdown('![Alt text](/x.png "The caption")\n')
    assert body.startswith("<figure><img")
    assert 'alt="Alt text"' in body and "<figcaption>The caption</figcaption>" in body


def test_a_post_without_a_date_is_refused() -> None:
    with pytest.raises(ValueError):
        blog.parse_post("x", '---\ntitle: "T"\n---\n\nBody\n')


# -- pages ------------------------------------------------------------------------


def test_index_lists_every_post_with_its_date(client: TestClient, posts) -> None:
    r = client.get("/blog")
    assert r.status_code == 200
    assert f'<link rel="canonical" href="{SITE}/blog">' in r.text
    assert 'type="application/rss+xml"' in r.text
    for post in posts:
        assert f'href="{post.path}"' in r.text
        assert f">{post.day.isoformat()}</time>" in r.text


def test_post_page_is_the_canonical_copy_and_shows_its_date(client: TestClient, posts) -> None:
    post = posts[0]
    r = client.get(post.path)
    assert r.status_code == 200
    page = r.text
    assert f'<link rel="canonical" href="{SITE}{post.path}">' in page
    assert '<meta property="og:type" content="article">' in page
    assert f'content="{post.published.isoformat()}"' in page
    assert blog.long_date(post.day) in page
    assert f"publicationDate {post.day.isoformat()}" in page
    assert f'href="{post.medium_url}"' in page  # credited, not canonical
    ld = json.loads(re.search(r'application/ld\+json">(.*?)</script>', page).group(1))
    assert ld["@type"] == "BlogPosting"
    assert ld["datePublished"] == post.published.isoformat()
    assert ld["mainEntityOfPage"] == f"{SITE}{post.path}"
    assert page.count("<h1>") == 1


def test_unknown_post_is_a_404_and_a_capitalised_slug_redirects(client: TestClient, posts) -> None:
    assert client.get("/blog/no-such-post").status_code == 404
    post = posts[0]
    r = client.get(f"/blog/{post.slug.upper()}", follow_redirects=False)
    assert r.status_code == 301 and r.headers["location"] == post.path


def test_no_topic_chips_or_filters(client: TestClient, posts) -> None:
    """Topics stay in the frontmatter and the feed, but no page shows them
    (Stephen, 20 Nov 2026): no filter row, no chips, no Topics row."""
    index = client.get("/blog")
    post = client.get(posts[0].path)
    for page in (index.text, post.text):
        assert 'class="chip"' not in page and "?topic=" not in page
    assert "Filter by topic" not in index.text
    assert "<dt>Topics</dt>" not in post.text
    old = client.get("/blog?topic=estonia")
    assert old.status_code == 200 and old.text.count('class="card"') == len(posts)


def test_media_refuses_anything_but_an_importer_file_name(client: TestClient, posts) -> None:
    post = next(p for p in posts if p.image)
    assert client.get(post.image).status_code == 200
    assert client.get("/blog/media/x/..%2F..%2Fpyproject.toml").status_code == 404
    assert client.get(f"/blog/media/{post.slug}/secret.txt").status_code == 404


def test_etag_answers_304(client: TestClient, posts) -> None:
    first = client.get(posts[0].path)
    again = client.get(posts[0].path, headers={"If-None-Match": first.headers["etag"]})
    assert again.status_code == 304


# -- feed, sitemap, robots ----------------------------------------------------


def test_feed_is_rss_with_every_post_in_full(client: TestClient, posts) -> None:
    r = client.get("/blog/feed.xml")
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/rss+xml")
    channel = ET.fromstring(r.content).find("channel")
    items = channel.findall("item")
    assert len(items) == len(posts)
    assert items[0].findtext("guid") == f"{SITE}{posts[0].path}"
    encoded = items[0].findtext("{http://purl.org/rss/1.0/modules/content/}encoded")
    assert 'src="/' not in encoded and 'href="/' not in encoded  # absolute in a feed


def test_sitemap_lists_every_post(client: TestClient, posts) -> None:
    root = ET.fromstring(client.get("/blog/sitemap.xml").content)
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = {loc.text for loc in root.findall("s:url/s:loc", ns)}
    assert f"{SITE}/blog" in locs
    assert {f"{SITE}{p.path}" for p in posts} <= locs


def test_robots_names_the_blog_sitemap(client: TestClient) -> None:
    text = client.get("/robots.txt").text
    assert "Allow: /blog" in text
    assert f"Sitemap: {SITE}/blog/sitemap.xml" in text


def test_the_site_header_links_to_the_blog() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "frontend" / "src"
    assert 'export const BLOG_PATH = "/blog";' in (root / "lib" / "views.ts").read_text()
    chrome = (root / "components" / "SiteChrome.tsx").read_text()
    assert chrome.count("href={BLOG_PATH}") == 2  # header nav and footer
