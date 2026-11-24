"""The SPA's pages as crawlers read them, the site sitemap, llms.txt and the
robots.txt changes that go with them (opencheck/site_pages.py)."""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from opencheck import site_pages
from opencheck.app import app

REPO = Path(__file__).resolve().parents[2]
INDEX_HTML = (REPO / "frontend" / "index.html").read_text(encoding="utf-8")
VIEWS_TS = (REPO / "frontend" / "src" / "lib" / "views.ts").read_text(encoding="utf-8")
SITE = "https://opencheck.world"


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _head(page: str, pattern: str) -> list[str]:
    return re.findall(pattern, page)


@pytest.mark.parametrize("page", site_pages.PAGES, ids=lambda p: p.path)
def test_every_page_gets_its_own_head(page: site_pages.SitePage) -> None:
    out = site_pages.render_shell(INDEX_HTML, page.path, SITE)
    assert out is not None
    url = SITE + page.path
    assert f"<title>{page.title}</title>" in out
    assert _head(out, r'<link rel="canonical" href="([^"]*)"') == [url]
    assert _head(out, r'<meta property="og:url" content="([^"]*)"') == [url]
    for tag in ('name="description"', 'property="og:description"', 'name="twitter:description"'):
        assert f'<meta {tag} content="{page.description.replace(chr(39), "&#x27;")}"' in out
    for tag in ('property="og:title"', 'name="twitter:title"'):
        assert f'<meta {tag} content="{page.title}"' in out
    # Nothing of the homepage's head is left behind.
    assert 'href="https://opencheck.world/"' not in out
    # The app itself is untouched.
    assert out.split("</head>")[1] == INDEX_HTML.split("</head>")[1]


def test_every_head_tag_pattern_matches_the_real_shell() -> None:
    """A renamed tag in index.html must fail here, not silently keep the
    homepage's value."""
    for pattern, _ in site_pages._HEAD_TAGS:
        assert re.search(pattern, INDEX_HTML), pattern


def test_the_homepage_and_reports_keep_the_shell_as_built() -> None:
    for path in ("", "/", "/report/abc", "/entity-ish", "/features/extra"):
        assert site_pages.render_shell(INDEX_HTML, path, SITE) is None
    assert site_pages.page_for("features") is site_pages.page_for("/features/")


def test_titles_match_the_frontend() -> None:
    """The server's titles are the ones the app sets in document.title."""
    block = VIEWS_TS.split("export const VIEW_DOCUMENT_TITLES", 1)[1].split("};", 1)[0]
    titles = dict(re.findall(r'(\w+): "([^"]+)"', block))
    paths = dict(re.findall(r'if \(v === "(\w+)"\) return "(/[^"]*)"', VIEWS_TS))
    assert {paths[v]: t for v, t in titles.items()} == {
        p.path: p.title for p in site_pages.PAGES
    }


def test_the_site_sitemap_lists_every_page(client: TestClient) -> None:
    r = client.get("/sitemap.xml")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/xml")
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locs = [e.text for e in ET.fromstring(r.content).findall("s:url/s:loc", ns)]
    assert locs[0] == f"{SITE}/"
    assert set(locs) == {f"{SITE}/", f"{SITE}/blog"} | {SITE + p.path for p in site_pages.PAGES}


def test_llms_txt_names_the_pages_and_the_mcp_server(client: TestClient) -> None:
    r = client.get("/llms.txt")
    assert r.status_code == 200  # not swallowed by the IndexNow /{key}.txt route
    assert r.headers["content-type"].startswith("text/markdown")
    text = r.text
    assert text.startswith("# OpenCheck\n\n> ")
    assert "/.well-known/mcp.json" in text and "/mcp" in text
    for page in site_pages.PAGES:
        assert f"]({SITE}{page.path})" in text
    assert not re.search(r"\b\d{2,}\s+(sources|registers|datasets)\b", text)


def test_robots_lets_link_previews_read_share_pages(client: TestClient) -> None:
    text = client.get("/robots.txt").text
    assert "Disallow: /share" not in text
    assert "Allow: /share/" in text
    assert f"Sitemap: {SITE}/sitemap.xml" in text
    # The live lookup paths stay closed.
    for path in ("/lookup", "/lookup-stream", "/batch-stream", "/saved-reports", "/mcp"):
        assert f"Disallow: {path}\n" in text


def test_share_pages_stay_out_of_search_results(client: TestClient) -> None:
    """Allowing /share/ is for link previews only: the page says noindex."""
    from opencheck.routers import share

    src = Path(share.__file__).read_text(encoding="utf-8")
    assert src.count('<meta name="robots" content="noindex') == 2
