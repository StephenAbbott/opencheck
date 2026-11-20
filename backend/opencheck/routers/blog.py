"""Routes for the blog: /blog, /blog/<slug>, its feed, sitemap and images.

Server-rendered like the entity pages (``routers/entity_pages.py``), and for
the same reason: a search engine and a feed reader should get the post, not
an empty SPA shell. No rate limit (each page is a read of a cached local
file); ``Cache-Control`` of an hour, because a post can be corrected.
"""

from __future__ import annotations

import hashlib

from fastapi import APIRouter, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse

from .. import blog
from ..config import get_settings

router = APIRouter(tags=["blog"])

_CACHE = "public, max-age=3600, stale-while-revalidate=86400"
_MEDIA_CACHE = "public, max-age=604800"


def _frontend() -> str:
    frontend = (get_settings().frontend_origin or "").rstrip("/")
    return frontend if frontend.startswith("http") else "https://opencheck.world"


def _etag(*parts: str) -> str:
    digest = hashlib.sha256("|".join((blog.TEMPLATE_VERSION, *parts)).encode()).hexdigest()[:20]
    return f'W/"{digest}"'


def _answer(request: Request, body: str, etag: str, media_type: str = "text/html") -> Response:
    headers = {"ETag": etag, "Cache-Control": _CACHE}
    inm = request.headers.get("if-none-match")
    if inm is not None and etag in inm:
        return Response(status_code=304, headers=headers)
    return Response(content=body, media_type=media_type, headers=headers)


def _posts_tag(posts: tuple[blog.Post, ...]) -> str:
    return ",".join(p.source_digest for p in posts)


@router.api_route("/blog", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def blog_index(request: Request, topic: str | None = None) -> Response:
    posts = blog.all_posts()
    frontend = _frontend()
    topic = blog.topic_slug(topic) if topic else None
    page = blog.render_index(posts, frontend=frontend, topic=topic or None)
    return _answer(request, page, _etag("index", topic or "", frontend, _posts_tag(posts)))


@router.api_route("/blog/", methods=["GET", "HEAD"], include_in_schema=False)
async def blog_index_slash() -> RedirectResponse:
    return RedirectResponse("/blog", status_code=301)


@router.api_route("/blog/feed.xml", methods=["GET", "HEAD"])
async def blog_feed(request: Request) -> Response:
    posts = blog.all_posts()
    frontend = _frontend()
    xml = blog.render_feed(posts, frontend=frontend)
    return _answer(request, xml, _etag("feed", frontend, _posts_tag(posts)), "application/rss+xml")


@router.api_route("/blog/sitemap.xml", methods=["GET", "HEAD"])
async def blog_sitemap(request: Request) -> Response:
    posts = blog.all_posts()
    frontend = _frontend()
    xml = blog.render_sitemap(posts, frontend=frontend)
    return _answer(request, xml, _etag("sitemap", frontend, _posts_tag(posts)), "application/xml")


@router.api_route("/blog/media/{slug}/{name}", methods=["GET", "HEAD"])
async def blog_media(slug: str, name: str) -> Response:
    path = blog.media_path(slug, name)
    if path is None:
        return Response(status_code=404, headers={"Cache-Control": "public, max-age=3600"})
    return FileResponse(path, headers={"Cache-Control": _MEDIA_CACHE})


@router.api_route("/blog/{slug}", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def blog_post(request: Request, slug: str) -> Response:
    frontend = _frontend()
    canonical = slug.lower().strip("/")
    post = blog.get_post(canonical)
    if post is None:
        return HTMLResponse(
            blog.render_not_found(frontend),
            status_code=404,
            headers={"Cache-Control": "public, max-age=600"},
        )
    if slug != canonical:
        return RedirectResponse(f"/blog/{canonical}", status_code=301)
    page = blog.render_post(post, frontend=frontend)
    return _answer(request, page, _etag("post", slug, frontend, post.source_digest))
