"""Cache headers for the built frontend and for file downloads
(the "Speed up first load" ticket, 1 Nov 2026)."""

from __future__ import annotations

import pytest
from starlette.applications import Starlette
from starlette.responses import Response
from starlette.routing import Route
from starlette.testclient import TestClient

from opencheck.app import app as opencheck_app
from opencheck.static_cache import (
    DOWNLOAD,
    IMMUTABLE,
    NEVER_KEEP,
    UNHASHED,
    DownloadNoStoreMiddleware,
    spa_cache_control,
)


@pytest.mark.parametrize(
    "path",
    ["assets/index-C34oXtVw.js", "assets/index-BoLehO8Y.css", "/assets/dm-sans-latin-wght-normal-Xz1IZZA0.woff2"],
)
def test_hashed_build_output_is_kept_for_a_year(path: str) -> None:
    assert spa_cache_control(path) == IMMUTABLE


@pytest.mark.parametrize(
    "path", ["", "index.html", "version.json", "robots.txt", "sitemap.xml", "changelog.html"]
)
def test_what_names_the_current_deploy_is_never_kept(path: str) -> None:
    # index.html names the entry chunk and version.json is how an open page
    # learns of a deploy (Phase 331): either kept stale breaks that.
    assert spa_cache_control(path) == NEVER_KEEP


@pytest.mark.parametrize(
    "path",
    [
        "godin-logo.png",
        "new-features/watchlist.svg",
        "bods-dagre-images/flags/gb.svg",
        "favicon.svg",
        "og-image.png",
    ],
)
def test_unhashed_static_files_are_kept_an_hour(path: str) -> None:
    assert spa_cache_control(path) == UNHASHED
    assert "max-age=3600" in UNHASHED and "no-cache" not in UNHASHED


def _client() -> TestClient:
    def attachment(_request):
        return Response(b"x", headers={"Content-Disposition": 'attachment; filename="a.zip"'})

    def attachment_with_own_header(_request):
        return Response(
            b"x",
            headers={"Content-Disposition": "attachment", "Cache-Control": "private, max-age=60"},
        )

    def inline(_request):
        return Response(b"{}", media_type="application/json")

    app = Starlette(
        routes=[
            Route("/download", attachment),
            Route("/own", attachment_with_own_header),
            Route("/json", inline),
        ]
    )
    app.add_middleware(DownloadNoStoreMiddleware)
    return TestClient(app)


def test_a_download_without_its_own_header_is_never_stored() -> None:
    assert _client().get("/download").headers["cache-control"] == DOWNLOAD


def test_a_download_that_sets_its_own_header_keeps_it() -> None:
    assert _client().get("/own").headers["cache-control"] == "private, max-age=60"


def test_an_ordinary_response_is_left_alone() -> None:
    assert "cache-control" not in _client().get("/json").headers


def test_the_app_installs_the_download_middleware() -> None:
    assert any(m.cls is DownloadNoStoreMiddleware for m in opencheck_app.user_middleware)
