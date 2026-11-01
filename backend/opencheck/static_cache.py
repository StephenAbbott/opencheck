"""How long a browser and the edge may keep what this app serves without
asking again (the "Speed up first load" ticket, 1 Nov 2026).

Two rules, both read by ``app.py``:

* ``spa_cache_control(path)`` — the header for a file from the built
  frontend. Render's edge cache ("Common static files", switched on 31 Oct
  2026) stores what ``Cache-Control`` lets it, and every deploy purges it.
  Hashed build output under ``assets/`` never changes under its name, so a
  year, immutable. The HTML shell and ``version.json`` must never be kept —
  the shell names the current entry chunk and ``version.json`` is how an
  open page learns a deploy happened (Phase 331) — so ``no-cache``, as are
  ``robots.txt`` and ``sitemap.xml``, which say what the site is today.
  Everything else at the top of the dist (logos, the feature illustrations,
  flags and BOVS icons) is unhashed but changes only with a deploy: an hour,
  then served stale while the browser revalidates for up to a day. Until this
  module those were ``no-cache`` too, so the edge kept none of them.

* ``DownloadNoStoreMiddleware`` — a file download (``Content-Disposition:
  attachment``) that sets no ``Cache-Control`` of its own is ``no-store``.
  ``GET /export`` (16 response shapes) and ``GET /batch-export`` set none,
  and a GET answer with no header is one the edge may hold for two hours
  under Render's defaults. A download is built from a lookup at that moment;
  keeping it would hand the next reader yesterday's BODS. A route that sets
  its own header is left alone.

Pure ASGI, never ``BaseHTTPMiddleware``, which breaks SSE and ``/mcp``
(see ``lookup_budget.ClientScopeMiddleware``).
"""

from __future__ import annotations

from starlette.types import ASGIApp, Message, Receive, Scope, Send

IMMUTABLE = "public, max-age=31536000, immutable"
NEVER_KEEP = "no-cache"
UNHASHED = "public, max-age=3600, stale-while-revalidate=86400"
DOWNLOAD = "no-store"

# Files that describe the current deploy and must be fetched fresh.
_CURRENT_STATE = frozenset({"", "index.html", "version.json", "robots.txt", "sitemap.xml"})


def spa_cache_control(path: str) -> str:
    """The ``Cache-Control`` for ``path``, relative to the frontend dist."""
    path = path.lstrip("/")
    if path.startswith("assets/"):
        return IMMUTABLE
    if path in _CURRENT_STATE or path.endswith((".html", ".htm")):
        return NEVER_KEEP
    return UNHASHED


class DownloadNoStoreMiddleware:
    """Mark a file download that names no ``Cache-Control`` as ``no-store``."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                names = {name.lower() for name, _ in headers}
                disposition = next(
                    (value for name, value in headers if name.lower() == b"content-disposition"),
                    b"",
                )
                if b"cache-control" not in names and disposition.lower().startswith(b"attachment"):
                    headers.append((b"cache-control", DOWNLOAD.encode("latin-1")))
                    message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_header)
