"""FastAPI entry point for OpenCheck.

Surface:

* ``GET /health`` — liveness probe.
* ``GET /sources`` — inventory of registered source adapters with live/stub status.
* ``GET /lookup?lei=<LEI>`` — **primary entry point**. Driven by the
  Legal Entity Identifier: GLEIF first, then dispatch to every other
  source using the LEI (and any cross-references GLEIF carries).
* ``GET /search?q=<query>&kind=<entity|person>`` — free-text fan-out
  search. Kept as a power-user / debugging endpoint; the LEI-driven
  flow is the supported UX.
* ``GET /stream?q=<query>&kind=<entity|person>`` — same fan-out, streamed as SSE.
* ``GET /deepen?source=<id>&hit_id=<id>`` — "Go deeper" on a specific hit.
* ``GET /report?q=<query>&kind=<entity|person>`` — free-text synthesis.
* ``GET /export?q=<query>&kind=<...>&format=<json|jsonl|zip>`` — downloadable BODS bundle.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import __version__
from .config import get_settings
from .routers import health, search, lookup, export, narrative
from .routers.search import _ch_ra_code as _ch_ra_code  # re-exported for backward compat

log = logging.getLogger(__name__)

# MCP server (in-process). Built defensively: a failure here must never take
# down the REST API, so we degrade to "no MCP mount" with a logged warning.
try:
    from . import mcp as _mcp_pkg

    _MCP = _mcp_pkg.mcp
    _MCP_ASGI = _mcp_pkg.asgi_app()  # also lazily creates _MCP.session_manager
except Exception as exc:  # noqa: BLE001
    _MCP = None
    _MCP_ASGI = None
    log.warning("MCP server unavailable, not mounting /mcp: %s", exc)


async def _warm_caches_background() -> None:
    """Pre-build the GEM/GLEIF/GEOT indexes off the event loop.

    Render's filesystem is ephemeral, so every deploy starts cold: the GEM
    CSVs (several MB) and the GLEIF GEM↔LEI mapping must be downloaded and
    parsed before the first climatetrace lookup can answer. Doing it here —
    in a background thread, started at lifespan — means the first user
    never pays that cost, and the event loop is never blocked by it.
    Failures are logged and non-fatal: the adapter falls back to its lazy
    path on first use.
    """
    try:
        from .sources.climatetrace import warm_caches

        stats = await asyncio.to_thread(warm_caches)
        log.info("Startup cache warm-up complete: %s", stats)
    except asyncio.CancelledError:  # shutdown before warm-up finished
        raise
    except Exception as exc:  # noqa: BLE001
        log.warning("Startup cache warm-up failed (lazy fallback remains): %s", exc)

    # OpenTender DB: pre-download + verify off the request path so the first
    # lookup never blocks on (and never races) a large S3 fetch. No-op unless
    # OpenTender is registered AND OPENTENDER_DB_FILE is configured — the
    # registry guard (in warm_opentender_db) stops a retired source from pulling
    # a multi-hundred-MB DB onto Render's 2 GB-capped /tmp on every cold start.
    # Failures are non-fatal (lazy fallback).
    try:
        from .sources.opentender import warm_opentender_db

        await asyncio.to_thread(warm_opentender_db)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        log.warning("OpenTender DB warm-up failed (lazy fallback remains): %s", exc)


# The MCP streamable-HTTP session manager is single-use per instance (its
# ``run()`` can be entered only once per process). Production starts the lifespan
# exactly once, but the test suite spins it up repeatedly in one process — so we
# guard with a module-level flag and only enter it the first time.
_mcp_session_started = False


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan hook — kicks off cache warm-up and runs the MCP server.

    The mounted MCP streamable-HTTP app does not get its own lifespan run by the
    parent, so the session manager must be entered here, or ``/mcp`` requests
    would fail. When MCP failed to build (or was already started in this
    process), we just run the warm-up.
    """
    global _mcp_session_started
    warmup = asyncio.create_task(_warm_caches_background())
    async with AsyncExitStack() as stack:
        if _MCP is not None and not _mcp_session_started:
            await stack.enter_async_context(_MCP.session_manager.run())
            _mcp_session_started = True
        try:
            yield  # server runs here
        finally:
            if not warmup.done():
                warmup.cancel()


app = FastAPI(
    title="OpenCheck",
    version=__version__,
    description=(
        "Customer due diligence risk checks driven by the Legal Entity "
        "Identifier (LEI) and open data — mapped to the "
        "Beneficial Ownership Data Standard (BODS)."
    ),
    lifespan=_lifespan,
)


_cors_origin = get_settings().cors_origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _cors_origin == "*" else [_cors_origin],
    allow_credentials=(_cors_origin != "*"),
    allow_methods=["*"],
    allow_headers=["*"],
    # Browser-based MCP clients must be able to read the session/protocol headers
    # off streamable-HTTP responses (otherwise the handshake can't continue).
    expose_headers=["Mcp-Session-Id", "Mcp-Protocol-Version"],
)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unhandled exceptions."""
    import logging
    logging.getLogger(__name__).exception("Unhandled exception in %s %s", request.method, request.url)
    origin = request.headers.get("origin")
    extra_headers: dict[str, str] = {"access-control-allow-origin": "*"} if origin else {}
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {exc}"},
        headers=extra_headers,
    )


app.include_router(health.router)
app.include_router(search.router)
app.include_router(lookup.router)
app.include_router(export.router)
app.include_router(narrative.router)


# ---------------------------------------------------------------------------
# MCP server: streamable-HTTP app mounted at /mcp, plus its ARD descriptor.
# ---------------------------------------------------------------------------
if _MCP_ASGI is not None:

    @app.get("/.well-known/mcp.json")
    async def mcp_descriptor() -> JSONResponse:
        """ARD / ai-catalog descriptor for the MCP server (CORS-readable)."""
        return JSONResponse(
            _mcp_pkg.descriptor(),
            headers={"access-control-allow-origin": "*"},
        )

    # Register the streamable-HTTP route (published at /mcp) directly on the app
    # instead of app.mount("/mcp", …): a Mount would 307-redirect a bare
    # POST /mcp → /mcp/, which breaks MCP clients that don't replay POST bodies
    # across redirects. A direct route serves /mcp with no redirect.
    app.router.routes.extend(_MCP_ASGI.routes)
