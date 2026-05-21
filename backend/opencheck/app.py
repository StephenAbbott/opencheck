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

import json
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import __version__
from .config import get_settings
from .routers import health, search, lookup, export
from .routers.search import _ch_ra_code as _ch_ra_code  # re-exported for backward compat


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    """FastAPI lifespan hook (reserved for future startup tasks)."""
    yield  # server runs here


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
