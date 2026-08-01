"""Guard: the MCP server must actually be mounted on the app.

``app.py`` builds the MCP mount defensively — if ``opencheck.mcp`` fails to
import (missing/incompatible ``mcp`` SDK), the REST API still starts and the
``/mcp`` routes are simply skipped with a logged warning. That is the right
production behaviour but the worst CI behaviour: a bad dependency resolution
(e.g. the breaking MCP SDK v2) would ship a server whose REST surface is green
while the MCP surface has silently vanished.

This module therefore asserts the mount unconditionally. It deliberately does
NOT import ``mcp`` or ``opencheck.mcp`` and has no ``importorskip`` — it must
fail, not skip, when the MCP surface is missing for any reason.
"""

from __future__ import annotations

from opencheck.app import app


def _route_paths() -> set[str]:
    return {p for r in app.router.routes if (p := getattr(r, "path", None))}


def test_mcp_route_is_mounted() -> None:
    assert "/mcp" in _route_paths(), (
        "/mcp is not mounted — opencheck.mcp failed to import (incompatible "
        "`mcp` SDK? see the pin comment in pyproject.toml) and app.py "
        "degraded to REST-only."
    )


def test_mcp_descriptor_route_is_mounted() -> None:
    assert "/.well-known/mcp.json" in _route_paths()
