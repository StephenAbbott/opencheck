"""OpenCheck MCP server package.

``mcp`` is the FastMCP instance (tools defined in ``server``); ``asgi_app()``
returns the streamable-HTTP ASGI app to mount on the FastAPI server; and
``descriptor()`` returns the ``/.well-known/mcp.json`` document that the ARD /
ai-catalog ``application/mcp-server+json`` entry points at.
"""

from __future__ import annotations

from typing import Any

from .. import __version__
from .server import TOOL_NAMES, mcp

_PUBLIC_BASE = "https://api.opencheck.world"


def asgi_app():
    """Build the streamable-HTTP ASGI app (also creates ``mcp.session_manager``)."""
    return mcp.streamable_http_app()


def descriptor() -> dict[str, Any]:
    """The MCP server descriptor served at ``/.well-known/mcp.json``."""
    return {
        "name": "opencheck",
        "displayName": "OpenCheck MCP Server",
        "description": (
            "LEI-driven customer due diligence: search a company to its LEI, "
            "resolve owners/controllers and sanctions/PEP/debarment risk, and "
            "export the beneficial-ownership graph as BODS v0.4."
        ),
        "version": __version__,
        "transport": "streamable-http",
        "endpoint": f"{_PUBLIC_BASE}/mcp",
        "tools": TOOL_NAMES,
        "documentation": "https://opencheck.world",
    }


__all__ = ["mcp", "asgi_app", "descriptor", "TOOL_NAMES"]
