"""Per-IP rate limiting for the MCP mount (Phase 234).

The REST routes carry ``@limiter.limit`` decorators; ``/mcp`` cannot — slowapi's
decorator needs a plain endpoint function and its middleware breaks the
streamable-HTTP app (see ``ratelimit.py``). Until this phase ``/mcp`` therefore
had no limit at all, and ``opencheck_lookup`` ran a full lookup per call for
as many calls as a client cared to send.

:class:`McpRateGuard` is a pure-ASGI wrapper around the MCP route. For each
request it spends one unit of the default tier (every JSON-RPC message costs
something, ``initialize`` and ``tools/list`` included), then reads the body and,
for every ``tools/call`` in it, spends the tier the equivalent REST route is on:

=============================  ==========  ===========================
tool                           tier        REST counterpart
=============================  ==========  ===========================
opencheck_lookup               lookup      ``GET /lookup``
opencheck_export_bods          lookup      ``GET /export``
opencheck_search               lookup      ``GET /search``
opencheck_person_check         lookup      ``GET /person-check``
opencheck_batch_lookup         heavy       ``POST /batch``
opencheck_save_report          heavy       ``POST /saved-reports``
=============================  ==========  ===========================

That is the *request* limit. The *work* limit is separate and shared with REST:
every fresh pipeline a tool starts is charged to the caller's one lookup
budget (``lookup_budget.charge``, via the client context the app's middleware
sets), so an agent and a browser on the same IP draw on the same ten a minute.

A refusal is HTTP 429 with ``Retry-After`` and a JSON-RPC error body per
message, so a client that surfaces JSON-RPC errors shows the reason.
"""

from __future__ import annotations

import json
import math
from typing import Any

from starlette.requests import Request
from starlette.routing import BaseRoute, Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ..config import get_settings
from ..lookup_budget import Quota
from ..ratelimit import client_ip

#: A JSON-RPC body larger than this is refused unread (tool arguments are
#: small; a batch of twenty LEIs is well under a kilobyte).
MAX_BODY_BYTES = 1_000_000

_REQUESTS = Quota("mcp:request", lambda: get_settings().rate_limit_default)
_LOOKUP = Quota("mcp:lookup", lambda: get_settings().rate_limit_lookup)
_HEAVY = Quota("mcp:heavy", lambda: get_settings().rate_limit_heavy)

TOOL_TIERS: dict[str, Quota] = {
    "opencheck_lookup": _LOOKUP,
    "opencheck_export_bods": _LOOKUP,
    "opencheck_search": _LOOKUP,
    "opencheck_person_check": _LOOKUP,
    "opencheck_batch_lookup": _HEAVY,
    "opencheck_save_report": _HEAVY,
}


def _messages(body: bytes) -> list[dict[str, Any]]:
    try:
        parsed = json.loads(body or b"null")
    except (ValueError, UnicodeDecodeError):
        return []
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [m for m in parsed if isinstance(m, dict)]
    return []


def _tool_calls(messages: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    for m in messages:
        if m.get("method") == "tools/call":
            params = m.get("params") if isinstance(m.get("params"), dict) else {}
            name = params.get("name")
            if isinstance(name, str):
                names.append(name)
    return names


async def _refuse(
    send: Send, status: int, message: str, ids: list[Any], retry_after: float | None = None
) -> None:
    errors = [
        {"jsonrpc": "2.0", "id": i, "error": {"code": -32000, "message": message}}
        for i in (ids or [None])
    ]
    body = json.dumps(errors[0] if len(errors) == 1 else errors).encode()
    headers = [(b"content-type", b"application/json")]
    if retry_after is not None:
        headers.append((b"retry-after", str(max(1, math.ceil(retry_after))).encode()))
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


class McpRateGuard:
    """Pure-ASGI rate guard in front of the streamable-HTTP MCP app."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        client = client_ip(Request(scope))

        if scope.get("method") != "POST":
            wait = _REQUESTS.retry_after(client)
            if wait is not None:
                await _refuse(send, 429, _limit_message("requests"), [], wait)
                return
            _REQUESTS.hit(client)
            await self.app(scope, receive, send)
            return

        # Read the body so the tool names can be charged, then hand the same
        # bytes to the MCP app.
        chunks: list[bytes] = []
        size = 0
        more = True
        while more:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > MAX_BODY_BYTES:
                await _refuse(send, 413, "Request body too large for an MCP call.", [])
                return
            chunks.append(chunk)
            more = bool(message.get("more_body"))
        body = b"".join(chunks)
        messages = _messages(body)
        ids = [m.get("id") for m in messages if "id" in m]

        charges: list[Quota] = [_REQUESTS] + [
            TOOL_TIERS[name] for name in _tool_calls(messages) if name in TOOL_TIERS
        ]
        worst: float | None = None
        for quota in charges:
            wait = quota.retry_after(client)
            if wait is not None:
                worst = wait if worst is None else max(worst, wait)
        if worst is not None:
            await _refuse(send, 429, _limit_message("tools"), ids, worst)
            return
        for quota in charges:
            quota.hit(client)

        sent = False

        async def replay() -> Message:
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def _limit_message(kind: str) -> str:
    what = "MCP requests" if kind == "requests" else "calls to this tool"
    return (
        f"Rate limit exceeded: too many {what} from this address. OpenCheck is a "
        "free, shared service — every full lookup counts against the same "
        "per-address budget as the website. Retry after the Retry-After interval."
    )


def guard_routes(routes: list[BaseRoute]) -> list[BaseRoute]:
    """The MCP app's routes with every endpoint route behind the guard."""
    guarded: list[BaseRoute] = []
    for route in routes:
        if isinstance(route, Route) and not _is_function(route.endpoint):
            guarded.append(
                Route(
                    route.path,
                    endpoint=McpRateGuard(route.endpoint),
                    methods=list(route.methods) if route.methods else None,
                    name=route.name,
                )
            )
        else:
            guarded.append(route)
    return guarded


def _is_function(endpoint: Any) -> bool:
    import inspect

    return inspect.isfunction(endpoint) or inspect.ismethod(endpoint)
