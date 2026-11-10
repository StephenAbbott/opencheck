"""The Phase 144 bot gate, with one door for OpenCheck's own test agents.

``/lookup-stream``, ``/lookup-register-stream``, ``/batch-stream`` and the
saved-report routes serve the interactive app and refuse declared automated
clients (``memwatch.is_bot`` on the User-Agent). OpenCheck's scheduled
post-deploy tester drives the site in headless Chromium, whose User-Agent
says ``HeadlessChrome``, so every report page it opened showed "Unknown
error" (10 Nov 2026).

The door is a shared secret, not a query parameter. ``?qa=1`` is on the page
URL, which anyone can type, and a public pass would let every crawler that
learns of it back onto the streams the gate exists to protect. Instead the
agent sends ``X-OpenCheck-QA: <token>`` on every request (Playwright's
``extraHTTPHeaders``), and the server compares it with
``OPENCHECK_QA_BYPASS_TOKEN``. No token configured means no door. A request
let through is still a lookup like any other: rate-limited and charged to
the shared lookup budget by its IP. Pair the header with ``?qa=1`` on the
page so the run is left out of the analytics too.
"""

from __future__ import annotations

import hmac

from fastapi import Request

from .config import get_settings
from .memwatch import is_bot

QA_HEADER = "X-OpenCheck-QA"

#: A token shorter than this is refused as a door: a guessable secret is a
#: public pass.
MIN_TOKEN_LENGTH = 24


def qa_pass(request: Request) -> bool:
    """True when the request carries the configured QA token."""
    token = (get_settings().qa_bypass_token or "").strip()
    if len(token) < MIN_TOKEN_LENGTH:
        return False
    sent = request.headers.get(QA_HEADER) or ""
    return hmac.compare_digest(sent.encode(), token.encode())


def refuses(request: Request) -> bool:
    """True when the gate is on, the User-Agent declares a bot and the
    request does not carry the QA token."""
    if not get_settings().bot_gate_lookup_stream:
        return False
    if not is_bot(request.headers.get("user-agent")):
        return False
    return not qa_pass(request)
