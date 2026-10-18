"""One shared client for RIK's e-Business Register services.

RIK publishes two credentialed services beside the open-data files: the SOAP
X-Road service at ``ariregxmlv6.rik.ee`` (the history reads,
``ariregister.py::fetch_timeline_data``) and a REST API at ``ar-rest.rik.ee``
(evaluated 17 Oct 2026 on the Notion ticket "Evaluate RIK's REST API"). Both
answer under one contract partner, and RIK's terms for that contract say:

* **50,000 answers a day** per contract partner;
* **at most one simultaneous query** per contract partner.

Those limits are shared by every OpenCheck user and by both services, so
nothing may call either service except through this module. What it does:

* **One query at a time.** :func:`contract_slot` holds one ``asyncio.Lock``
  for the length of a request, per event loop (the ``ny_dos`` ``_bucket()``
  rule: a lock that has been waited on binds to its loop). A caller that
  cannot get the slot within ``ARIREGISTER_QUEUE_WAIT_S`` is refused
  (:class:`RikUnavailable`, ``rate_limited``) rather than queueing behind a
  pile of other lookups.
* **A daily answer budget**, counted per request sent, on the register's own
  day (Europe/Tallinn). It stops at ``ARIREGISTER_DAILY_BUDGET`` (40,000 by
  default, under the contract's 50,000: the counter is in-process and starts
  again at zero on a deploy, so it must leave room for what an earlier
  process already spent that day). A spent budget is a refusal before
  anything is sent.
* **The REST token.** ``GET /keyLogin/`` with the key in the ``apiKey``
  header (a query parameter is a 401) returns a bearer token, cached for the
  process and fetched again when a call answers 401. Every REST call sends
  ``acceptLanguage: eng`` (``en`` is a 422) and ``apiVersion: V1``. The key
  and the token never go in a URL, a log line or an error.
* **Counted, in a closed vocabulary**, for ``/signalstats`` (:func:`stats`):
  requests by service and outcome, refusals by reason, the queue — never a
  registry code, a name or a path.

How a caller reads the outcomes (the Phase 262 rule):

* :class:`RikNotFound` — RIK's definitive "no such registry code" (HTTP 404
  with ``code: 4043``). The one answer a caller may treat as "no record".
* :class:`RikUnavailable` — a spent budget, a busy slot, a 429, a timeout, a
  5xx, a rejected key, an unreadable body. Never "no record" and never
  cached: the caller records a ``DegradedSource`` with
  :attr:`RikUnavailable.reason` (the degradation vocabulary) and
  :attr:`RikUnavailable.detail` (a fixed sentence).
* :class:`RikNotConfigured` — no key, or live calls are off. The caller
  falls back exactly as it did before this module existed.

**Personal data.** REST answers carry personal identification codes
(``relatedPersons[].identityCode``, ``representationRights[].entityCode``)
and shareholders' e-mail and street addresses. :func:`get_json` hands the
parsed body to its caller, which may use a code to join and must then drop
it (as ``ariregister_history.py`` does for SOAP). This module never logs,
caches or repeats a body; :func:`personal_data_paths` is the check a caller's
schema runs before anything leaves the adapter.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
import weakref
from collections import Counter
from collections.abc import AsyncIterator, Iterator, Mapping
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from typing import Any

import httpx

from .. import degradation, provenance
from ..config import get_settings

logger = logging.getLogger(__name__)

REST_BASE = "https://ar-rest.rik.ee"
LOGIN_PATH = "/keyLogin/"

#: The contract's ceiling, for the record. The client stops at the configured
#: budget, which is lower — see the module docstring.
CONTRACT_DAILY_ANSWERS = 50_000

#: Sent on every REST call. ``acceptLanguage`` takes ``est`` or ``eng``.
REST_HEADERS = {"acceptLanguage": "eng", "apiVersion": "V1", "Accept": "application/json"}

#: Per-request timeout. Most reads answer in 0.2–0.6 s; Bolt Technology OÜ's
#: full person history (485 KB) took 2 s on 17 Oct 2026.
REST_TIMEOUT_S = 30.0

#: RIK's error code for "Registry code … did not return any matches".
NOT_FOUND_CODE = 4043

#: The services a request may be counted under. A closed vocabulary: a call
#: under any other name is a programming error (``ValueError``), never a new
#: counter key. The SOAP services are here because they spend the same budget.
SERVICES = frozenset(
    {
        "login",
        "company",
        "general_data",
        "related_persons",
        "company_relations",
        "commercial_pledges",
        "soap_detail",
        "soap_beneficial_owners",
    }
)

#: What a request came to.
OUTCOMES = frozenset(
    {
        "answered",
        "not_found",
        "unauthorized",
        "forbidden",
        "rate_limited",
        "server_error",
        "client_error",
        "timeout",
        "network_error",
        "bad_response",
    }
)

#: Why a request was not sent.
REFUSALS = frozenset({"budget", "busy"})

#: Fixed sentences for a degradation's ``detail``, each reading after the
#: source's name. No registry code, name or status text ever reaches one.
DETAILS = {
    "budget": (
        "OpenCheck's daily share of the e-Business Register's query allowance "
        "is used up, so the register's credentialed service was not asked"
    ),
    "busy": (
        "the e-Business Register's credentialed service allows one query at a "
        "time and was busy with other checks, so it was not asked"
    ),
    "unauthorized": "the e-Business Register refused OpenCheck's API key",
    "forbidden": "the e-Business Register has not enabled this service for OpenCheck's key",
    "rate_limited": "the e-Business Register is limiting requests",
    "server_error": "the e-Business Register's service returned an error",
    "client_error": "the e-Business Register's service did not accept the request",
    "timeout": "the e-Business Register's service did not answer in time",
    "network_error": "the e-Business Register's service could not be reached",
    "bad_response": "the e-Business Register's service returned an answer that could not be read",
}

_REASON_FOR = {
    "budget": degradation.REASON_RATE_LIMITED,
    "busy": degradation.REASON_RATE_LIMITED,
    "rate_limited": degradation.REASON_RATE_LIMITED,
    "timeout": degradation.REASON_TIMEOUT,
}


class RikError(Exception):
    """Base class. The message is always one of the fixed sentences."""


class RikNotConfigured(RikError):
    """No REST key, or live calls are off: the caller falls back."""


class RikNotFound(RikError):
    """RIK's definitive answer that it holds no such registry code."""


class RikUnavailable(RikError):
    """The service could not answer this time. Never "no record"."""

    def __init__(self, cause: str) -> None:
        self.cause = cause
        self.reason = _REASON_FOR.get(cause, degradation.REASON_UPSTREAM_ERROR)
        self.detail = DETAILS[cause]
        super().__init__(self.detail)


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #


def rest_key() -> str | None:
    key = (get_settings().ariregister_rest_api_key or "").strip()
    return key or None


def rest_configured() -> bool:
    """Is the REST client on? A key, and live calls allowed."""
    return bool(rest_key()) and bool(get_settings().allow_live)


def daily_budget() -> int:
    return max(0, min(int(get_settings().ariregister_daily_budget), CONTRACT_DAILY_ANSWERS))


def queue_wait_s() -> float:
    return max(0.0, float(get_settings().ariregister_queue_wait_s))


# --------------------------------------------------------------------------- #
# The register's day
# --------------------------------------------------------------------------- #

try:  # pragma: no cover — tzdata is present on Render and in CI
    from zoneinfo import ZoneInfo

    _TALLINN: Any = ZoneInfo("Europe/Tallinn")
except Exception:  # noqa: BLE001
    _TALLINN = timezone.utc


def _register_day(now: datetime | None = None) -> date:
    """Today on the register's calendar. RIK's daily allowance runs on
    Estonian time; without tz data the day is UTC's, which is at most three
    hours off and only ever early."""
    return (now or datetime.now(timezone.utc)).astimezone(_TALLINN).date()


# --------------------------------------------------------------------------- #
# Counters
# --------------------------------------------------------------------------- #


class _Totals:
    """Today's counters. Thread-safe: ``/signalstats`` reads from any thread."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started = time.time()
        self.day = _register_day()
        self.sent = 0
        self.outcomes: Counter[tuple[str, str]] = Counter()
        self.refused: Counter[tuple[str, str]] = Counter()
        self.waiting = 0
        self.peak_waiting = 0
        self.logins = 0
        self.wait_ms: list[float] = []

    def roll(self) -> None:
        """Start a new day's counts when the register's day has turned.
        Call with ``lock`` held."""
        today = _register_day()
        if today != self.day:
            self.day = today
            self.sent = 0
            self.outcomes.clear()
            self.refused.clear()
            self.peak_waiting = self.waiting
            self.logins = 0
            self.wait_ms = []


totals = _Totals()

#: The last waits kept for the percentile, as ``pipelinestats`` does.
_WAIT_SAMPLE = 500


def _check_service(service: str) -> None:
    if service not in SERVICES:
        raise ValueError(f"unknown e-Business Register service {service!r}")


def _charge(service: str) -> None:
    """Spend one answer from today's budget, or refuse before sending."""
    _check_service(service)
    with totals.lock:
        totals.roll()
        if totals.sent >= daily_budget():
            totals.refused[(service, "budget")] += 1
            refused = True
        else:
            totals.sent += 1
            refused = False
    if refused:
        raise RikUnavailable("budget")


def record_outcome(service: str, outcome: str) -> None:
    """Count what one sent request came to."""
    _check_service(service)
    if outcome not in OUTCOMES:
        raise ValueError(f"unknown outcome {outcome!r}")
    with totals.lock:
        totals.roll()
        totals.outcomes[(service, outcome)] += 1


def _refuse_busy(service: str) -> None:
    with totals.lock:
        totals.roll()
        totals.refused[(service, "busy")] += 1


def remaining_today() -> int:
    with totals.lock:
        totals.roll()
        return max(0, daily_budget() - totals.sent)


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round(q * len(ordered) + 0.5)) - 1))
    return round(ordered[index], 1)


def stats() -> dict[str, Any]:
    """The ``ariregister_rik`` section of ``/signalstats``. Counts and dates
    only: every key is a service, an outcome or a refusal reason."""
    settings = get_settings()
    with totals.lock:
        totals.roll()
        outcomes = Counter(totals.outcomes)
        refused = Counter(totals.refused)
        sent = totals.sent
        day = totals.day.isoformat()
        waiting = totals.waiting
        peak = totals.peak_waiting
        logins = totals.logins
        waits = list(totals.wait_ms)
        started = totals.started
    budget = daily_budget()
    return {
        "rest_configured": rest_configured(),
        "soap_configured": bool(settings.ariregister_username and settings.ariregister_password),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "register_day": day,
        "contract_daily_answers": CONTRACT_DAILY_ANSWERS,
        "daily_budget": budget,
        "sent_today": sent,
        "remaining_today": max(0, budget - sent),
        "token_logins_today": logins,
        "outcomes": {f"{s}|{o}": n for (s, o), n in sorted(outcomes.items())},
        "refused_total": sum(refused.values()),
        "refused": {f"{s}|{r}": n for (s, r), n in sorted(refused.items())},
        "queue": {
            "waiting": waiting,
            "peak_waiting_today": peak,
            "max_wait_s": queue_wait_s(),
            "wait_ms_p50": _percentile(waits, 0.5),
            "wait_ms_p95": _percentile(waits, 0.95),
        },
    }


# --------------------------------------------------------------------------- #
# The one-query slot
# --------------------------------------------------------------------------- #

_LOCKS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock]" = (
    weakref.WeakKeyDictionary()
)


def _lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _LOCKS.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _LOCKS[loop] = lock
    return lock


@asynccontextmanager
async def _slot(service: str) -> AsyncIterator[None]:
    """Hold the contract's one query slot, or refuse as ``busy``."""
    _check_service(service)
    lock = _lock()
    with totals.lock:
        totals.roll()
        totals.waiting += 1
        totals.peak_waiting = max(totals.peak_waiting, totals.waiting)
    started = time.monotonic()
    acquired = False
    try:
        try:
            async with asyncio.timeout(queue_wait_s()):
                await lock.acquire()
            acquired = True
        except TimeoutError:
            pass
    finally:
        with totals.lock:
            totals.waiting -= 1
            if acquired:
                totals.wait_ms.append((time.monotonic() - started) * 1000.0)
                del totals.wait_ms[:-_WAIT_SAMPLE]
    if not acquired:
        _refuse_busy(service)
        raise RikUnavailable("busy")
    try:
        yield
    finally:
        lock.release()


@asynccontextmanager
async def contract_slot(service: str) -> AsyncIterator[None]:
    """One request to either service: the slot, then one answer charged.

    The SOAP history reads wrap their ``POST`` in this. Raises
    :class:`RikUnavailable` (``busy`` or ``budget``) before anything is sent.
    The caller records the outcome with :func:`record_outcome`.
    """
    async with _slot(service):
        _charge(service)
        yield


# --------------------------------------------------------------------------- #
# REST
# --------------------------------------------------------------------------- #

#: The bearer token, for this process. A string is loop-agnostic.
_token: str | None = None


def _client(service: str) -> httpx.AsyncClient:
    """The REST client, built once the slot is held. Building it is the
    moment the request is committed to, so the live observation is recorded
    here (the PR #153 rule; ``test_source_probes`` checks it is)."""
    provenance.record_live(f"ariregister REST {service}")
    # The shared User-Agent: from a Mac on 17 Oct 2026 RIK answered curl in
    # 0.3 s and held Python urllib's default agent past a 30 s timeout.
    from ..http import _USER_AGENT

    return httpx.AsyncClient(
        base_url=REST_BASE, timeout=REST_TIMEOUT_S, follow_redirects=False,
        headers={"User-Agent": _USER_AGENT},
    )


def outcome_of_status(status: int) -> str:
    if status == 401:
        return "unauthorized"
    if status == 403:
        return "forbidden"
    if status == 429:
        return "rate_limited"
    if status >= 500:
        return "server_error"
    return "client_error"


async def _send(
    client: httpx.AsyncClient, service: str, path: str, *, headers: Mapping[str, str],
    params: Mapping[str, Any] | None = None,
) -> httpx.Response:
    """One request inside the held slot: charge it, send it, count it.
    Transport failures become :class:`RikUnavailable`."""
    _charge(service)
    try:
        response = await client.get(path, headers=dict(headers), params=dict(params or {}))
    except httpx.TimeoutException:
        record_outcome(service, "timeout")
        raise RikUnavailable("timeout") from None
    except httpx.HTTPError as exc:
        record_outcome(service, "network_error")
        logger.info("ariregister REST: %s on %s", type(exc).__name__, service)
        raise RikUnavailable("network_error") from None
    return response


async def _login(client: httpx.AsyncClient, key: str) -> str:
    global _token
    response = await _send(client, "login", LOGIN_PATH, headers={**REST_HEADERS, "apiKey": key})
    if response.status_code != 200:
        # An unknown key is a 404 here ({"detail": "API Key not found"}),
        # not a 401 — it is still the key being refused.
        outcome = (
            "unauthorized" if response.status_code == 404 else outcome_of_status(response.status_code)
        )
        record_outcome("login", outcome)
        raise RikUnavailable(outcome)
    try:
        body = response.json()
        token = body["access_token"]
        if not isinstance(token, str) or not token:
            raise ValueError
    except (ValueError, KeyError, TypeError):
        record_outcome("login", "bad_response")
        raise RikUnavailable("bad_response") from None
    record_outcome("login", "answered")
    with totals.lock:
        totals.roll()
        totals.logins += 1
    _token = token
    return token


def _is_not_found(response: httpx.Response) -> bool:
    if response.status_code != 404:
        return False
    try:
        body = response.json()
    except ValueError:
        return False
    return isinstance(body, dict) and str(body.get("code")) == str(NOT_FOUND_CODE)


async def get_json(
    path: str, *, service: str, params: Mapping[str, Any] | None = None
) -> Any:
    """``GET`` one REST path and return the parsed JSON body.

    ``path`` is relative to ``https://ar-rest.rik.ee`` (``/company/10421629``).
    Raises :class:`RikNotConfigured`, :class:`RikNotFound` or
    :class:`RikUnavailable`; ``None`` for a 204 (a search with no result). The body may carry personal codes — see the
    module docstring.
    """
    global _token
    _check_service(service)
    if service == "login":
        raise ValueError("log in through get_json's own token handling")
    key = rest_key()
    if not key or not get_settings().allow_live:
        raise RikNotConfigured("the e-Business Register's REST service is not configured")

    async with _slot(service):
        async with _client(service) as client:
            token = _token or await _login(client, key)
            response = await _send(
                client, service, path, headers={**REST_HEADERS, "Authorization": f"Bearer {token}"},
                params=params,
            )
            if response.status_code == 401:
                # The token expired. One more login, one more try; a second
                # 401 is the key itself being refused.
                record_outcome(service, "unauthorized")
                _token = None
                token = await _login(client, key)
                response = await _send(
                    client, service, path,
                    headers={**REST_HEADERS, "Authorization": f"Bearer {token}"}, params=params,
                )

    if response.status_code == 204:
        # "No Content": a search that found nothing (``companyRelations`` for
        # a code with no relations). An answer, not a failure.
        record_outcome(service, "answered")
        return None
    if _is_not_found(response):
        record_outcome(service, "not_found")
        raise RikNotFound("the e-Business Register holds no company with this registry code")
    if response.status_code != 200:
        outcome = outcome_of_status(response.status_code)
        record_outcome(service, outcome)
        if outcome == "unauthorized":
            _token = None
        raise RikUnavailable(outcome)
    try:
        body = response.json()
    except ValueError:
        record_outcome(service, "bad_response")
        raise RikUnavailable("bad_response") from None
    if not isinstance(body, (dict, list)):
        record_outcome(service, "bad_response")
        raise RikUnavailable("bad_response")
    record_outcome(service, "answered")
    return body


def record_degradation(exc: RikUnavailable, *, check: str = degradation.CHECK_SOURCE_FETCH) -> None:
    """The ``DegradedSource`` a caller records for a refusal or failure."""
    degradation.record("ariregister", exc.detail, reason=exc.reason, check=check)


# --------------------------------------------------------------------------- #
# Personal data
# --------------------------------------------------------------------------- #

#: Keys whose value is personal data whatever it looks like: the REST
#: answers' person codes (``identityCode`` on ``relatedPersons``,
#: ``entityCode`` on ``representationRights``) and contact details.
PERSONAL_KEYS = frozenset({"identityCode", "entityCode", "personalCode", "email", "eMail"})

#: Keys that hold a company's code *or* a person's, depending on the row
#: (the files' ``isikukood_registrikood``). Refused only when the value is a
#: personal code — the ``AriregisterBundle`` rule.
CODE_KEYS = frozenset({"isikukood_registrikood", "isikukood", "code", "registryCode"})

#: An Estonian personal identification code, matched as the whole value:
#: century-and-sex digit 1–6, then ten digits (the schema's ``_PERSONAL_CODE``).
PERSONAL_CODE = re.compile(r"^[1-6]\d{10}$")


def personal_data_paths(value: Any, path: str = "") -> Iterator[str]:
    """Every JSON path in ``value`` that carries personal data: a key in
    :data:`PERSONAL_KEYS` with a value, or a key in :data:`CODE_KEYS` whose
    value is a personal code. A bundle built from a REST answer must yield
    nothing here before it leaves the adapter. Paths name keys, never values.

    Free text is not scanned: an eleven-digit Latvian registry code has the
    same shape as a personal code, and texts are scrubbed where they are
    read (``ariregister.scrub_text``)."""
    if isinstance(value, Mapping):
        for k, v in value.items():
            here = f"{path}/{k}"
            if k in PERSONAL_KEYS and v not in (None, "", [], {}):
                yield here
            elif k in CODE_KEYS and PERSONAL_CODE.match(str(v).strip() if v is not None else ""):
                yield here
            else:
                yield from personal_data_paths(v, here)
    elif isinstance(value, list):
        for i, v in enumerate(value):
            yield from personal_data_paths(v, f"{path}/{i}")


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


def reset_for_tests() -> None:
    """Forget the token, the counters and every loop's lock."""
    global totals, _token
    _token = None
    totals = _Totals()
    _LOCKS.clear()


__all__ = [
    "CODE_KEYS", "CONTRACT_DAILY_ANSWERS", "DETAILS", "NOT_FOUND_CODE", "OUTCOMES", "PERSONAL_CODE",
    "PERSONAL_KEYS", "REFUSALS", "REST_BASE", "REST_HEADERS", "SERVICES", "RikError",
    "RikNotConfigured", "RikNotFound", "RikUnavailable", "contract_slot", "daily_budget",
    "get_json", "outcome_of_status", "personal_data_paths", "record_degradation", "record_outcome",
    "remaining_today", "reset_for_tests", "rest_configured", "stats",
]
