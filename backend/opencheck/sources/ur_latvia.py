"""Latvia Register of Enterprises (UR) adapter.

The Uzņēmumu reģistrs (UR) is Latvia's statutory business register, operating
under the Ministry of Justice.  Open data is published on Latvia's national
open-data portal (data.gov.lv) via a CKAN instance.  Every table is loaded
into the CKAN Datastore, so records can be queried row-by-row through the API
without downloading the full CSV bulk files.

Five datasets are used by the lookup
------------------------------------
business_register   (128 MB CSV) — entity profiles
beneficial_owners   ( 17 MB CSV) — UBO declarations
historical_names    (  7 MB CSV) — former names
officers            ( 40 MB CSV) — board members / representatives
members_sia         ( 24 MB CSV) — LLC (SIA) shareholders

Resource IDs on data.gov.lv
----------------------------
``25e80bf3-f107-4ab4-89ef-251b5b9374e9``  business register
``20a9b26d-d056-4dbb-ae18-9ff23c87bdee``  beneficial owners
``ad772b8b-76e4-4334-83d9-3beadf513aa6``  historical names
``e665114a-73c2-4375-9470-55874b4cfa6b``  officers
``837b451a-4833-4fd1-bfdd-b45b35a994fd``  members (SIA shares)

The History tab (``fetch_timeline_data``, the Estonia–Latvia emitters
phase, 3 Nov 2026) reads six more, all dated, all CC0 on data.gov.lv:
AS stockholders, liquidations, reorganisations, insolvency proceedings,
suspensions and prohibitions, and securing measures. **UR's open data is
current rows only**: no people table carries an end date, a removed owner
or officer simply leaves the next day's file, and a row is edited in place.
So the Latvian history can say when every current owner, officer and
beneficial owner arrived and date every status, name, liquidation,
reorganisation, insolvency and suspension event, and it must say that
removals are not published — see ``timeline/ur_latvia.py``.

The historical-names table on the CKAN datastore stopped being reloaded
on 24 March 2025 (90,974 rows; the raw file had 94,241 on 3 Nov 2026), so
both readings take the names from the daily raw file at
``dati.ur.gov.lv/register/register_name_history.csv`` (cached a day) and
fall back to the CKAN copy only when that download fails.

The flow with GLEIF
-------------------
1. GLEIF returns ``registeredAt.id == "RA000423"`` and ``registeredAs``
   holding the 11-digit Latvian registration number (regcode).
2. app.py extracts ``derived["lv_regcode"]`` and calls ``fetch()`` here.
3. ``fetch()`` joins all five datasets for the given regcode, returning a
   single payload that the BODS mapper converts into statements.

Authentication: none — all endpoints are public.
License: Open data (PSI Directive / Public Information Act).
Attribution: "Contains data from the Latvian Register of Enterprises (UR),
  open data published on data.gov.lv."
CKAN portal: https://data.gov.lv/dati/lv/organization/ur
UR website: https://www.ur.gov.lv/en/
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any
from urllib.parse import quote

from .. import degradation
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..search_rank import clean_bare_name, match_tier
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo

_log = logging.getLogger(__name__)

# CKAN base URL for Latvia's open-data portal.
_CKAN_BASE = "https://data.gov.lv/dati/api/3/action"

# Resource IDs — taken from the five UR datasets on data.gov.lv.
_RES_BUSINESS = "25e80bf3-f107-4ab4-89ef-251b5b9374e9"
_RES_BOWNERS = "20a9b26d-d056-4dbb-ae18-9ff23c87bdee"
_RES_HIST_NAMES = "ad772b8b-76e4-4334-83d9-3beadf513aa6"
_RES_OFFICERS = "e665114a-73c2-4375-9470-55874b4cfa6b"
_RES_MEMBERS = "837b451a-4833-4fd1-bfdd-b45b35a994fd"

#: The History tab's six further tables (the Estonia–Latvia emitters phase).
_RES_STOCKHOLDERS = "6adabd83-93f9-4d7f-bebd-fa109bbf794a"
_RES_LIQUIDATIONS = "59e7ec49-f1c6-4410-8ee6-e7737ac5eaee"
_RES_REORGANIZATIONS = "83cebcee-ba0c-4162-b061-a8a1fae84f63"
_RES_INSOLVENCY = "8065ad80-1a4d-4afb-b1d1-d93b9a62b1cc"
_RES_SUSPENSIONS = "1a077ff6-4c4e-4cfe-b603-b7e40851e8a9"
_RES_SECURING = "a572e7a4-b23d-44b7-93d8-d09fc47729dd"

#: UR's own daily files, regenerated once a day (``ur_opendata.pdf``). The
#: historical names are read from here because the CKAN copy is stale.
_NAME_HISTORY_URL = "https://dati.ur.gov.lv/register/register_name_history.csv"
_NAME_HISTORY_CACHE_DAYS = 1.0

_CACHE_NS = "ur_latvia"

# GLEIF Registration Authority code for Latvia's UR.
LV_RA_CODE: str = "RA000423"

#: Phase 262: a table that did not answer is never read as an empty one. The
#: sentences name the register, never the company (``degradation.record``).
_ENTITY_FAILED_DETAIL = (
    "The Latvian Register of Enterprises (data.gov.lv) did not answer, so the "
    "company's record was not read."
)
_TABLES_FAILED_DETAIL = (
    "The Latvian Register of Enterprises (data.gov.lv) answered only in part: "
    "{tables} could not be read, so an empty list there is unchecked."
)
#: The lookup's tables, in the words the degradation sentence uses.
_TABLE_WORDS = {
    "historical_names": "former names",
    "beneficial_owners": "beneficial owners",
    "officers": "officers",
    "members": "members",
}


class UrLatviaUnavailable(Exception):
    """The register did not answer the History tab's read of the company.

    ``reason`` is in the degradation reason vocabulary.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _failure_label(failure: Any) -> str:
    """A short label for a response or exception ("HTTP 503", "ReadTimeout")."""
    status = getattr(failure, "status_code", None)
    if isinstance(status, int):
        return f"HTTP {status}"
    return type(failure).__name__


# CKAN datastore_search page size — keep generous for officers/members.
_PAGE = 100

#: Name search (the ur_latvia name-search ticket, 5 Nov 2026). data.gov.lv
#: dropped CKAN's ``datastore_search_sql`` action (HTTP 400 "Action name not
#: known") by 3 Nov 2026, so the name search is ``datastore_search`` with a
#: full-text ``q`` scoped to the ``name`` column. CKAN ranks a company's
#: branches and trade-union locals above the company itself (34 rows match
#: "Latvenergo"; AS "Latvenergo" is eighth), so a page of
#: ``_SEARCH_FETCH`` rows is read and :func:`rank_records` keeps the best
#: ``_SEARCH_LIMIT``.
_SEARCH_FETCH = 100
_SEARCH_LIMIT = 10
_SEARCH_FIELDS = (
    "regcode", "name", "name_in_quotes", "type", "type_text", "registered",
    "terminated", "closed", "address",
)
_SEARCH_TIMEOUT_S = 20.0
#: Register types that are part of another legal person — a branch, a
#: foreign company's branch or representative, a union's local unit —
#: listed after the legal persons at the same match tier.
_DEPENDENT_TYPES = frozenset({"FIL", "AKF", "PAR", "ASF", "ARV"})

# Latvian entity-type codes → English labels.
_ENTITY_TYPES: dict[str, str] = {
    "SIA": "Sabiedrība ar ierobežotu atbildību (LLC)",
    "AS": "Akciju sabiedrība (Joint-stock company)",
    "IK": "Individuālais komersants (Sole trader)",
    "IND": "Individuālais uzņēmums (Private enterprise)",
    "ZEM": "Zemnieku saimniecība (Farm enterprise)",
    "PS": "Pilnsabiedrība (General partnership)",
    "KS": "Komandītsabiedrība (Limited partnership)",
    "KB": "Kooperatīvā sabiedrība (Cooperative)",
    "BDR": "Biedrība (Association)",
    "NOD": "Nodibinājums (Foundation)",
    "VU": "Valsts uzņēmums (State enterprise)",
    "PSV": "Pašvaldības uzņēmums (Municipal enterprise)",
    "FIL": "Filiāle (Branch)",
    "AKF": "Ārvalsts komersanta filiāle (Foreign branch)",
    "PAR": "Ārvalsts komersanta pārstāvniecība (Foreign representative office)",
    "DRZ": "Draudze (Religious congregation)",
    "MIL": "Masu informācijas līdzeklis (Mass media entity)",
    "SPO": "Sporta organizācija (Sports organisation)",
    "SAB": "Sabiedriskā organizācija (Public organisation)",
    "PAJ": "Paju sabiedrība (Share company)",
    "ASF": "AS filiāle (JSC branch)",
}


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def normalise_regcode(rc: str | int) -> str:
    """Return the plain 11-digit registration number string.

    GLEIF stores Latvian regcodes as plain digit strings.  The CKAN
    ``regcode`` column is typed as integer, so we strip any whitespace and
    non-digit characters then return the numeric string.
    """
    return str(rc).strip().lstrip("0").rjust(11, "0") if str(rc).strip().isdigit() else str(rc).strip()


def _parse_name_history(text: str) -> list[dict[str, Any]]:
    """``register_name_history.csv`` (``regcode;date_to;name``, day-only
    dates) as CKAN-shaped rows, so the mapper and the emitter read one
    shape whichever copy answered."""
    import csv
    import io

    rows: list[dict[str, Any]] = []
    for rec in csv.DictReader(io.StringIO(text), delimiter=";"):
        code = (rec.get("regcode") or "").strip()
        name = (rec.get("name") or "").strip()
        if not code or not name:
            continue
        day = (rec.get("date_to") or "").strip()
        rows.append({
            "regcode": int(code) if code.isdigit() else code,
            "date_to": f"{day}T00:00:00" if day else None,
            "name": name,
        })
    return rows


def _slug(text: str) -> str:
    return hashlib.sha256(text.lower().strip().encode()).hexdigest()[:16]


class UrLatviaSearchError(RuntimeError):
    """The register's name search did not answer.

    Raised rather than returning the stub, so ``/search`` records the source
    in ``errors`` ("did not answer") instead of reading as no match. The
    message never carries the request URL or the response body.
    """


def search_url(query: str) -> str:
    """The ``datastore_search`` request for a name search — pinned by tests,
    because the previous shape asked for an action the portal stopped
    serving and its test mocked that action."""
    q = json.dumps({"name": query}, ensure_ascii=False)
    return (
        f"{_CKAN_BASE}/datastore_search"
        f"?resource_id={_RES_BUSINESS}"
        f"&q={quote(q)}"
        f"&fields={','.join(_SEARCH_FIELDS)}"
        f"&limit={_SEARCH_FETCH}"
    )


def parse_search_response(resp: Any) -> list[dict[str, Any]]:
    """The records of a ``datastore_search`` answer, or
    :class:`UrLatviaSearchError` for anything that is not a successful CKAN
    answer — an HTTP error, a non-JSON body, ``success: false``."""
    if not resp.is_success:
        raise UrLatviaSearchError(
            f"data.gov.lv name search answered HTTP {resp.status_code}"
        )
    try:
        data = resp.json()
    except ValueError as exc:
        raise UrLatviaSearchError(
            "data.gov.lv name search answered with a body that is not JSON"
        ) from exc
    if not isinstance(data, dict) or not data.get("success"):
        raise UrLatviaSearchError("data.gov.lv name search answered success: false")
    records = (data.get("result") or {}).get("records")
    if not isinstance(records, list):
        raise UrLatviaSearchError("data.gov.lv name search answered without records")
    return records


def _is_closed(rec: dict[str, Any]) -> bool:
    return bool(rec.get("terminated") or str(rec.get("closed") or "").strip())


def _record_tier(query: str, rec: dict[str, Any]) -> int:
    """The better ``search_rank.match_tier`` of the full name and the
    register's ``name_in_quotes`` — the name without its legal form.
    ``names`` knows no Latvian legal form, so 'Akciju sabiedrība
    "Latvenergo"' alone ties with every branch on the query "Latvenergo"."""
    tiers = [match_tier(query, str(rec.get("name") or ""))]
    bare = clean_bare_name(rec.get("name_in_quotes"))
    if bare:
        tiers.append(match_tier(query, bare))
    return min(tiers)


def rank_records(query: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Best ``_SEARCH_LIMIT`` records: the cross-source match tier first
    (``search_rank.match_tier`` — the one ``/search`` orders by — on the name
    or its bare form), then live before closed, then a legal person before
    its branch or local unit, then CKAN's own order."""
    def key(item: tuple[int, dict[str, Any]]) -> tuple[int, int, int, int]:
        position, rec = item
        return (
            _record_tier(query, rec),
            1 if _is_closed(rec) else 0,
            1 if str(rec.get("type") or "").strip() in _DEPENDENT_TYPES else 0,
            position,
        )

    ranked = sorted(enumerate(records), key=key)
    return [rec for _, rec in ranked[:_SEARCH_LIMIT]]


def _entity_url(regcode: str) -> str:
    return f"https://www.latvija.lv/lv/bizness/uznemumu-registrs/{regcode}"


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class UrLatviaAdapter(SourceAdapter):
    """Source adapter for the Latvian Register of Enterprises (UR)."""

    id = "ur_latvia"

    lookup_derivers = (
        LookupDeriver(frozenset({LV_RA_CODE}), "lv_regcode", normalise_regcode),
    )
    lookup_pass_legal_name = True


    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        live = get_settings().allow_live
        return SourceInfo(
            id=self.id,
            name="UR — Latvian Register of Enterprises",
            homepage="https://www.ur.gov.lv/en/",
            description=(
                "Latvian company data from the Register of Enterprises (UR), "
                "sourced via Latvia's open-data portal (data.gov.lv). "
                "Provides entity profiles, beneficial owners, officers, and "
                "shareholders for companies registered in Latvia."
            ),
            license="Open Government Data (PSI Directive)",
            attribution=(
                "Contains data from the Latvian Register of Enterprises (UR), "
                "open data published on data.gov.lv."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=live,
            is_national_register=True,
            country="LV",
        )

    # ------------------------------------------------------------------
    # Search — full text on the business register's name column
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        query = " ".join(query.split())
        if not query:
            return []

        cache_key = f"{_CACHE_NS}/search-v2/{_slug(query)}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_search(query)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            records = cached[0]
        else:
            async with build_client() as client:
                resp = await client.get(
                    search_url(query), timeout=_SEARCH_TIMEOUT_S
                )
            records = parse_search_response(resp)
            self._cache.put(cache_key, records)

        return [self._entity_hit(rec) for rec in rank_records(query, records)]

    # ------------------------------------------------------------------
    # Fetch — join all five datasets for a given regcode
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the full UR payload for a given Latvian registration number.

        ``hit_id`` is the 11-digit regcode (may arrive as a string from GLEIF).
        """
        regcode = normalise_regcode(hit_id)
        cache_key = f"{_CACHE_NS}/entity/{regcode}"

        if not self.info.live_available and not self._cache.has(cache_key):
            return self._stub_payload(regcode, legal_name)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        # Phase 262: a table that did not answer is ``None`` here, never ``[]``,
        # and a bundle built while any part failed is not cached — data.gov.lv
        # answering 502/503 once must not make a company read "none filed"
        # until the cache expires (AS "Remars-Rīga", 19 Nov 2026).
        failures: list[str] = []
        async with build_client() as client:
            entity = await self._fetch_entity(client, regcode, failures)
            if entity is None:
                reason = degradation.reason_for_failure(failures[-1] if failures else "")
                degradation.record(self.id, _ENTITY_FAILED_DETAIL, reason=reason)
                stub = self._stub_payload(regcode, legal_name)
                stub["read_failed"] = reason
                return stub
            hist_names = await self._name_history(client, regcode, failures)
            bowners = await self._fetch_rows(
                client, _RES_BOWNERS, "legal_entity_registration_number", regcode, failures
            )
            officers = await self._fetch_rows(
                client, _RES_OFFICERS, "at_legal_entity_registration_number", regcode, failures
            )
            members = await self._fetch_rows(
                client, _RES_MEMBERS, "at_legal_entity_registration_number", regcode, failures
            )

        tables = {
            "historical_names": hist_names,
            "beneficial_owners": bowners,
            "officers": officers,
            "members": members,
        }
        unread = [name for name, rows in tables.items() if rows is None]
        payload: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": regcode,
            "lv_regcode": regcode,
            "legal_name": legal_name,
            "entity": entity,
            **{name: rows or [] for name, rows in tables.items()},
            "is_stub": False,
        }
        if unread:
            payload["unread"] = unread
            degradation.record(
                self.id,
                _TABLES_FAILED_DETAIL.format(
                    tables=", ".join(_TABLE_WORDS[name] for name in unread)
                ),
                check=degradation.CHECK_SOURCE_READ,
                reason=degradation.reason_for_failure(failures[-1] if failures else ""),
            )
            return payload
        self._cache.put(cache_key, payload)
        return payload

    # ------------------------------------------------------------------
    # The History tab (the Estonia–Latvia emitters phase)
    # ------------------------------------------------------------------

    async def fetch_timeline_data(self, regcode: str, *, legal_name: str = "") -> dict[str, Any] | None:
        """The dated rows the Time Machine reconstructs Latvian events from.

        The lookup's five tables (one cached read serves both), plus the
        six event tables the lookup does not need. Read on the ``/history``
        path only — never per lookup. Returns ``None`` when not live or the
        register holds no such entity. ``unavailable`` names the tables
        that did not answer, so a thin history can be told from a refused
        one; a table that answered with no rows is simply empty. Raises
        :class:`UrLatviaUnavailable` when the company's own record could not
        be read, so the tab says the register did not answer.
        """
        regcode = normalise_regcode(regcode)
        payload = await self.fetch(regcode, legal_name=legal_name)
        if payload and payload.get("read_failed"):
            raise UrLatviaUnavailable(payload["read_failed"])
        if not payload or payload.get("is_stub"):
            return None
        if not payload.get("entity"):
            return None
        tables: dict[str, list[dict[str, Any]]] = {}
        unavailable: list[str] = list(payload.get("unread") or [])
        async with build_client() as client:
            for name, resource, field in (
                ("stockholders", _RES_STOCKHOLDERS, "at_legal_entity_registration_number"),
                ("liquidations", _RES_LIQUIDATIONS, "legal_entity_registration_number"),
                ("insolvency", _RES_INSOLVENCY, "debtor_registration_number"),
                ("suspensions", _RES_SUSPENSIONS, "legal_entity_registration_number"),
                ("securing_measures", _RES_SECURING, "legal_entity_registration_number"),
            ):
                rows = await self._fetch_rows(client, resource, field, regcode)
                if rows is None:
                    unavailable.append(name)
                tables[name] = rows or []
            # A reorganisation names the company on either side.
            reorg: list[dict[str, Any]] = []
            reorg_failed = False
            for field in ("source_entity_regcode", "final_entity_regcode"):
                rows = await self._fetch_rows(client, _RES_REORGANIZATIONS, field, regcode)
                if rows is None:
                    reorg_failed = True
                else:
                    reorg += rows
            if reorg_failed:
                unavailable.append("reorganizations")
            seen: set[Any] = set()
            distinct: list[dict[str, Any]] = []
            for r in reorg:
                if r.get("id") in seen:
                    continue
                seen.add(r.get("id"))
                distinct.append(r)
            tables["reorganizations"] = distinct
        return {
            "regcode": regcode,
            "entity": payload.get("entity") or {},
            "historical_names": payload.get("historical_names") or [],
            "beneficial_owners": payload.get("beneficial_owners") or [],
            "officers": payload.get("officers") or [],
            "members": payload.get("members") or [],
            **tables,
            "unavailable": unavailable,
        }

    async def _fetch_rows(
        self, client: Any, resource_id: str, filter_field: str, regcode: str,
        failures: list[str] | None = None,
    ) -> list[dict[str, Any]] | None:
        """Every row of a table matching the regcode, or ``None`` when the
        table did not answer — never a refusal read as an empty table.

        The filter value may be stored as integer or string in the CKAN
        datastore, so the integer is tried first, then the string. A failure's
        label is appended to ``failures`` when given.
        """
        try:
            filter_val: int | str = int(regcode)
        except (ValueError, TypeError):
            filter_val = regcode
        try:
            for value in (filter_val, str(regcode)) if isinstance(filter_val, int) else (filter_val,):
                filters = json.dumps({filter_field: value})
                url = (
                    f"{_CKAN_BASE}/datastore_search"
                    f"?resource_id={resource_id}"
                    f"&filters={quote(filters)}&limit={_PAGE}"
                )
                resp = await client.get(url)
                if not resp.is_success:
                    _log.warning(
                        "UR Latvia table did not answer: %s (resource=%s, regcode=%s)",
                        resp.status_code, resource_id, regcode,
                    )
                    if failures is not None:
                        failures.append(_failure_label(resp))
                    return None
                records = ((resp.json().get("result") or {}).get("records")) or []
                if records:
                    return records
            return []
        except Exception as exc:  # noqa: BLE001 — a failed table is said, never read as empty
            _log.warning("UR Latvia table failed: %s (resource=%s)", type(exc).__name__, resource_id)
            if failures is not None:
                failures.append(_failure_label(exc))
            return None

    async def _name_history(
        self, client: Any, regcode: str, failures: list[str] | None = None,
    ) -> list[dict[str, Any]] | None:
        """The entity's former names from UR's daily raw file (cached a day),
        falling back to the CKAN datastore copy — which stopped being
        reloaded on 24 March 2025 — when the download fails."""
        rows = await self._name_history_file(client)
        if rows is None:
            return await self._fetch_rows(client, _RES_HIST_NAMES, "regcode", regcode, failures)
        code = str(int(regcode)) if regcode.isdigit() else regcode
        return [r for r in rows if str(r.get("regcode") or "").lstrip("0") == code.lstrip("0")]

    async def _name_history_file(self, client: Any) -> list[dict[str, Any]] | None:
        """Every row of ``register_name_history.csv`` in the CKAN row shape
        (``regcode``, ``date_to`` as ``YYYY-MM-DDT00:00:00``, ``name``), or
        ``None`` when the file could not be read."""
        key = f"{_CACHE_NS}/name-history-file"
        cached = self._cache.get_payload(key, max_age_days=_NAME_HISTORY_CACHE_DAYS)
        text: str | None = cached[0] if cached is not None else None
        if text is None:
            try:
                resp = await client.get(_NAME_HISTORY_URL, timeout=60.0)
            except Exception as exc:  # noqa: BLE001
                _log.warning("UR Latvia name-history file failed: %s", type(exc).__name__)
                return None
            if not resp.is_success:
                _log.warning("UR Latvia name-history file: HTTP %s", resp.status_code)
                return None
            text = resp.text
            if not text.startswith("regcode;"):
                _log.warning("UR Latvia name-history file: unexpected header")
                return None
            self._cache.put(key, text)
        return _parse_name_history(text)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _fetch_entity(
        self, client: Any, regcode: str, failures: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """The entity's business-register row; ``{}`` when the register holds
        no such entity, ``None`` when it did not answer."""
        filters = json.dumps({"regcode": int(regcode)})
        url = (
            f"{_CKAN_BASE}/datastore_search"
            f"?resource_id={_RES_BUSINESS}"
            f"&filters={quote(filters)}&limit=1"
        )
        try:
            resp = await client.get(url)
            if not resp.is_success:
                _log.warning("UR Latvia entity fetch failed: %s (regcode=%s)", resp.status_code, regcode)
                if failures is not None:
                    failures.append(_failure_label(resp))
                return None
            data = resp.json()
        except Exception as exc:  # noqa: BLE001 — a failed read is said, never read as "no record"
            _log.warning("UR Latvia entity fetch failed: %s (regcode=%s)", type(exc).__name__, regcode)
            if failures is not None:
                failures.append(_failure_label(exc))
            return None
        records = (data.get("result") or {}).get("records") or []
        return records[0] if records else {}

    # ------------------------------------------------------------------
    # Hit factory
    # ------------------------------------------------------------------

    @staticmethod
    def _entity_hit(rec: dict[str, Any]) -> SourceHit:
        regcode = str(rec.get("regcode") or "")
        name = (rec.get("name") or regcode or "Unknown").strip()
        entity_type = (rec.get("type") or "").strip()
        type_label = _ENTITY_TYPES.get(entity_type, entity_type)
        closed = (rec.get("closed") or "").strip()
        terminated = rec.get("terminated")

        status_parts: list[str] = []
        if terminated or closed:
            status_parts.append("inactive")
        else:
            status_parts.append("active")
        if type_label:
            status_parts.append(type_label)

        return SourceHit(
            source_id="ur_latvia",
            hit_id=regcode,
            kind=SearchKind.ENTITY,
            name=name,
            summary=" · ".join(filter(None, [f"LV {regcode}"] + status_parts)),
            identifiers={"lv_regcode": regcode},
            raw=rec,
            is_stub=False,
        )

    # ------------------------------------------------------------------
    # Stub helpers
    # ------------------------------------------------------------------

    def _stub_search(self, query: str) -> list[SourceHit]:
        return [
            SourceHit(
                source_id=self.id,
                hit_id="40003567907",
                kind=SearchKind.ENTITY,
                name=f"{query} (stub)",
                summary=(
                    "Stub UR Latvia record — set OPENCHECK_ALLOW_LIVE=true to "
                    "query the live data.gov.lv CKAN portal."
                ),
                identifiers={"lv_regcode": "40003567907"},
                raw={"regcode": 40003567907, "name": f"{query} (stub)"},
            )
        ]

    def _stub_payload(self, regcode: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "hit_id": regcode,
            "lv_regcode": regcode,
            "legal_name": legal_name,
            "entity": {},
            "historical_names": [],
            "beneficial_owners": [],
            "officers": [],
            "members": [],
            "is_stub": True,
        }
