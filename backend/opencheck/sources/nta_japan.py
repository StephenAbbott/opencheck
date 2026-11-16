"""Japan — the National Tax Agency's Corporate Number (法人番号) register.

An **index-tier** register source, the ``cyprus_drcor`` shape: the lookup
reads a SQLite index of the NTA's monthly nationwide file (全件データ),
downloaded at boot from the ``nta-japan-index`` release asset
(``NTA_JAPAN_DB_URL``) onto the persistent disk and replaced when the asset
changes. The index is rebuilt monthly by
``.github/workflows/refresh-nta-japan-index.yml`` running
``scripts/build_nta_japan_index.py`` — the Corporate Number Publication Site
answers a datacentre address and its bulk files need no application ID.

A second, optional tier is the NTA's **Web-API** (``api.houjin-bangou.nta.go.jp``),
which needs an application ID (``NTA_JAPAN_APP_ID``). With an ID set, a number
the index does not hold — a company registered since the monthly cut, or any
number when there is no index — is read live, one ``num`` request, cached a
day. Without an ID that path is off and nothing changes.

What the register publishes
---------------------------
Every corporate number the NTA has assigned (5,850,621 on the 30 Oct 2026
file): the number, the name as registered (kanji), the kind of body, the
head-office address (in Japan, and abroad for a foreign company), the date
and cause of a closed registration record, the successor's number after a
merger, the latest change and its cause, the day the number was assigned,
and — where the body registered them, ~0.3 % of records — an English name
and address. **No officers, shareholders or beneficial owners**: Japan's
beneficial-owner list (実質的支配者リスト, held by the Legal Affairs Bureaus
since January 2022) is voluntary and not public.

The numbers, measured against GLEIF on 31 Oct 2026
---------------------------------------------------
* ``RA000412`` (Companies Registration, Legal Affairs Bureau, 2,564 ISSUED
  LEIs) files the **12-digit 会社法人等番号**, mostly hyphenated
  (``0210-01-021865``). The corporate number is that number with a check
  digit in front (``check_digit``); 2,550 of 2,564 resolve.
* ``RA001075`` (the Corporate Number Publication Site, 198 ISSUED LEIs) files
  the 13-digit corporate number itself; all resolve.
* ``RA000413`` (the FSA's register of funds, 7,824 ISSUED LEIs) files fund
  codes. They are **not** corporate numbers — a fund code with a check digit
  in front can land on an unrelated association — so it is never dispatched.
* No name gate (the JAR / GCIS precedent): the 119 resolved names that differ
  from GLEIF's are all the same company spelled another way (katakana or
  Latin, full width, a missing 株式会社). The RA-keyed deriver and the check
  digit are the safeguard.

Format traps
------------
* The full file is one row per number in its **latest** state only; there is
  no history in it. Name and address history is in the Web-API
  (``history=1``) or the 40 days of daily diff files.
* ``assignmentDate`` is when the number was assigned — 5 Oct 2015 for every
  body that existed then — and is never a founding date.
* ``hihyoji`` = 1 means the registered address no longer exists (a renamed
  street) and the record is left out of the site's search. It is not a status.

Licence: 公共データ利用規約 第1.0版 (PDL 1.0), stated compatible with CC BY 4.0.
Processed data is attributed 「国税庁法人番号公表サイト（国税庁）を加工して作成」.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import sqlite3
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import degradation, provenance
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..identifiers import japanese_corporate_number, jcn_check_digit
from ..secret_scrub import describe_exception
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.nta_japan import NtaJapanBundle

logger = logging.getLogger(__name__)

SOURCE_ID = "nta_japan"

#: GLEIF Registration Authority codes. ``RA000412`` files the 12-digit
#: 会社法人等番号, ``RA001075`` the 13-digit corporate number; both reach the
#: same record. ``RA000413`` (the FSA's fund register) is deliberately absent.
JP_COMPANY_REGISTRATION_RA = "RA000412"
JP_CORPORATE_NUMBER_RA = "RA001075"
JP_RA_CODES: frozenset[str] = frozenset({JP_COMPANY_REGISTRATION_RA, JP_CORPORATE_NUMBER_RA})
#: Named so a test can pin that nothing dispatches on it.
JP_FUND_RA = "RA000413"

JP_JCN_SCHEME = "JP-JCN"
JP_JCN_SCHEME_NAME = "Corporate Number (法人番号), National Tax Agency (Japan)"

#: The nationwide file's thirty columns, in order (リソース定義書 4.1, items
#: 7–36). The Web-API's CSV rows (Ver. 4) carry the same columns.
COLUMNS: tuple[str, ...] = (
    "sequenceNumber", "corporateNumber", "process", "correct", "updateDate",
    "changeDate", "name", "nameImageId", "kind", "prefectureName", "cityName",
    "streetNumber", "addressImageId", "prefectureCode", "cityCode", "postCode",
    "addressOutside", "addressOutsideImageId", "closeDate", "closeCause",
    "successorCorporateNumber", "changeCause", "assignmentDate", "latest",
    "enName", "enPrefectureName", "enCityName", "enAddressOutside", "furigana",
    "hihyoji",
)

#: 法人種別 — the kind of body, from the resource definition. The English is
#: OpenCheck's; the Japanese is the register's.
KINDS: dict[str, tuple[str, str]] = {
    "101": ("国の機関", "national government body"),
    "201": ("地方公共団体", "local public body"),
    "301": ("株式会社", "stock company (kabushiki kaisha)"),
    "302": ("有限会社", "special limited company (tokurei yūgen kaisha)"),
    "303": ("合名会社", "general partnership company (gōmei kaisha)"),
    "304": ("合資会社", "limited partnership company (gōshi kaisha)"),
    "305": ("合同会社", "limited liability company (gōdō kaisha)"),
    "399": ("その他の設立登記法人", "other registered corporation"),
    "401": ("外国会社等", "foreign company or other foreign body"),
    "499": ("その他", "other body (not registered by incorporation)"),
}
#: National organs and local public bodies are state bodies (Phase 240 keeps
#: state enterprises — a 株式会社 the state owns — as registered entities).
STATE_BODY_KINDS: frozenset[str] = frozenset({"101", "201"})
#: Kinds that are registered by incorporation and so carry a 会社法人等番号.
REGISTERED_KINDS: frozenset[str] = frozenset({"301", "302", "303", "304", "305", "399", "401"})

#: 登記記録の閉鎖等の事由 — why a registration record was closed. Every cause
#: ends the body's record: the class is ``terminal`` and ``closeDate`` the
#: dissolution date.
CLOSE_CAUSES: dict[str, tuple[str, str]] = {
    "01": ("清算の結了等", "liquidation completed"),
    "11": ("合併による解散等", "dissolved by merger"),
    # Commercial Registration Rules, art. 81: a dormant company's record.
    "21": ("登記官による閉鎖", "closed by the registrar"),
    "31": (
        "その他の清算の結了等",
        "liquidation completed or similar (a body not registered by incorporation)",
    ),
}

#: 処理区分 — what the latest change to the record was.
PROCESSES: dict[str, str] = {
    "01": "number assigned",
    "11": "name changed",
    "12": "address in Japan changed",
    "13": "address abroad changed",
    "21": "registration record closed",
    "22": "registration record restored",
    "71": "absorbed another company by merger",
    "72": "merger annulled",
    "81": "trade-name registration struck off",
    "99": "number withdrawn",
}

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

SITE_URL = "https://www.houjin-bangou.nta.go.jp/"
DOWNLOAD_URL = "https://www.houjin-bangou.nta.go.jp/download/zenken/"
TERMS_URL = "https://www.houjin-bangou.nta.go.jp/riyokiyaku/index.html"
PDL_URL = "https://www.digital.go.jp/resources/open_data/public_data_license_v1.0"
API_BASE = "https://api.houjin-bangou.nta.go.jp/4"

#: Required wherever the Web-API is used (Web-API terms, art. 6): "This
#: service is built on information obtained through the NTA Corporate Number
#: System Web-API, but its content is not guaranteed by the NTA."
API_NOTICE = (
    "このサービスは、国税庁法人番号システムWeb-API機能を利用して取得した情報を"
    "もとに作成しているが、サービスの内容は国税庁によって保証されたものではない。"
)

ATTRIBUTION = (
    "出典：国税庁法人番号公表サイト（国税庁）を加工して作成 "
    f"({SITE_URL}). Contains information from the National Tax Agency's "
    "Corporate Number Publication Site, processed by OpenCheck, under the "
    f"Public Data License (Version 1.0) ({PDL_URL})."
)

COVERAGE_NOT_IN_INDEX = (
    "This corporate number is not in the National Tax Agency's nationwide file "
    "as indexed. The company may be newer than the monthly file; a limited "
    "liability partnership (有限責任事業組合) or a sole proprietor has no "
    "corporate number at all."
)
COVERAGE_NOT_ASSIGNED = (
    "The National Tax Agency's Web-API holds no record for this corporate number."
)

#: The Web-API answer is cached this long on success.
_API_CACHE_DAYS = 1.0
_CACHE_NS = "nta_japan/num-v1"


# ---------------------------------------------------------------------------
# The number
# ---------------------------------------------------------------------------


#: The check digit and the 12 → 13 conversion live in ``identifiers`` so the
#: GLEIF mapper can write the same number without importing this module.
check_digit = jcn_check_digit
corporate_number = japanese_corporate_number


def normalise_corporate_number(raw: str) -> str:
    """The deriver's normaliser: the 13-digit number, or ValueError (the
    adapter is then skipped for that lookup, never sent a guess)."""
    number = corporate_number(raw)
    if number is None:
        raise ValueError(f"not a Japanese corporate number: {raw!r}")
    return number


def registration_number(number: str) -> str:
    """The 会社法人等番号 inside a corporate number, as GLEIF and the
    registry print it: ``0210-01-021865``."""
    body = number[1:]
    return f"{body[:4]}-{body[4:6]}-{body[6:]}"


def corporate_number_spellings(raw: str) -> tuple[str, ...]:
    """Every way GLEIF may store the number, for its exact-match filter: the
    13 digits (RA001075), the hyphenated and the bare 12 (RA000412). The
    input unchanged when it is neither."""
    number = corporate_number(raw)
    if number is None:
        return ((raw or "").strip(),)
    return (number, registration_number(number), number[1:])


# ---------------------------------------------------------------------------
# Reading the register's words
# ---------------------------------------------------------------------------


def kind_label(kind: str | None) -> str | None:
    entry = KINDS.get((kind or "").strip())
    return entry[1] if entry else None


def kind_japanese(kind: str | None) -> str | None:
    entry = KINDS.get((kind or "").strip())
    return entry[0] if entry else None


def close_cause_label(cause: str | None) -> str | None:
    entry = CLOSE_CAUSES.get((cause or "").strip())
    return entry[1] if entry else None


def status_class(record: dict[str, Any]) -> str:
    """``live`` / ``terminal`` / ``unknown`` for one record.

    A closed registration record is terminal whatever its cause. An open one
    is live, except where the latest change struck off the trade-name
    registration (``81``, eleven records) — that says nothing certain about
    the body, so it is unknown.
    """
    if (record.get("close_date") or "").strip() or (record.get("close_cause") or "").strip():
        return "terminal"
    if (record.get("process") or "").strip() == "81":
        return "unknown"
    return "live"


def row_from_csv(fields: list[str]) -> dict[str, Any] | None:
    """One row of the nationwide file (or a Web-API CSV row) as the index's
    record; None when it is not a row of that shape."""
    if len(fields) < 24:
        return None
    row = dict(zip(COLUMNS, (f.strip() for f in fields), strict=False))
    number = row.get("corporateNumber") or ""
    if corporate_number(number) != number:
        return None

    def _v(key: str) -> str | None:
        value = (row.get(key) or "").strip()
        return value or None

    return {
        "number": number,
        "name": _v("name"),
        "kind": _v("kind"),
        "prefecture": _v("prefectureName"),
        "city": _v("cityName"),
        "street": _v("streetNumber"),
        "post_code": _v("postCode"),
        "address_outside": _v("addressOutside"),
        "close_date": _v("closeDate"),
        "close_cause": _v("closeCause"),
        "successor": _v("successorCorporateNumber"),
        "process": _v("process"),
        "change_date": _v("changeDate"),
        "change_cause": _v("changeCause"),
        "update_date": _v("updateDate"),
        "assignment_date": _v("assignmentDate"),
        "en_name": _v("enName"),
        "en_prefecture": _v("enPrefectureName"),
        "en_city": _v("enCityName"),
        "en_address_outside": _v("enAddressOutside"),
        "hihyoji": 1 if (row.get("hihyoji") or "").strip() == "1" else 0,
    }


def search_text(value: str | None) -> str:
    """The form names are searched in: NFKC (full-width Latin and digits to
    half width, half-width katakana to full), case-folded, spaces removed."""
    text = unicodedata.normalize("NFKC", value or "").casefold()
    return re.sub(r"\s+", "", text)


def record_url(number: str) -> str:
    """The number's page on the Corporate Number Publication Site."""
    return f"https://www.houjin-bangou.nta.go.jp/henkorireki-johoto.html?selHouzinNo={number}"


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``NTA_JAPAN_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().nta_japan_db_file
    return Path(configured) if configured else data_root() / "nta_japan.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().nta_japan_db_file:
            logger.warning("nta_japan: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning(
            "nta_japan: index at %s has an unexpected schema (want %s); ignoring",
            path, INDEX_SCHEMA_VERSION,
        )
        conn.close()
        return None
    return conn


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        _SHARED["conn"] = _connect()
    return _SHARED.get("conn")


def reset_connection() -> None:
    """Forget the cached connection (after a download, or in tests)."""
    conn = _SHARED.pop("conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - best effort
            pass


def index_available() -> bool:
    """True when a readable index of the expected schema is present."""
    return _shared_conn() is not None


def read_meta() -> dict[str, str]:
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def cut_date(meta: dict[str, str] | None = None) -> datetime | None:
    """The day the NTA made the file — the date in its name."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: the file's date is the register's
    clock, ``built_at`` is ours (Phase 314)."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"NTA Corporate Number nationwide file, {cut}"
            if cut else "NTA Corporate Number nationwide file"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state this
    module models (the source is announced only with the Web-API set)."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        record_asset_stamp,
    )
    from ..entity_pages import (
        read_meta as read_file_meta,
    )

    settings = get_settings()
    path = db_path()
    url = settings.nta_japan_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {SOURCE_ID: f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {SOURCE_ID: f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {SOURCE_ID: f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("nta_japan: asset check/download failed: %s", describe_exception(exc))
        return {SOURCE_ID: f"failed: {describe_exception(exc)}"}


def entity_record(number: str) -> dict[str, Any] | None:
    """The index's row for one corporate number; None when it does not hold
    it (or there is no index). ``sqlite3.Error`` propagates."""
    conn = _shared_conn()
    if conn is None or not number.isdigit():
        return None
    row = conn.execute("SELECT * FROM entity WHERE number = ?", (int(number),)).fetchone()
    if row is None:
        return None
    record = dict(row)
    record["number"] = f"{int(record['number']):013d}"
    return record


def _name_in_index(number: str | None) -> str | None:
    """A successor's name, when the index holds it."""
    if not number:
        return None
    try:
        record = entity_record(number)
    except sqlite3.Error:
        return None
    return (record or {}).get("name")


# ---------------------------------------------------------------------------
# The Web-API (needs an application ID)
# ---------------------------------------------------------------------------


class NtaApiUnavailable(Exception):
    """The Web-API did not answer; ``reason`` is in the degradation vocabulary."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def api_configured() -> bool:
    """An application ID is set and live calls are allowed."""
    settings = get_settings()
    return bool((settings.nta_japan_api_key or "").strip()) and bool(settings.allow_live)


def parse_api_csv(text: str) -> list[dict[str, Any]]:
    """The rows of a Web-API ``type=02`` (CSV, Unicode) answer.

    The first line is the answer's header — last update day, count, divide
    number, divide size — and every following line one record in the
    nationwide file's columns. A count of 0 is a definitive "no record".
    """
    reader = csv.reader(io.StringIO(text.lstrip("﻿")))
    header = next(reader, None)
    if not header or len(header) < 2 or not header[1].strip().isdigit():
        raise NtaApiUnavailable("upstream_error", "The Web-API answer had no header line.")
    rows = [r for r in (row_from_csv(f) for f in reader if f) if r is not None]
    if int(header[1]) and not rows:
        raise NtaApiUnavailable("upstream_error", "The Web-API answer named records but held none.")
    return rows


async def api_records(number: str, *, history: bool = False) -> list[dict[str, Any]]:
    """The Web-API's records for one corporate number.

    ``history=False`` (the lookup's fallback) asks for the latest record
    only; ``history=True`` asks for every change the NTA holds, oldest first —
    the call the History tab will make. Raises ``NtaApiUnavailable`` for
    anything but an answer; an empty list is the NTA saying it holds nothing.
    Caches nothing (the caller caches a success).
    """
    settings = get_settings()
    app_id = (settings.nta_japan_api_key or "").strip()
    if not app_id:
        raise NtaApiUnavailable("not_configured", "No Web-API application ID is set.")
    params = {"id": app_id, "number": number, "type": "02", "history": "1" if history else "0"}
    try:
        async with build_client() as client:
            response = await client.get(f"{API_BASE}/num", params=params)
    except Exception as exc:  # noqa: BLE001
        raise NtaApiUnavailable(
            degradation.reason_for_failure(type(exc).__name__),
            f"The Web-API did not answer ({describe_exception(exc)}).",
        ) from None
    if response.status_code != 200:
        raise NtaApiUnavailable(
            degradation.reason_for_failure(f"HTTP {response.status_code}"),
            f"The Web-API answered HTTP {response.status_code}.",
        )
    return parse_api_csv(response.text)


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class NtaJapanAdapter(SourceAdapter):
    """Source adapter for the NTA Corporate Number register (index + Web-API)."""

    id = SOURCE_ID

    lookup_derivers = (
        LookupDeriver(JP_RA_CODES, "jp_jcn", normalise_corporate_number),
    )
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        attribution = ATTRIBUTION
        if api_configured():
            attribution = f"{ATTRIBUTION} {API_NOTICE}"
        return SourceInfo(
            id=self.id,
            name="National Tax Agency — Corporate Number (Japan)",
            homepage=SITE_URL,
            description=(
                "Japan's register of corporate numbers (法人番号), published by "
                "the National Tax Agency and read from a monthly index of its "
                "nationwide file: the name as registered, the kind of body, the "
                "head-office address, the date and cause of a closed record and "
                "a merger's successor, the latest change, and an English name "
                "where the body registered one. No officers, shareholders or "
                "beneficial owners are published."
            ),
            license="PDL-1.0",
            attribution=attribution,
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available() or api_configured(),
            is_national_register=True,
            country="JP",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file or a configured Web-API."""
        return index_available() or api_configured()

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY or not (query or "").strip():
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        number = corporate_number(query)
        try:
            if number:
                record = entity_record(number)
                records = [record] if record else []
            else:
                text = search_text(query)
                # The trigram index needs three characters.
                if len(text) < 3:
                    return []
                ids = [
                    r[0] for r in conn.execute(
                        "SELECT rowid FROM name_fts WHERE name_fts MATCH ? LIMIT 400",
                        ('"' + text.replace('"', '""') + '"',),
                    )
                ]
                if not ids:
                    return []
                marks = ",".join("?" * len(ids))
                records = [
                    dict(r) for r in conn.execute(
                        f"SELECT * FROM entity WHERE number IN ({marks})", ids
                    )
                ]
                for r in records:
                    r["number"] = f"{int(r['number']):013d}"
                records.sort(key=lambda r: (
                    search_text(r.get("name")) != text and search_text(r.get("en_name")) != text,
                    len(r.get("name") or ""),
                    r["number"],
                ))
                records = records[:10]
        except sqlite3.Error:
            return []
        if not records:
            return []
        declare_snapshot()
        return [self._search_hit(r) for r in records]

    @staticmethod
    def _search_hit(record: dict[str, Any]) -> SourceHit:
        number = record["number"]
        segments = [f"JP {number}"]
        kind = kind_label(record.get("kind"))
        if kind:
            segments.append(kind)
        if status_class(record) == "terminal":
            segments.append("closed")
        return SourceHit(
            source_id=SOURCE_ID,
            hit_id=number,
            kind=SearchKind.ENTITY,
            name=record.get("name") or f"JP {number}",
            summary=" · ".join(segments),
            identifiers={"jp_jcn": number},
            raw={"number": number},
            is_stub=False,
            liveness="snapshot",
        )

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def _stub(self, number: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index and no Web-API, a failed read, or not a
        number. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "number": number,
            "name": legal_name or "",
            "legal_name": legal_name,
            "link": DOWNLOAD_URL,
            "is_stub": True,
        }

    def _note_card(self, number: str, legal_name: str, note: str, **extra: Any) -> dict[str, Any]:
        """The register answered and holds no such number (the JAR / INPI
        shape: ``is_stub`` False, ``not_found`` True)."""
        bundle = self._stub(number, legal_name)
        bundle.update({"is_stub": False, "not_found": True, "coverage_note": note, **extra})
        return bundle

    def _bundle(
        self, record: dict[str, Any], legal_name: str, *, tier: str, cut: str | None
    ) -> dict[str, Any]:
        number = record["number"]
        successor = record.get("successor")
        bundle: dict[str, Any] = {
            "source_id": self.id,
            **{k: record.get(k) for k in (
                "name", "kind", "prefecture", "city", "street", "post_code",
                "address_outside", "close_date", "close_cause", "process",
                "change_date", "change_cause", "update_date", "assignment_date",
                "en_name", "en_prefecture", "en_city", "en_address_outside",
            )},
            "number": number,
            "registration_number": (
                registration_number(number) if record.get("kind") in REGISTERED_KINDS else None
            ),
            "legal_name": legal_name,
            "hihyoji": bool(record.get("hihyoji")),
            "successor": successor,
            "successor_name": _name_in_index(successor),
            "status_class": status_class(record),
            "tier": tier,
            "cut": cut,
            "link": record_url(number),
            "is_stub": False,
        }
        if not bundle["name"]:
            bundle["name"] = legal_name or f"JP {number}"
        if tier == "api":
            bundle["api_notice"] = API_NOTICE
        validate_raw(self.id, NtaJapanBundle, bundle)
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the record for one corporate number.

        ``hit_id`` is what GLEIF files — the 12-digit 会社法人等番号 (with or
        without hyphens) under RA000412, the 13-digit number under RA001075 —
        or a search hit's 13 digits. ``legal_name`` is GLEIF's name, used for
        the card when the register holds nothing; it gates nothing.
        """
        number = corporate_number(hit_id)
        if number is None:
            logger.warning("nta_japan: not a corporate number: %r", hit_id)
            return self._stub(str(hit_id or "").strip(), legal_name)

        meta: dict[str, str] = {}
        if index_available():
            try:
                record = entity_record(number)
                meta = read_meta()
            except sqlite3.Error as exc:
                degradation.record(
                    self.id,
                    "The Japanese corporate-number index could not be read; the card "
                    "shows nothing from the register. This is not a confirmation "
                    "that the register holds nothing.",
                    reason=degradation.reason_for_failure(type(exc).__name__),
                    check="source_read",
                )
                return self._stub(number, legal_name)
            if record is not None:
                # Declared only once a row is in hand: a miss is not a read.
                declare_snapshot(meta)
                return self._bundle(record, legal_name, tier="index", cut=meta.get("cut"))
            if not api_configured():
                return self._note_card(
                    number, legal_name, COVERAGE_NOT_IN_INDEX, cut=meta.get("cut"),
                )
        elif not api_configured():
            return self._stub(number, legal_name)

        return await self._fetch_api(number, legal_name, index_cut=meta.get("cut"))

    async def _fetch_api(
        self, number: str, legal_name: str, *, index_cut: str | None
    ) -> dict[str, Any]:
        """The Web-API tier: a number the index lacks, or every number when
        there is no index. A success is cached a day; a definitive "no
        record" is remembered as an absence; a failure is a note card and a
        degradation, never cached."""
        cache_key = f"{_CACHE_NS}/{number}"
        cached = self._cache.get_payload(cache_key, max_age_days=_API_CACHE_DAYS)
        if cached is not None:
            payload = cached[0]
            if isinstance(payload, dict):
                return self._bundle(payload, legal_name, tier="api", cut=None)
            return self._note_card(number, legal_name, COVERAGE_NOT_ASSIGNED, index_cut=index_cut)
        try:
            rows = await api_records(number)
        except NtaApiUnavailable as exc:
            degradation.record(
                self.id,
                f"{exc.detail} The card shows nothing from the register for this number.",
                reason=exc.reason,
                check="source_read" if index_cut else "source_fetch",
            )
            if index_cut:
                return self._note_card(
                    number, legal_name, COVERAGE_NOT_IN_INDEX, cut=index_cut,
                )
            return self._stub(number, legal_name)
        record = next((r for r in reversed(rows) if r["number"] == number), None)
        if record is None:
            self._cache.put_absent(cache_key)
            return self._note_card(number, legal_name, COVERAGE_NOT_ASSIGNED, index_cut=index_cut)
        self._cache.put(cache_key, record)
        return self._bundle(record, legal_name, tier="api", cut=None)
