"""Bulgarian Commercial Register (Търговски регистър) — an index of the daily
open-data files (the Bulgaria Commercial Register ticket, 18 Nov 2026).

The Registry Agency (Агенция по вписванията) keeps the Commercial Register
and the Register of Non-Profit Legal Entities in one system and publishes
every day's entries as an XML file on data.egov.bg, under **CC0**:

    https://data.egov.bg/data/view/2df0c2af-e769-4397-be33-fcbe269806f3

Why an index, and why it is built by hand
-----------------------------------------

data.egov.bg answers 403 to every address outside Bulgaria (geo-blocking,
not a datacentre rule: a Bulgarian VPN exit reads it). So the files are
fetched on a Bulgarian connection by ``data/bg/fetch_bg_tr.py``, packed by
``scripts/build_bg_tr_index.py`` (standard library only) and shipped as the
``bg-tr-index`` release asset by ``scripts/refresh_bg_tr_index.sh``;
:func:`warm_index` downloads it at boot — the JAR / ЄДР arrangement. It can
never be a workflow.

What the index can and cannot say
---------------------------------

The daily files are **deltas**: one day's field entries. The entries for
2008–2020 are listed on the portal but hold no file, so the index is a replay
of every entry since **1 January 2021**. A list field (managers, partners,
beneficial owners…) is entered whole, so a list entered since 2021 is
complete; a field nobody has touched since 2021 is not in the files at all.
So:

* a company with no entry since 2021 is not in the index — a note card that
  says so, never "not registered";
* an absent field is unknown, never "none": the finding never says "no
  beneficial owners", only how many the files hold;
* "live" means no deletion, liquidation or insolvency was entered since
  2021, and the status text says exactly that.

Beneficial owners (Stephen, 18 Nov 2026)
----------------------------------------

The register publishes beneficial owners (``ActualOwners``) in its CC0 open
data although Art. 63b of the AML law restricted access to them in June 2025.
OpenCheck republishes an owner's name, nationality, country of residence and
the nature and extent of the control — never a birth date, home address,
passport field, or the register's personal-number hash as an identifier. The
builder drops all of them; :class:`~.schemas.bg_tr.BgTrBundle` refuses a
party carrying one.

Nothing here touches the response :class:`~opencheck.cache.Cache` (the Phase
325 rule).
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import degradation, provenance
from ..config import get_settings
from ..findings import human_date
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.bg_tr import BgTrBundle

logger = logging.getLogger(__name__)

#: GLEIF Registration Authorities that file a Bulgarian company's ЕИК:
#: the Commercial Register (RA000065), the BULSTAT register (RA000730 — the
#: same number) and the register of public-benefit non-profits (RA000731).
#: RA000066 (the Financial Supervision Commission) files funds' codes, which
#: are not in the Commercial Register, and is never dispatched.
BG_RA_CODES: frozenset[str] = frozenset({"RA000065", "RA000730", "RA000731"})

#: The table layout this adapter reads; the builder writes the same value.
INDEX_SCHEMA_VERSION = "2"

DATASET_URL = "https://data.egov.bg/data/view/2df0c2af-e769-4397-be33-fcbe269806f3"

#: Where the open data begins. Every sentence about absence names it.
OPEN_DATA_SINCE = "1 January 2021"

COVERAGE_NOT_IN_INDEX = (
    "This ЕИК has no entry in the Commercial Register's open data, which "
    "holds the entries made since 1 January 2021 only: a company with no "
    "change entered since then is not in it. It may still be on the register."
)

_EIK_RE = re.compile(r"^\d{9}(\d{4})?$")


# ---------------------------------------------------------------------------
# The register's vocabularies
# ---------------------------------------------------------------------------

#: The Deed's ``LegalForm`` code → English. Shown beside the register's own
#: words where the files carry them.
LEGAL_FORMS: dict[str, str] = {
    "ET": "sole trader",
    "SD": "general partnership",
    "KD": "limited partnership",
    "OOD": "private limited company",
    "EOOD": "single-member private limited company",
    "AD": "joint-stock company",
    "EAD": "single-member joint-stock company",
    "IAD": "special-purpose investment company",
    "IEAD": "single-member special-purpose investment company",
    "KDA": "partnership limited by shares",
    "K": "cooperative",
    "KCHT": "branch of a foreign trader",
    "TPPD": "public enterprise",
    "TPPO": "public enterprise",
    "TPP": "public enterprise",
    "EUIE": "European economic interest grouping",
    "DEUIE": "branch of a European economic interest grouping",
    "ED": "European company",
    "EKD": "European cooperative society",
    "LEKD": "European cooperative society",
    "ASSOC": "association",
    "FOUND": "foundation",
    "BFLE": "branch of a foreign non-profit legal entity",
    "CC": "community centre (читалище)",
    "DPK": "variable-capital company",
    "EDPK": "single-member variable-capital company",
}

#: Forms with shares or capital contributions, where partners hold a share.
CAPITAL_FORMS = frozenset({"OOD", "EOOD", "AD", "EAD", "IAD", "IEAD", "KDA", "DPK", "EDPK"})

#: Forms with no members' capital: the parties of these are never owners.
NON_PROFIT_FORMS = frozenset({"ASSOC", "FOUND", "BFLE", "CC"})

#: Search-rank words (``search_rank``) for a status class.
_RANK_WORDS = {"terminal": "deleted", "pending": "in liquidation"}


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def normalise_eik(value: str | int) -> str:
    """The digits of a ЕИК: GLEIF files it bare; a caller may add ``BG``."""
    text = str(value or "").strip().upper()
    if text.startswith("BG"):
        text = text[2:]
    return re.sub(r"\D", "", text)


def eik_check_digit(first8: str) -> int:
    """The ninth digit of a ЕИК: weights 1–8 mod 11, then 3–10, else 0."""
    digits = [int(c) for c in first8]
    rem = sum(d * w for d, w in zip(digits, range(1, 9), strict=True)) % 11
    if rem == 10:
        rem = sum(d * w for d, w in zip(digits, range(3, 11), strict=True)) % 11
        if rem == 10:
            rem = 0
    return rem


def is_valid_eik(value: str) -> bool:
    """A nine-digit ЕИК with a correct check digit, or a 13-digit unit code
    whose first nine are one."""
    code = normalise_eik(value)
    if not _EIK_RE.match(code):
        return False
    return eik_check_digit(code[:8]) == int(code[8])


def record_url(eik: str) -> str:
    """The register's public page for a company's current state."""
    return f"https://portal.registryagency.bg/CR/Reports/ActiveConditionTabResult?uic={eik}"


def form_label(code: str | None, text: str | None = None) -> str | None:
    """The register's legal-form words with an English gloss, or the gloss."""
    gloss = LEGAL_FORMS.get(str(code or "").strip())
    words = (text or "").strip()
    if words and gloss:
        return f"{gloss} ({words})"
    return gloss or words or None


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    from ..cache import data_root

    configured = get_settings().bg_tr_db_file
    return Path(configured) if configured else data_root() / "bg_tr.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().bg_tr_db_file:
            logger.warning("bg_tr: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("bg_tr: index at %s has an unexpected schema; ignoring", path)
        conn.close()
        return None
    return conn


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        _SHARED["conn"] = _connect()
    return _SHARED.get("conn")


def reset_connection() -> None:
    conn = _SHARED.pop("conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - best effort
            pass


def index_available() -> bool:
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
    """The latest entry day the files hold — the register's clock."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    meta = read_meta() if meta is None else meta
    cut = human_date(meta.get("cut"))
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"Commercial Register open data, entries {OPEN_DATA_SINCE} to {cut}"
            if cut else "Commercial Register open data"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed. Never raises: no index is a state this module models."""
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
    url = settings.bg_tr_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"bg_tr": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"bg_tr": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"bg_tr": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("bg_tr: asset check/download failed: %s", exc)
        return {"bg_tr": f"failed: {exc}"}


def company_record(eik: str) -> dict[str, Any] | None:
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM company WHERE uic = ?", (eik,)).fetchone()
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------
# From the index row to the bundle
# ---------------------------------------------------------------------------

#: Party list fields, in the order they are shown.
PARTY_FIELDS: tuple[str, ...] = (
    "PhysicalPersonTrader", "SoleCapitalOwner", "Partners", "UnlimitedLiabilityPartners",
    "Managers", "Representatives", "Representatives101", "Representatives103",
    "ChairMan", "BoardOfDirectors", "BoardOfManagers", "BoardOfManagers2",
    "SupervisingBoard", "ControllingBoard", "ManagementBodies12d",
    "Liquidators", "Procurators",
)

#: Keys a party may carry into the bundle — an allowlist (the schema refuses
#: anything that names a personal identifier or an address).
PARTY_KEYS = frozenset({
    "name", "kind", "country", "country_name", "eik", "foreign", "legal_form",
    "key", "position", "role", "field", "share", "since", "residence",
    "residence_name", "rights", "owned_rights", "percent",
})


def _party(raw: dict[str, Any], field: str, since: str | None) -> dict[str, Any]:
    out = {k: v for k, v in raw.items() if k in PARTY_KEYS}
    out["field"] = field
    if since:
        out["since"] = since
    return out


def _seat_text(seat: dict[str, Any] | None) -> str | None:
    if not isinstance(seat, dict):
        return None
    order = ("Street", "StreetNumber", "HousingEstate", "Block", "Entrance", "Floor",
             "Apartment", "Settlement", "Municipality", "District", "PostCode",
             "ForeignPlace", "Country")
    parts: list[str] = []
    for key in order:
        value = str(seat.get(key) or "").strip()
        if value and value not in parts:
            parts.append(value)
    return ", ".join(parts) or None


def bundle_from_row(row: dict[str, Any], meta: dict[str, str]) -> dict[str, Any]:
    """The adapter's bundle for one ``company`` row."""
    fields: dict[str, Any] = json.loads(row.get("data") or "{}")

    def value(tag: str) -> Any:
        return (fields.get(tag) or {}).get("value")

    def since(tag: str) -> str | None:
        return (fields.get(tag) or {}).get("entry_date")

    parties: list[dict[str, Any]] = []
    for tag in PARTY_FIELDS:
        for raw in value(tag) or []:
            if isinstance(raw, dict) and raw.get("name"):
                parties.append(_party(raw, tag, since(tag)))
    owners = [
        _party(raw, "ActualOwners", since("ActualOwners"))
        for raw in value("ActualOwners") or []
        if isinstance(raw, dict) and raw.get("name")
    ]
    seat = value("Seat") if isinstance(value("Seat"), dict) else None
    legal_form = value("LegalForm") or {}
    eik = row["uic"]
    return {
        "source_id": "bg_tr",
        "hit_id": eik,
        "eik": eik,
        "name": row.get("name") or "",
        "transliteration": row.get("transliteration"),
        "legal_form_code": row.get("legal_form"),
        "legal_form_text": (legal_form.get("text") if isinstance(legal_form, dict) else None) or None,
        "seat": seat,
        "seat_text": _seat_text(seat),
        "seat_country": (seat or {}).get("CountryCode") or None,
        "capital": value("Funds"),
        "way_of_representation": value("WayOfRepresentation"),
        "foreign_trader": value("ForeignTraders"),
        "parties": parties,
        "beneficial_owners": owners,
        "beneficial_owners_since": since("ActualOwners"),
        "status_class": row.get("status_class"),
        "status": row.get("status_text"),
        "status_since": row.get("status_since"),
        "deed_status": row.get("deed_status"),
        "registered_on": row.get("registered_on"),
        "first_entry": row.get("first_entry"),
        "last_entry": row.get("last_entry"),
        "cut": meta.get("cut"),
        "open_data_since": meta.get("first_entry"),
        "link": record_url(eik),
        "dataset_url": DATASET_URL,
        "is_stub": False,
    }


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class BgTrAdapter(SourceAdapter):
    """Source adapter for Bulgaria's Commercial Register."""

    id = "bg_tr"

    lookup_derivers = (LookupDeriver(BG_RA_CODES, "bg_eik", normalise_eik),)
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Commercial Register — Bulgaria (Registry Agency)",
            homepage="https://portal.registryagency.bg/CR/",
            description=(
                "Bulgaria's Commercial Register and Register of Non-Profit "
                "Legal Entities, kept by the Registry Agency and published as "
                "daily open data: name, legal form, seat, capital, managers, "
                "boards, partners, sole owners, procurators, liquidators and "
                "beneficial owners. Read from an index of every entry made "
                "since 1 January 2021 — a field not changed since then is not "
                "in the open data."
            ),
            license="CC0-1.0",
            attribution=(
                "Contains data from the Commercial Register and Register of "
                "Non-Profit Legal Entities, published by the Registry Agency "
                "of Bulgaria (Агенция по вписванията) on data.egov.bg under CC0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="BG",
        )

    def covers_lei(self, lei: str) -> bool:
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        text = (query or "").strip()
        if kind != SearchKind.ENTITY or not text:
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        digits = normalise_eik(text)
        try:
            if _EIK_RE.match(digits) and digits == text.replace(" ", "").upper().removeprefix("BG"):
                rows = conn.execute("SELECT * FROM company WHERE uic = ?", (digits,)).fetchall()
            else:
                like = f"%{text.casefold()}%"
                rows = conn.execute(
                    "SELECT * FROM company WHERE name_fold LIKE ? OR lower(transliteration) LIKE ?"
                    " ORDER BY (name_fold = ?) DESC, status_class = 'terminal' LIMIT 10",
                    (like, like, text.casefold()),
                ).fetchall()
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(row)) for row in rows]

    @staticmethod
    def _search_hit(row: dict[str, Any]) -> SourceHit:
        eik = row["uic"]
        segments = [f"BG {eik}"]
        form = LEGAL_FORMS.get(str(row.get("legal_form") or ""))
        if form:
            segments.append(form)
        rank = _RANK_WORDS.get(str(row.get("status_class") or ""))
        if rank:
            segments.append(rank)
        return SourceHit(
            source_id="bg_tr",
            hit_id=eik,
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"BG {eik}",
            summary=" · ".join(segments),
            identifiers={"bg_eik": eik},
            raw={"eik": eik},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, eik: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id, "hit_id": eik, "eik": eik, "name": legal_name or "",
            "link": DATASET_URL, "is_stub": True,
        }

    def _not_in_index(self, eik: str, legal_name: str, meta: dict[str, str]) -> dict[str, Any]:
        """The index answered and holds no entry for the ЕИК — a note card."""
        return {
            "source_id": self.id, "hit_id": eik, "eik": eik, "name": legal_name or "",
            "link": record_url(eik), "is_stub": False, "not_found": True,
            "coverage_note": COVERAGE_NOT_IN_INDEX, "cut": meta.get("cut"),
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """The index's record for one ЕИК (GLEIF's ``registeredAs``)."""
        eik = normalise_eik(hit_id)
        if not _EIK_RE.match(eik):
            logger.warning("bg_tr: not an EIK %r", hit_id)
            return self._stub(str(hit_id or ""), legal_name)
        if _shared_conn() is None:
            return self._stub(eik, legal_name)
        try:
            row = company_record(eik)
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Bulgarian Commercial Register index could not be read; the "
                "card shows nothing from the register. This is not a "
                "confirmation that the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(eik, legal_name)
        meta = read_meta()
        if row is None:
            return self._not_in_index(eik, legal_name, meta)
        declare_snapshot(meta)
        bundle = bundle_from_row(row, meta)
        if not bundle["name"]:
            bundle["name"] = legal_name or ""
        validate_raw("bg_tr", BgTrBundle, bundle)
        return bundle
