"""Romania — ONRC, the National Trade Register Office (data.gov.ro).

Registered source over a pre-built SQLite index. Build it with
``scripts/build_onrc_romania_index.py`` and point ``ONRC_ROMANIA_DB_FILE`` at
it; **without that file the source is not announced at all** (``covers_lei``),
because a card that says it has nothing still counts itself among the sources
that answered.

The index is **scoped to the GLEIF Romanian LEI population** by default: the
register holds 2,855,557 companies, 9,033 Romanian LEI records exist, and every
ONRC lookup is downstream of one of them, so the shipped index is 4.3 MB rather
than 1.2 GB.

This module also owns the **Romanian identifier grammar** — ``normalise_cui``,
``normalise_registration_number``, ``to_new_format`` and ``resolve_cui`` — used
by ``anaf_romania`` to turn whatever GLEIF filed into something ANAF can be
asked about. The grammar lives here because the formats are ONRC's, and
``anaf_romania`` imports from this module rather than the other way round.

Why bulk and not live
---------------------
ONRC has no free API. The only official open channel is a monthly dump on
data.gov.ro: six ``^``-delimited CSVs, 1.6 GB in total, republished under a new
set of resource UUIDs each month (so the adapter resolves them through CKAN's
``package_show`` and never hardcodes a download URL). Measured against the
2 September 2026 export: 4,219,081 companies, 3,689,931 legal-representative
rows.

Why the index matters even for the live path
--------------------------------------------
ANAF — the live, keyless channel — is keyed on the **CUI** (fiscal code) and
nothing else. GLEIF's ``registeredAs`` for a Romanian entity holds a CUI only
about half the time; the rest carry an ONRC **registration number** (the
``J`` number), which ANAF cannot be queried with. Measured over 1,800 live RO
LEI records on 14 September 2026:

=========================================  =====  ======
path                                       count   share
=========================================  =====  ======
CUI direct — no index needed                 860   47.8%
J-number, exact match in the index           783   43.5%
J-number, recovered by format normalisation  122    6.8%
unresolved                                    35    1.9%
=========================================  =====  ======

So the index is not an enrichment; it is half the reach. Without it a Romanian
lookup resolves for 47.8% of LEI holders, and which half you land in depends on
nothing more principled than which identifier the entity's LEI issuer chose to
file.

The two registration-number formats
-----------------------------------
ONRC renumbered the register. The old ``X{county}/{seq}/{year}`` and the new
14-character ``X{year}{seq:06d}{county:02d}{check}`` are **the same number**,
and the conversion is deterministic::

    J40/15812/2017  ->  J2017015812405   CUI 38218844  IMAFLUX DESIGN SRL
    J23/6841/2022   ->  J2022006841234   CUI 46959989  SAFEGATE ADVISORS S.R.L.
    J20/3/2023      ->  J2023000003206   CUI 47397820  NEW TECH IMOB S.R.L.

The final digit is a check digit, so the join is on the **13-character prefix**.
Both spellings occur in GLEIF, and a register full of one and a GLEIF record
holding the other is the ordinary case, not an edge case — normalising is worth
6.8 points of reach on its own.

    **The prefix index must be restricted to** ``^[A-Z]\\d{13}$``. An old-format
    code such as ``J40/13003/1991`` is *also* fourteen characters long, and
    truncating those to thirteen collapses ``…/1991``, ``…/1992`` and
    ``…/1993`` onto one key. Measuring collisions without that restriction
    reports 50,437 of them and makes the whole approach look unsound; with it,
    862,979 new-format codes yield 862,950 distinct prefixes — 29 collisions,
    0.003%. The name check in ``resolve_cui`` covers those.

Traps, each measured against the real export
--------------------------------------------
* **CUI is not a primary key.** 95,158 CUIs appear on more than one row
  (branches filed against a parent's fiscal code), and 86,426 rows (2.0%) carry
  no CUI at all. ``COD_INMATRICULARE`` is the key — 4,214,297 distinct values
  over 4,219,081 rows.
* **Some dates of birth carry a spurious time**, e.g. ``19/06/1967 14:45:06``.
  That is a data-entry artefact, not a precision claim; ``parse_ro_date``
  drops it rather than parsing a datetime.
* ``EUID`` is ``'ROONRC.' + COD_INMATRICULARE`` on 100.00% of rows. It is
  derived, not stored.
* The CSVs are UTF-8 **with a BOM** and delimited by ``^``, not a comma.

Scope: corporate forms only
---------------------------
About 1.35M of the 4.22M rows are sole traders — PFA, II, PF, AF, IF — natural
persons trading under a business name. They are excluded at index-build time
(see ``CORPORATE_FORMS``): none of them will ever be the subject of an LEI
lookup, and modelling a person as an entity to hold their own trading name
asserts something the register does not.

Licence: CC BY 4.0, Oficiul Național al Registrului Comerțului, via data.gov.ro.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any

from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.onrc_romania import OnrcRomaniaBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities
# ---------------------------------------------------------------------------
# SIX authorities register Romanian LEIs, and — unusually — **what
# ``registeredAs`` means depends on which one it is**. Verified against 1,200
# live RO legal-address LEI records, 14 September 2026:
#
#   RA000497  ONRC Trade Register          1,140  CUI *or* J-number, no marker
#   RA000719  Tax Payer Register (MF)         51  bare CUI, 51/51
#   RA000718  NGO register (MJ)                6  "4325/A/2003", 6/6
#   RA000498  ASF instruments registry         3  "CSC06FDIR/120135", 3/3
#
# So RA000719 is the *cleanest* key of the four — a fiscal code, directly
# queryable, never needing the index — while RA000497, the dominant one, is
# ambiguous and has to be sniffed. RA000718 and RA000498 are court- and
# regulator-held registers whose numbers are not fiscal codes and cannot be
# resolved to one: an association's CUI is simply not on its LEI record. They
# are deliberately absent below, so the deriver raises and the source is
# skipped rather than guessing.
RO_ONRC_RA_CODE: str = "RA000497"
RO_TAX_RA_CODE: str = "RA000719"

#: The authorities whose ``registeredAs`` can reach a CUI. See above for the
#: two that cannot.
RO_RA_CODES: frozenset[str] = frozenset({RO_ONRC_RA_CODE, RO_TAX_RA_CODE})

_DATASET_SEARCH_URL = "https://data.gov.ro/dataset?organization=onrc"

#: Said when the index is present but holds no row for this company.
#:
#: Three reasons it happens, all honest answers rather than failures: the
#: company is a sole trader (an ``F``-prefixed number, excluded by design); it
#: was registered after the monthly snapshot; or it was issued an LEI after the
#: index was scoped to the LEI population. Measured over the 8,677 dispatchable
#: Romanian LEI records on 15 September 2026, 199 land here — 105 sole traders,
#: 92 absent from the September export, 2 unparseable.
COVERAGE_NOT_INDEXED = (
    "This company is not in the indexed extract of the Trade Register. The "
    "index is built from ONRC's monthly open-data dump and covers registered "
    "companies, not sole traders, so a very recent registration or a "
    "natural-person business will not appear."
)
_PORTAL_URL = "https://portal.onrc.ro/"

#: Legal forms kept in the index. Everything else in ``FORMA_JURIDICA`` is a
#: sole-trader form (PFA, II, PF, AF, IF) — a natural person, not an entity.
#: Counted on the 2 September 2026 export: SRL 2,789,302, SA 33,758,
#: SNC 28,418, CA 3,406, SC 2,360, SCS 1,486.
CORPORATE_FORMS: frozenset[str] = frozenset(
    {"SRL", "SA", "SNC", "SCS", "SCA", "CA", "SC", "RA", "OC", "GIE", "SE"}
)

#: ``FORMA_JURIDICA`` values excluded as natural persons. Named explicitly so
#: the exclusion is auditable rather than implied by the allowlist above.
SOLE_TRADER_FORMS: frozenset[str] = frozenset({"PFA", "II", "PF", "AF", "IF"})


# ---------------------------------------------------------------------------
# Identifier grammar
# ---------------------------------------------------------------------------
# A Romanian CUI (cod unic de înregistrare) is 2–10 digits, sometimes written
# with the "RO" VAT prefix. It is NOT zero-padded — 361 and 0000361 are the
# same taxpayer to ANAF, which accepts an integer.
_CUI_RE = re.compile(r"^(?:RO)?\s*0*(\d{2,10})$", re.I)

#: Old format: a letter, a 1–2 digit county code, a sequence, a 4-digit year.
_REG_OLD_RE = re.compile(r"^([A-Z])(\d{1,2})/(\d+)/(\d{4})$")

#: New format: a letter and exactly 13 digits (year, 6-digit sequence,
#: 2-digit county, 1 check digit).
_REG_NEW_RE = re.compile(r"^([A-Z])(\d{13})$")


def normalise_cui(raw: str) -> str:
    """Return a Romanian CUI with no ``RO`` prefix and no leading zeros.

    Raises ``ValueError`` when ``raw`` is not a CUI. Leading zeros are dropped
    rather than padded: ANAF's v9 service takes ``cui`` as a JSON **number**,
    so ``0000361`` and ``361`` are the same query and the unpadded form is the
    one that round-trips.
    """
    m = _CUI_RE.match(str(raw or "").strip())
    if not m:
        raise ValueError(f"not a Romanian CUI: {raw!r}")
    return m.group(1)


def is_cui(raw: str) -> bool:
    """True when ``raw`` reads as a CUI rather than a registration number."""
    try:
        normalise_cui(raw)
    except ValueError:
        return False
    return True


def normalise_registration_number(raw: str) -> str:
    """Return an ONRC registration number in its filed spelling, uppercased.

    Accepts both formats and normalises only whitespace and case — the filed
    spelling is preserved because it is what the register itself publishes and
    what ``registeredAs`` holds. Use ``to_new_format`` to cross between them.

    Raises ``ValueError`` when ``raw`` is neither format. That includes the
    NGO register's ``4325/A/2003`` and the ASF's ``CSC06FDIR/120135``, which
    are real identifiers from other Romanian registers and deliberately not
    accepted here.
    """
    text = re.sub(r"\s+", "", str(raw or "")).upper()
    if _REG_OLD_RE.match(text) or _REG_NEW_RE.match(text):
        return text
    raise ValueError(f"not an ONRC registration number: {raw!r}")


def is_registration_number(raw: str) -> bool:
    """True when ``raw`` reads as an ONRC registration number."""
    try:
        normalise_registration_number(raw)
    except ValueError:
        return False
    return True


def to_new_format(raw: str) -> str | None:
    """Convert an old-format registration number to its new-format prefix.

    ``J40/15812/2017`` → ``J2017015812405``'s first thirteen characters,
    ``J2017015812 40``. Returns the **13-character prefix**, not a full code:
    the fourteenth digit is a check digit this function does not compute, and
    the prefix is what the index is keyed on.

    Returns ``None`` for anything that is not an old-format number — including
    a number already in the new format, which needs no conversion.
    """
    m = _REG_OLD_RE.match(re.sub(r"\s+", "", str(raw or "")).upper())
    if not m:
        return None
    letter, county, seq, year = m.groups()
    return f"{letter}{year}{int(seq):06d}{int(county):02d}"


def prefix_for(raw: str) -> str | None:
    """The 13-character new-format prefix for a number in **either** format.

    This is the join key, and it has to be derivable from both sides or the
    old↔new bridge only half works — which is exactly how it shipped.

    ``to_new_format`` answers "convert this old number", so it returns None for
    a number already in the new format. Used on its own it therefore keyed the
    index on old-format rows (where the prefix is redundant, since the exact
    match on ``registration_number`` already finds them) and left new-format
    rows with no prefix at all — and a new-format row is precisely what an
    incoming old-format GLEIF number needs to reach.

    Measured against the whole Romanian LEI population on 15 September 2026:
    **1,474 of 3,823 J-number-keyed LEIs (38.6%) failed to resolve**, including
    every worked example in the research — IMAFLUX DESIGN SRL
    (``J40/15812/2017`` → ``J2017015812405``), CALLINVEST, SAFEGATE ADVISORS,
    NEW TECH IMOB. Deriving the prefix from both formats recovers 1,356 of
    them and takes coverage from 83.0% to 97.7% of dispatchable RO LEIs.

    Both directions matter: GLEIF holds the old spelling for ~28% of Romanian
    records and the new one for ~23%, while ONRC files 2.19M rows old and
    0.66M new, so either side can be the one that needs converting.
    """
    number = re.sub(r"\s+", "", str(raw or "")).upper()
    if _REG_NEW_RE.match(number):
        return number[:13]
    return to_new_format(number)


def parse_ro_date(raw: str | None) -> str | None:
    """Parse a ``DD/MM/YYYY`` ONRC date to ISO ``YYYY-MM-DD``, or None.

    **Drops a trailing time.** A minority of ``DATA_NASTERE`` values are filed
    as ``19/06/1967 14:45:06`` — a data-entry artefact of whatever system wrote
    the row, not a claim that someone was born at a quarter to three. Parsing
    it as a datetime would carry that noise into a BODS ``birthDate``.

    Returns None rather than guessing on anything that does not parse, so a
    malformed date is visibly absent instead of silently wrong.
    """
    text = str(raw or "").strip()
    if not text:
        return None
    text = text.split()[0]
    m = re.fullmatch(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})", text)
    if not m:
        return None
    day, month, year = (int(p) for p in m.groups())
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


# ---------------------------------------------------------------------------
# Name agreement
# ---------------------------------------------------------------------------
_LEGAL_SUFFIXES = re.compile(
    r"\b(S\s?R\s?L|SRL|S\s?A|SA|SNC|SCS|SCA|IFN|SE|PFA|II|IF|AF)\b"
)


def normalise_name(raw: str) -> str:
    """Fold a Romanian company name for comparison.

    Strips diacritics, punctuation and legal-form suffixes. Romanian registers
    write the same company as ``Formosa SRL``, ``FORMOSA S.R.L.`` and
    ``FORMOSA S.R.L``; GLEIF holds a fourth spelling.
    """
    text = unicodedata.normalize("NFKD", str(raw or ""))
    text = text.encode("ascii", "ignore").decode().upper()
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = _LEGAL_SUFFIXES.sub(" ", text)
    return " ".join(text.split())


def names_agree(left: str, right: str) -> bool:
    """True when two company names agree well enough to accept a join.

    Deliberately a prefix comparison on the folded forms rather than a
    similarity score: this gate exists to catch a *wrong company*, not to
    measure how similar two strings are. Measured on the live sample, it
    accepts 1,532 of 1,545 direct matches and 118 of 122 normalisation-
    recovered ones; the rejections are companies GLEIF still holds under a
    former name, which is a fact worth surfacing rather than smoothing over.
    """
    a, b = normalise_name(left), normalise_name(right)
    if not a or not b:
        return False
    return a[:16] == b[:16]


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """Where the index lives: ``ONRC_ROMANIA_DB_FILE`` or the data root default.

    One function, so the reader and the boot downloader cannot disagree about
    the path — see ``_connect`` for what happened when they did.
    """
    from ..cache import data_root

    configured = get_settings().onrc_romania_db_file
    return Path(configured) if configured else data_root() / "onrc_romania.sqlite"


def _connect() -> sqlite3.Connection | None:
    """Open the ONRC index, or None when it is missing.

    Resolves through ``db_path`` rather than reading the setting directly, so
    this and ``warm_index`` cannot disagree about where the file is. They did
    at first: the boot download wrote to the data-root default while this read
    ``ONRC_ROMANIA_DB_FILE`` and returned None when it was unset, so a
    perfectly good downloaded index was ignored and the source stayed dark.
    """
    path = db_path()
    if not path.exists():
        # Only worth a warning when someone named a path that isn't there.
        # Unset-and-absent is the ordinary no-index state, not a fault.
        if get_settings().onrc_romania_db_file:
            logger.warning("onrc_romania: index not found at %s", path)
        return None
    conn = sqlite3.connect(str(path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _shared_conn() -> sqlite3.Connection | None:
    """A process-wide connection, so ``resolve_cui`` need not reopen the file."""
    if "conn" not in _SHARED:
        _SHARED["conn"] = _connect()
    return _SHARED["conn"]


def reset_connection() -> None:
    """Drop the cached connection. Tests point the setting at a new file."""
    conn = _SHARED.pop("conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - close is best effort
            pass


def index_available() -> bool:
    """True when an ONRC index is configured and present."""
    return _shared_conn() is not None


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset is not the one on disk; keep it otherwise.

    The MEIP / PSC-graph rule, and for the same reason: Render's filesystem is
    ephemeral, the index is a build artifact rather than a repo file, and a
    4.3 MB gzipped asset is cheap enough to re-fetch on a cold start. Never
    raises — no index is a state this module already models (``covers_lei``
    returns False and the source is not announced), not a reason to fail boot.
    """
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.onrc_romania_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"onrc_romania": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"onrc_romania": f"already present: {path}"}
            logger.info(
                "onrc_romania: the release asset is not the one on disk; replacing"
            )
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {
            "onrc_romania": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"
        }
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("onrc_romania: asset check/download failed: %s", exc)
        return {"onrc_romania": f"failed: {exc}"}


def company_row(registration_number: str) -> dict[str, Any] | None:
    """Return the ``company`` row for a registration number, in either format.

    Tries the filed spelling first, then — for an old-format number — the
    13-character new-format prefix. Returns None when the index is absent or
    holds no such company.
    """
    conn = _shared_conn()
    if conn is None:
        return None
    try:
        number = normalise_registration_number(registration_number)
    except ValueError:
        return None
    try:
        cur = conn.execute(
            "SELECT * FROM company WHERE registration_number = ?", (number,)
        )
        row = cur.fetchone()
        if row is not None:
            return dict(row)
        prefix = prefix_for(number)
        if prefix is None:
            return None
        cur = conn.execute(
            "SELECT * FROM company WHERE registration_prefix = ?", (prefix,)
        )
        rows = cur.fetchall()
    except sqlite3.OperationalError as exc:
        logger.warning("onrc_romania: company query failed: %s", exc)
        return None
    if len(rows) != 1:
        # 29 prefixes in 862,979 are shared. Refuse rather than pick one.
        return None
    return dict(rows[0])


def resolve_cui(registration_number: str, *, legal_name: str = "") -> str | None:
    """Resolve an ONRC registration number to a CUI, or None.

    ``legal_name``, when given, must agree with the registrar's name for the
    company (see ``names_agree``) or the resolution is refused. GLEIF's
    ``registeredAs`` is reliable — 99.2% of joins agree on the name — but the
    failures are not random: they are companies GLEIF still holds under a
    former name, and accepting one would attach a live lookup to a company the
    user did not ask about.
    """
    row = company_row(registration_number)
    if row is None:
        return None
    cui = (row.get("cui") or "").strip()
    if not cui:
        return None
    if legal_name and not names_agree(legal_name, row.get("name") or ""):
        logger.info(
            "onrc_romania: refusing %s — GLEIF name %r vs register name %r",
            registration_number,
            legal_name,
            row.get("name"),
        )
        return None
    return cui


def _rows(conn: sqlite3.Connection, table: str, number: str) -> list[dict[str, Any]]:
    try:
        cur = conn.execute(
            f"SELECT * FROM {table} WHERE registration_number = ? ORDER BY seq",
            (number,),
        )
    except sqlite3.OperationalError as exc:
        logger.warning("onrc_romania: query on %s failed: %s", table, exc)
        return []
    return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class OnrcRomaniaAdapter(SourceAdapter):
    """Source adapter for the Romanian Trade Register (local SQLite index)."""

    id = "onrc_romania"

    #: The deriver lives on ``anaf_romania`` — it owns the RA codes and the
    #: shape-sniffing — and this adapter reuses its derived key, the
    #: ``rpvs_slovakia`` → ``rpo_slovakia`` pattern. Declaring a second deriver
    #: for the same key would make two adapters race to define it.
    lookup_dispatch_keys = ("ro_fiscal_or_reg_id",)
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="ONRC — Oficiul Național al Registrului Comerțului (Romania)",
            homepage="https://www.onrc.ro/",
            description=(
                "Romanian Trade Register, published as a monthly open-data "
                "dump on data.gov.ro. Carries the company, its registration "
                "number in both of the register's formats, legal form, "
                "registered office, status, and its legal representatives "
                "with their filed roles and dates of birth. No beneficial "
                "ownership: shareholders remain behind the paid certificate."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains information from the National Trade Register Office "
                "(Oficiul Național al Registrului Comerțului), published on "
                "data.gov.ro under a Creative Commons Attribution 4.0 licence."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="RO",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index **file**, not on the LEI.

        The pipeline calls this before announcing a source. Without it, a
        deployment with no index would announce ONRC for every Romanian lookup
        and then show a card saying it had nothing — counting itself in
        "N of N sources answered" while answering nothing. That is the failure
        the ``cac_nigeria`` / ``eiti_soe`` gate exists to prevent.

        The argument is ignored on purpose: this adapter is keyed on the
        registration number, and resolving an LEI to one would mean a GLEIF
        call inside an applicability check. Whether the *company* is in the
        index is answered honestly by ``fetch`` returning a stub, which the hit
        builder renders as a coverage note — the KvK/INPI shape.
        """
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY:
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        try:
            cur = conn.execute(
                "SELECT registration_number, cui, name FROM company "
                "WHERE name LIKE ? LIMIT 10",
                (f"%{query}%",),
            )
        except sqlite3.OperationalError:
            return []
        return [
            SourceHit(
                source_id=self.id,
                hit_id=row["registration_number"],
                kind=SearchKind.ENTITY,
                name=row["name"],
                summary=f"RO · {row['registration_number']}",
                identifiers=(
                    {"ro_cui": row["cui"]} if row["cui"] else {}
                ),
                raw={"registration_number": row["registration_number"]},
                is_stub=False,
                liveness="snapshot",
            )
            for row in cur.fetchall()
        ]

    def _stub(self, number: str, legal_name: str) -> dict[str, Any]:
        return {
            "source_id": self.id,
            "registration_number": number,
            "cui": None,
            "name": legal_name or "",
            "company": None,
            "representatives": [],
            "legal_name": legal_name,
            "link": _DATASET_SEARCH_URL,
            "is_stub": True,
            "coverage_note": COVERAGE_NOT_INDEXED,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the ONRC bundle for one registration number.

        ``hit_id`` is an ONRC registration number in either format.
        """
        conn = _shared_conn()
        try:
            number = normalise_registration_number(hit_id)
        except ValueError:
            return self._stub(str(hit_id or ""), legal_name)
        if conn is None:
            return self._stub(number, legal_name)

        company = company_row(number)
        if company is None:
            return self._stub(number, legal_name)

        # Representatives are filed against the register's own spelling of the
        # number, which may not be the spelling GLEIF holds.
        filed = company.get("registration_number") or number
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "registration_number": filed,
            "cui": (company.get("cui") or None),
            "name": company.get("name") or legal_name or f"RO {filed}",
            "company": company,
            "representatives": _rows(conn, "representative", filed),
            "legal_name": legal_name,
            "link": _PORTAL_URL,
            "is_stub": False,
        }
        validate_raw("onrc_romania", OnrcRomaniaBundle, bundle)
        return bundle
