"""Ukraine — ЄДР, the Unified State Register of Legal Entities (data.gov.ua).

An **index-tier** register source, the ``jar_lithuania`` shape: the lookup
reads a SQLite index of the weekly bulk export, downloaded at boot from the
``edr-ukraine-index`` release asset (``EDR_UKRAINE_DB_URL``) onto the
persistent disk and replaced when the asset changes. Dispatch is gated on
the index being present (``covers_lei``); a code the index does not hold is
a note card, never a failure. Build the index with
``scripts/refresh_edr_ukraine_index.sh`` (download + build + upload) or
``scripts/build_edr_ukraine_index.py`` by hand.

Why bulk and not live
---------------------
There is no government API for the ЄДР. The only official channel is a weekly
ZIP of XML on data.gov.ua: ``UO.zip`` is 328 MB compressed and **3.16 GB of XML**
holding 2,017,706 legal entities (measured against the 8 Sep 2026 export). The
whole file streams through ``iterparse`` in 113 s at 209 MB peak RSS, so the
cost is storage, not parsing — which is why the index is built once, offline.
The download itself sits behind a Cloudflare rule that refuses datacentre
addresses (HTTP 403 from the cloud and, like Lithuania's register, from a
GitHub runner) and answers a home connection, so the index is built
off-datacentre and shipped in, never rebuilt by a workflow.

What the register publishes
---------------------------
Unusually for a commercial register, the ЄДР files **beneficial owners** as a
first-class element: 533,958 entities carry a named UBO (690,421 records), and
a further 117,266 record a *stated reason* for the absence of one. Access to
the open data was suspended after the full-scale invasion in February 2022 and
restored on 19 January 2026 under Law 4576-IX, which also withholds precise
locations, addresses and KVED activity codes for the duration of martial law
and one year after. **Nothing here reconstructs a withheld field**: the absence
of an address is the publisher's decision, not a gap for OpenCheck to fill.

Format traps, each measured against the real export
---------------------------------------------------
* The XML is declared ``windows-1251`` — the *schema* zip is UTF-8, the data
  file is not. ``iterparse`` honours the declaration; anything that assumes
  UTF-8 gets mojibake.
* **EDRPOU is not a primary key.** 24,885 codes appear more than once (26,678
  extra rows), almost always a ``припинено`` (terminated) record beside a live
  one, and 150 records carry a blank code. The builder dedupes, preferring
  ``зареєстровано``.
* EDRPOU is occasionally fewer than 8 digits and is zero-padded here to match
  GLEIF ``registeredAs``.
* Founder holdings are **hryvnia amounts, not percentages** — see
  ``parse_founder``.
* **The index holds the ownership graph, not the whole register** (the
  ``graph`` tier): every live entity with a beneficial-ownership record, a
  corporate founder or a state authority, plus every entity a founder list
  points at. A live company with none of those, or a terminated one nothing
  points at, is "not in the index" — the note card says so, and says what
  the index holds.

The four repeating elements (``BENEFICIARIES``, ``FOUNDERS``, ``SIGNERS``,
``MEMBERS``) are all ``xs:string`` in the register's own schema: semicolon-
delimited text, not structured XML. The parsers below are deliberately the
only place that text is interpreted, so the index stays dumb and the reading
can change without a 3 GB rebuild.

Licence: CC BY 4.0, Ministry of Justice of Ukraine, via data.gov.ua.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from .. import degradation, provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.edr_ukraine import EdrUkraineBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities
# ---------------------------------------------------------------------------
# THREE authorities register Ukrainian LEIs and **all three carry the 8-digit
# EDRPOU in ``registeredAs``** (verified against all 303 live UA legal-address
# LEI records, 14 Sep 2026: RA000567 238, RA001027 34, RA001026 11, the rest
# unassigned). Keying on the register's own code alone would silently miss 15%
# of Ukrainian LEIs — the Hong Kong / Singapore shape. Treat this as a set, as
# ``zefix`` does with ``CH_RA_CODES``, never as a single constant.
UA_RA_CODES: frozenset[str] = frozenset(
    {
        "RA000567",  # ЄДР — Unified State Register (State Enterprise IRC)
        "RA001027",  # National Bank of Ukraine
        "RA001026",  # National Securities and Stock Market Commission
    }
)
UA_EDR_RA_CODE: str = "RA000567"

DATASET_URL = "https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7"
_DATASET_URL = DATASET_URL
_SEARCH_URL = "https://usr.minjust.gov.ua/content/free-search"

#: ``meta.schema_version`` the reader accepts. Bump when a table or column
#: the adapter reads changes; an index of another version is ignored (with a
#: warning) rather than read wrongly.
INDEX_SCHEMA_VERSION = "1"

#: Said on the card when the index answered and holds no row for the code.
COVERAGE_NOT_IN_INDEX = (
    "This EDRPOU code is not in OpenCheck's index of the ЄДР open data. The "
    "index holds the register's ownership graph — every live entity with a "
    "beneficial-ownership record, a corporate founder or a state authority, "
    "and every entity a founder list points at — so a live company with none "
    "of those, a terminated company, or a company newer than the export is "
    "not in it. This is not a statement that the register holds no record."
)

#: ``STAN`` value for a live registration. Every other value is some flavour of
#: terminated, in-termination, cancelled or bankrupt.
ACTIVE_STAN = "зареєстровано"

#: The register's ``STAN`` values with an English gloss, for the card's
#: sentence. An unlisted value is shown in the register's own words.
STATUS_GLOSSES: dict[str, str] = {
    "зареєстровано": "Registered",
    "припинено": "Terminated",
    "в стані припинення": "In termination",
    "порушено справу про банкрутство": "Bankruptcy proceedings opened",
    "порушено справу про банкрутство (санація)": "Bankruptcy proceedings opened (rehabilitation)",
    "скасовано": "Cancelled",
    "архівний": "Archived",
}

#: The commonest ``OPF`` (legal form) values, glossed. An unlisted form is
#: shown as the register writes it, in sentence case.
FORM_GLOSSES: dict[str, str] = {
    "ТОВАРИСТВО З ОБМЕЖЕНОЮ ВІДПОВІДАЛЬНІСТЮ": "limited liability company (ТОВ)",
    "АКЦІОНЕРНЕ ТОВАРИСТВО": "joint-stock company (АТ)",
    "ПУБЛІЧНЕ АКЦІОНЕРНЕ ТОВАРИСТВО": "public joint-stock company (ПАТ)",
    "ПРИВАТНЕ АКЦІОНЕРНЕ ТОВАРИСТВО": "private joint-stock company (ПрАТ)",
    "ПРИВАТНЕ ПІДПРИЄМСТВО": "private enterprise (ПП)",
    "ДЕРЖАВНЕ ПІДПРИЄМСТВО": "state enterprise (ДП)",
    "КОМУНАЛЬНЕ ПІДПРИЄМСТВО": "municipal enterprise (КП)",
    "ТОВАРИСТВО З ДОДАТКОВОЮ ВІДПОВІДАЛЬНІСТЮ": "additional liability company (ТДВ)",
    "ПОВНЕ ТОВАРИСТВО": "general partnership",
    "КОМАНДИТНЕ ТОВАРИСТВО": "limited partnership",
    "ГРОМАДСЬКА ОРГАНІЗАЦІЯ": "civic organisation (ГО)",
    "БЛАГОДІЙНА ОРГАНІЗАЦІЯ": "charitable organisation",
    "ДОЧІРНЄ ПІДПРИЄМСТВО": "subsidiary enterprise",
    "ІНОЗЕМНЕ ПІДПРИЄМСТВО": "foreign enterprise",
    "ФЕРМЕРСЬКЕ ГОСПОДАРСТВО": "farm enterprise",
    "ОБСЛУГОВУЮЧИЙ КООПЕРАТИВ": "service cooperative",
    "ОБ'ЄДНАННЯ СПІВВЛАСНИКІВ БАГАТОКВАРТИРНОГО БУДИНКУ": "apartment-building co-owners' association (ОСББ)",
}


def status_label(stan: str | None) -> str | None:
    """The register's status, glossed in English where known."""
    text = (stan or "").strip()
    if not text:
        return None
    return STATUS_GLOSSES.get(text.lower(), text)


def form_label(opf: str | None) -> str | None:
    """The register's legal-form words, glossed where known."""
    text = (opf or "").strip()
    if not text:
        return None
    gloss = FORM_GLOSSES.get(text.upper())
    if gloss:
        return gloss
    return text[0].upper() + text[1:].lower()


# ---------------------------------------------------------------------------
# Field grammar
# ---------------------------------------------------------------------------
# BENEFICIARY:
#   NAME; громадянство: X; <influence>; [відсоток частки - N];
#   [відсоток частки (непрямий вплив) - N]; [інший характер та міра впливу: …]
# or, where no UBO is filed:
#   причина відсутності: <reason>
#
# 684,256 of 690,421 named records have exactly four fields, 5,906 have five,
# 258 have six, and exactly ONE is malformed. A plain ``split(";")`` is
# sufficient; the pre-2022 format needed ML because beneficial ownership was
# buried in free-text FOUNDERS, and that is no longer true.

_ABSENCE_PREFIX = "причина відсутності"
_CITIZENSHIP_PREFIX = "громадянство"
_OTHER_INFLUENCE_PREFIX = "інший характер та міра впливу"

#: The register's closed vocabulary of influence types, in its own words.
INFLUENCE_DIRECT = "Прямий вирішальний вплив"
INFLUENCE_INDIRECT = "Непрямий вирішальний вплив"
INFLUENCE_BOTH = "Прямий та непрямий вирішальний вплив"

_INFLUENCE_MAP: dict[str, str] = {
    INFLUENCE_DIRECT.lower(): "direct",
    INFLUENCE_INDIRECT.lower(): "indirect",
    INFLUENCE_BOTH.lower(): "both",
}

# "відсоток частки - N" (direct) and "відсоток частки (непрямий вплив) - N".
# The parenthetical form must be tried FIRST — the direct pattern would
# otherwise match the shared "відсоток частки" prefix of the indirect one.
_PCT_INDIRECT = re.compile(
    r"відсоток\s+частки\s*\(непрямий\s+вплив\)\s*[-–—:]?\s*([\d.,]+)", re.I
)
_PCT_DIRECT = re.compile(r"відсоток\s+частки\s*(?!\()\s*[-–—:]?\s*([\d.,]+)", re.I)

# FOUNDER: NAME; [EDRPOU]; [громадянство: X]; розмір частки - N,NN грн.
_FOUNDER_AMOUNT = re.compile(
    r"розмір\s+частки\s*[-–—:]?\s*([\d\s  .,]+?)\s*грн", re.I
)
_BARE_CODE = re.compile(r"^\d{6,10}$")
_NUMBER = re.compile(r"[\d\s  .,]+")


def normalise_edrpou(raw: str) -> str:
    """Return an 8-digit EDRPOU, or "" when there are no digits.

    The register occasionally files fewer than 8 digits; GLEIF's
    ``registeredAs`` is consistently 8, so pad rather than compare ragged.
    """
    digits = re.sub(r"\D", "", str(raw or "").strip())
    if not digits:
        return ""
    return digits.zfill(8)[-8:] if len(digits) <= 8 else digits


def _to_float(raw: str | None) -> float | None:
    """Parse a Ukrainian decimal ("3 620,10") to a float, or None."""
    if not raw:
        return None
    cleaned = (
        str(raw)
        .replace(" ", "")
        .replace(" ", "")
        .replace(" ", "")
        .replace(",", ".")
    )
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_beneficiary(raw: str) -> dict[str, Any]:
    """Parse one ``BENEFICIARY`` string.

    Returns ``{"kind": "absence", "reason_text": …}`` for a filed reason, or
    ``{"kind": "named", "name", "citizenship", "influence", "pct_direct",
    "pct_indirect", "note"}`` for a declared UBO. A string that cannot be read
    as either returns ``{"kind": "unparsed", "raw": …}`` rather than guessing —
    there is exactly one such record in the register and it should stay
    visible, not be silently coerced into a person.
    """
    text = (raw or "").strip()
    if not text:
        return {"kind": "unparsed", "raw": text}
    if text.lower().startswith(_ABSENCE_PREFIX):
        return {
            "kind": "absence",
            "reason_text": text.split(":", 1)[-1].strip() if ":" in text else "",
            "raw": text,
        }

    parts = [p.strip() for p in text.split(";") if p.strip()]
    if len(parts) < 3:
        return {"kind": "unparsed", "raw": text}

    citizenship: str | None = None
    influence: str | None = None
    note: str | None = None
    for part in parts[1:]:
        low = part.lower()
        if low.startswith(_CITIZENSHIP_PREFIX):
            citizenship = part.split(":", 1)[-1].strip() or None
        elif low.startswith(_OTHER_INFLUENCE_PREFIX):
            note = part.split(":", 1)[-1].strip() or None
        elif low in _INFLUENCE_MAP:
            influence = _INFLUENCE_MAP[low]

    m_ind = _PCT_INDIRECT.search(text)
    # Blank the indirect clause before looking for the direct one, so the
    # shared "відсоток частки" prefix cannot be double-counted.
    direct_hunting_ground = _PCT_INDIRECT.sub(" ", text)
    m_dir = _PCT_DIRECT.search(direct_hunting_ground)

    return {
        "kind": "named",
        "name": parts[0],
        "citizenship": citizenship,
        "influence": influence,
        "pct_direct": _to_float(m_dir.group(1)) if m_dir else None,
        "pct_indirect": _to_float(m_ind.group(1)) if m_ind else None,
        "note": note,
        "raw": text,
    }


def parse_founder(raw: str) -> dict[str, Any]:
    """Parse one ``FOUNDER`` string.

    A founder is a ``засновник`` — a participant holding equity, **not** an
    officer. The holding is filed as a hryvnia capital contribution
    (``розмір частки - 134493517,83 грн.``), never as a percentage, so
    ``amount_uah`` is a money amount and has no home in a BODS ``share``
    without the authorised capital to divide it by (see
    ``edr_ukraine_shares`` in the mapper).

    ``code`` is present only where the founder is itself a legal entity.
    """
    text = (raw or "").strip()
    if not text:
        return {"name": "", "code": None, "citizenship": None, "amount_uah": None}
    parts = [p.strip() for p in text.split(";") if p.strip()]
    code: str | None = None
    citizenship: str | None = None
    for part in parts[1:]:
        if _BARE_CODE.match(part):
            code = normalise_edrpou(part)
        elif part.lower().startswith(_CITIZENSHIP_PREFIX):
            citizenship = part.split(":", 1)[-1].strip() or None
    m = _FOUNDER_AMOUNT.search(text)
    return {
        "name": parts[0] if parts else "",
        "code": code,
        "citizenship": citizenship,
        "amount_uah": _to_float(m.group(1)) if m else None,
        "raw": text,
    }


def parse_role_holder(raw: str) -> dict[str, Any]:
    """Parse one ``SIGNER`` or ``MEMBER`` string into name + role text.

    SIGNERS carry the head of the legal entity and anyone who may act for it
    (``керівник``, ``представник``, with the powers spelled out); MEMBERS carry
    members of governing bodies (``Голова Правління``, ``Член Ради``). Both are
    ``name; <role and qualifiers>``; the role half is kept verbatim because the
    register writes it freely and translating it would invent precision.
    """
    text = (raw or "").strip()
    if not text:
        return {"name": "", "role": None}
    parts = [p.strip() for p in text.split(";") if p.strip()]
    return {
        "name": parts[0] if parts else "",
        "role": "; ".join(parts[1:]) or None,
        "raw": text,
    }


def parse_capital(raw: str) -> float | None:
    """Parse ``AUTHORIZED_CAPITAL`` to a number of hryvnia, or None."""
    m = _NUMBER.search(str(raw or ""))
    return _to_float(m.group(0)) if m else None


# ---------------------------------------------------------------------------
# Absence reasons — BODS missing-information modelling (data-standard #389)
# ---------------------------------------------------------------------------
# Every one of the 117,266 absence records carries a stated reason; the field
# is literally "причина відсутності". Classified on the real corpus, with the
# field label and its boilerplate restatement stripped first — the word
# "відсутності" appears in almost every reason as part of the label, and
# matching on it naively claims everything.
#
# The BODS reason codes are the v0.4 camelCase values already used by
# ``_ch_psc_statement_statements`` for Companies House PSC statements.

_ABSENCE_BOILERPLATE = re.compile(
    r"(причина\s+)?відсутност[іи]\s+кінцевого\s+бенефіціарного\s+власника"
    r"(\s+юридичної\s+особи)?\s*(є|[-–—])?\s*",
    re.I,
)

#: Ordered most-specific-first. Order matters: nearly every reason contains
#: some form of "відсутн", so the catch-all must be last.
_ABSENCE_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("structure_filed", re.compile(r"структур\w*\s+власност", re.I)),
    (
        "state_or_municipal",
        re.compile(
            r"держав|комунальн|орган\w*\s+(місцев|виконавч)|територіальн\w*\s+громад",
            re.I,
        ),
    ),
    (
        "legal_form_exemption",
        re.compile(
            r"неприбутков|громадськ\w*\s+(організац|формуванн)|релігійн|профспілк"
            r"|кооператив|осбб|споживч\w*\s+товариств|партія|адвокат"
            r"|об'єднанн\w*\s+співвласник",
            re.I,
        ),
    ),
    (
        "statutory_exemption",
        re.compile(
            r"не\s+передбач|не\s+вимага|закон|ст\.?\s*9|п\.?\s*9|кодекс"
            r"|відповідно\s+до\s+законод|згідно\s+(з\s+)?(чинн|закон)",
            re.I,
        ),
    ),
    (
        "not_stated",
        re.compile(
            r"не\s+зазнач|незазнач|не\s+надан|не\s+надавал|не\s+подан"
            r"|не\s+визначен|не\s+встановлен|не\s+виявлен",
            re.I,
        ),
    ),
    (
        "no_qualifying_person",
        re.compile(r"відсутн|не\s+має|немає|не\s+відповіда|жоден|жодна", re.I),
    ),
)

#: Classified reason → BODS v0.4 ``unspecifiedReason``.
#:
#: The values are the seven in the v0.4 codelist, checked against
#: ``libcovebods``' own ``schema-0-4-0/components.json`` rather than against a
#: summary — an earlier draft of this mapping used ``unknownUnknown``, which
#: does not exist, and collapsed the exemptions onto ``noBeneficialOwners`` in
#: the belief that v0.4 had no exemption value. It has two.
#:
#: The split that matters: "no natural person meets the definition" is a
#: finding (``noBeneficialOwners``), while "this legal form is not required to
#: file" is an exemption (``subjectExemptFromDisclosure``) — data-standard
#: #389's first category, and 13,726 Ukrainian records. Collapsing the two
#: would say "we looked and there is nobody" about companies that were never
#: required to look.
ABSENCE_BODS_REASON: dict[str, str] = {
    # A finding of absence: the company looked and no natural person qualifies.
    "no_qualifying_person": "noBeneficialOwners",
    # Still a finding of absence — the filing says the REASON is documented in
    # the separately filed ownership-structure declaration, not that the owner
    # is recorded elsewhere.
    "structure_filed": "noBeneficialOwners",
    # A state or municipal founder has no natural-person owner to name.
    "state_or_municipal": "noBeneficialOwners",
    # Exemptions: the duty to file does not apply to this subject.
    "statutory_exemption": "subjectExemptFromDisclosure",
    "legal_form_exemption": "subjectExemptFromDisclosure",
    # An absence was filed but the reason was left out. That is not a claim
    # that no owner exists, and there is no identified party who withheld it,
    # so neither "noBeneficialOwners" nor "interestedPartyHasNotProvided…".
    "not_stated": "unknown",
    "blank": "unknown",
    "other": "unknown",
}


def classify_absence(reason_text: str) -> str:
    """Classify a filed absence reason. See ``ABSENCE_BODS_REASON``.

    ``structure_filed`` is a deliberate reading: "причина відсутності …
    зазначена у структурі власності" says the *reason for the absence* is
    documented in the separately filed ownership-structure declaration. It
    still asserts that there is no UBO — it is not saying the UBO is recorded
    elsewhere — so it maps to ``noBeneficialOwners`` with the pointer kept in
    the description, not to a withheld-information code.
    """
    text = (reason_text or "").strip()
    if not text or text in {"-", "--", ".", "_"}:
        return "blank"
    core = _ABSENCE_BOILERPLATE.sub("", text).strip(" .,-–—:") or text
    for code, pattern in _ABSENCE_RULES:
        if pattern.search(core):
            return code
    return "other"


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``EDR_UKRAINE_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree (the
    ONRC lesson: they once did not, and a good index sat unread)."""
    from ..cache import data_root

    configured = get_settings().edr_ukraine_db_file
    return Path(configured) if configured else data_root() / "edr_ukraine.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().edr_ukraine_db_file:
            logger.warning("edr_ukraine: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning(
            "edr_ukraine: index at %s has an unexpected schema (want %s); ignoring",
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
    """The index's ``meta`` table, or ``{}`` when there is no index."""
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot of the register's open data.

    ``retrieved_at`` is when OpenCheck built the index (``meta.built_at``);
    ``source_as_of`` is the day data.gov.ua cut the export
    (``meta.export_date``), so ``statementDate`` is the register's date and
    not the day of the lookup (Phase 314). Without this the recorder resolves
    to its ``stub`` default and real register rows badge as "Placeholder
    data" — the ariregister (Phase 45) and ONRC (PR #275) bug.
    """
    meta = read_meta() if meta is None else meta
    cut = meta.get("export_date") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=provenance.parse_moment(cut),
        detail=(
            f"data.gov.ua ЄДР bulk export, cut {cut} (local SQLite index)"
            if cut else "data.gov.ua ЄДР bulk export (local SQLite index)"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state this
    module models (``covers_lei`` is False and the source is not announced).

    The index cannot be built here: data.gov.ua's download refuses every
    datacentre address (``refresh_edr_ukraine_index.sh`` runs at home).
    """
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.edr_ukraine_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"edr_ukraine": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"edr_ukraine": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"edr_ukraine": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("edr_ukraine: asset check/download failed: %s", exc)
        return {"edr_ukraine": f"failed: {exc}"}


def _rows(table: str, edrpou: str) -> list[dict[str, Any]]:
    conn = _shared_conn()
    if conn is None:
        return []
    cur = conn.execute(f"SELECT * FROM {table} WHERE edrpou = ? ORDER BY seq", (edrpou,))
    return [dict(r) for r in cur.fetchall()]


def entity_record(edrpou: str) -> dict[str, Any] | None:
    """The index's ``entity`` row for one code; None when the index does not
    hold it (or there is no index)."""
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM entity WHERE edrpou = ?", (edrpou,)).fetchone()
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class EdrUkraineAdapter(SourceAdapter):
    """Source adapter for the Ukrainian ЄДР (local SQLite index)."""

    id = "edr_ukraine"

    # All three authorities file the 8-digit EDRPOU in ``registeredAs``.
    lookup_derivers = (
        LookupDeriver(UA_RA_CODES, "ua_edrpou", normalise_edrpou),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="ЄДР — Unified State Register of Legal Entities (Ukraine)",
            homepage="https://usr.minjust.gov.ua/",
            description=(
                "Ukrainian company register published as open data by the "
                "Ministry of Justice on data.gov.ua, read from a local index "
                "of the weekly export. Carries founders with their hryvnia "
                "holdings, heads and signatories, governing-body members, and "
                "beneficial owners — or the register's stated reason for "
                "their absence. Addresses and activity codes are withheld at "
                "source under martial law."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains information from the Unified State Register of Legal "
                "Entities, Individual Entrepreneurs and Public Organisations, "
                "published by the Ministry of Justice of Ukraine on data.gov.ua "
                "under a Creative Commons Attribution 4.0 licence."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="UA",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file, not on the LEI.

        Without an index the source would be announced for every Ukrainian
        lookup and then answer nothing — counted in "N of N sources answered"
        while answering nothing (the ``jar_lithuania`` / ``onrc_romania``
        gate). Whether the *entity* is in the index is ``fetch``'s answer: a
        note card when it is not.
        """
        return index_available()

    # -- SourceAdapter ----------------------------------------------------
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY or not (query or "").strip():
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        try:
            rows = conn.execute(
                "SELECT edrpou, name, stan FROM entity WHERE name LIKE ? LIMIT 10",
                (f"%{query.strip()}%",),
            ).fetchall()
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        hits: list[SourceHit] = []
        for row in rows:
            segments = [f"UA-EDR {row['edrpou']}"]
            if row["stan"] and row["stan"] != ACTIVE_STAN:
                segments.append(str(row["stan"]))
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=row["edrpou"],
                    kind=SearchKind.ENTITY,
                    name=row["name"],
                    summary=" · ".join(segments),
                    identifiers={"ua_edrpou": row["edrpou"]},
                    raw={"edrpou": row["edrpou"], "stan": row["stan"]},
                    is_stub=False,
                    liveness="snapshot",
                )
            )
        return hits

    def _stub(self, edrpou: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a code. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "edrpou": edrpou,
            "name": legal_name or "",
            "entity": None,
            "beneficiaries": [],
            "founders": [],
            "signers": [],
            "members": [],
            "executive_power": None,
            "legal_name": legal_name,
            "link": _SEARCH_URL,
            "is_stub": True,
        }

    def _not_in_index(self, edrpou: str, legal_name: str, meta: dict[str, str]) -> dict[str, Any]:
        """The index answered, and does not hold the code — a note card.

        ``is_stub`` False and ``not_found`` True, the INPI / ONRC / JAR shape:
        ``_lookup_pipeline`` drops every ``is_stub`` result above the registry
        branch, so a True here would announce the source and then show
        nothing. Not cached and not a degradation: nothing failed.
        """
        bundle = self._stub(edrpou, legal_name)
        bundle.update(
            {
                "link": DATASET_URL,
                "is_stub": False,
                "not_found": True,
                "coverage_note": COVERAGE_NOT_IN_INDEX,
                "export_date": meta.get("export_date"),
            }
        )
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the ЄДР bundle for one EDRPOU code.

        ``hit_id`` is the EDRPOU (zero-padded here); ``legal_name`` is the
        GLEIF legal name, used for the display name and the stub.
        """
        edrpou = normalise_edrpou(hit_id)
        if not edrpou:
            return self._stub(edrpou, legal_name)
        conn = _shared_conn()
        if conn is None:
            return self._stub(edrpou, legal_name)

        try:
            entity = entity_record(edrpou)
            meta = read_meta()
            if entity is None:
                return self._not_in_index(edrpou, legal_name, meta)
            exec_rows = _rows("executive_power", edrpou)
            bundle: dict[str, Any] = {
                "source_id": self.id,
                "edrpou": edrpou,
                "name": entity.get("name") or legal_name or f"UA {edrpou}",
                "entity": entity,
                "beneficiaries": _rows("beneficiary", edrpou),
                "founders": _rows("founder", edrpou),
                "signers": _rows("signer", edrpou),
                "members": _rows("member", edrpou),
                "executive_power": exec_rows[0] if exec_rows else None,
                "legal_name": legal_name,
                "export_date": meta.get("export_date"),
                "link": DATASET_URL,
                "is_stub": False,
            }
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The ЄДР index could not be read; the card shows nothing from "
                "the register. This is not a confirmation that the register "
                "holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(edrpou, legal_name)

        # Declared only once a row is in hand: a miss is not a read of data.
        declare_snapshot(meta)
        validate_raw("edr_ukraine", EdrUkraineBundle, bundle)
        return bundle
