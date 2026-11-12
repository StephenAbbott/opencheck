"""The Répertoire national des associations (RNA) index — read side.

The Ministry of the Interior publishes the RNA every month as two zipped CSV
families on data.gouv.fr (dataset ``repertoire-national-des-associations``,
Licence Ouverte 2.0): ``rna_waldec_<YYYYMM01>.zip``, every association
declared or changed since the WALDEC system went live in 2009, and
``rna_import_…``, the older paper-era records. ``scripts/build_rna_index.py``
packs the **waldec** file, one row per ``W`` number, into ``rna.sqlite``,
rebuilt monthly by ``.github/workflows/refresh-rna-index.yml`` and downloaded
at boot by :func:`warm_index`.

It is read by the ``sirene`` adapter only, and only **from** Sirene: INSEE's
unit carries the association's RNA number (``identifiantAssociationUniteLegale``
— all 1,850 French LEI associations that have one are in waldec, measured
11 Nov 2026), while the RNA's own ``siret`` column is filled for 3.4 % of
rows. So the join is always Sirene → RNA, never the reverse.

What the index keeps — and what it never keeps
-----------------------------------------------

**Only the associations a Sirene unit names.** The whole waldec file packs to
1.6 GB (415 MB gzipped) — too large to download at every boot — and a record
no Sirene unit names can never be reached, because the adapter arrives by
SIREN. So the builder reads INSEE's ``StockUniteLegale`` (its
``identifiantAssociationUniteLegale`` column, one parquet column) and keeps
those associations only, with the naming unit's SIREN beside each. Units whose
Sirene record is not fully public (``statutDiffusionUniteLegale`` ``P``) are
not kept.

:data:`COLUMNS` is an **allowlist**. The file's ``adrg_*`` columns are the
*management* address, which for most small associations is the declarant's
home and names the declarant (``adrg_declarant``); ``dir_civilite`` is the
director's title. Both are personal data and are dropped by the builder
(:data:`DROPPED_COLUMNS`); ``publiweb`` and ``observation`` (free text the
prefecture types) go with them. The association's own seat (``adrs_*``) is
its registered address and is kept.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from pathlib import Path
from typing import Any

from ..config import get_settings

logger = logging.getLogger(__name__)

#: The table layout this module reads; the builder writes the same value into
#: ``meta`` (a test pins the two equal).
INDEX_SCHEMA_VERSION = "1"

#: The dataset's page and the licence the file is published under.
DATASET_PAGE = "https://www.data.gouv.fr/fr/datasets/repertoire-national-des-associations/"
LICENCE = "Licence Ouverte / Open Licence 2.0"

#: The waldec file's header row, in order (checked on every build: a changed
#: header is a refusal, never a guess).
WALDEC_HEADER: tuple[str, ...] = (
    "id", "id_ex", "siret", "rup_mi", "gestion", "date_creat", "date_decla",
    "date_publi", "date_disso", "nature", "groupement", "titre", "titre_court",
    "objet", "objet_social1", "objet_social2", "adrs_complement",
    "adrs_numvoie", "adrs_repetition", "adrs_typevoie", "adrs_libvoie",
    "adrs_distrib", "adrs_codeinsee", "adrs_codepostal", "adrs_libcommune",
    "adrg_declarant", "adrg_complemid", "adrg_complemgeo", "adrg_libvoie",
    "adrg_distrib", "adrg_codepostal", "adrg_achemine", "adrg_pays",
    "dir_civilite", "siteweb", "publiweb", "observation", "position",
    "maj_time",
)

#: What the index keeps (an allowlist): what the ``sirene`` mapper reads, and
#: nothing else. The seat address is Sirene's (its head-office record), so the
#: RNA's ``adrs_*`` copy is not kept either.
COLUMNS: tuple[str, ...] = (
    "id", "siret", "rup_mi", "date_creat", "date_decla", "date_publi",
    "date_disso", "nature", "groupement", "titre", "objet", "position",
    "maj_time",
)

#: Columns the builder adds from Sirene, beside the waldec ones: the SIREN of
#: the unit that names this association.
SIRENE_COLUMNS: tuple[str, ...] = ("siren",)

#: What the builder drops — above all the management address (the
#: declarant's, for most associations), the director's title, and the
#: prefecture's free text, which must never be published.
DROPPED_COLUMNS: frozenset[str] = frozenset(WALDEC_HEADER) - frozenset(COLUMNS)

#: ``position``: the record's state in the register.
POSITIONS = {
    "A": "active",
    "D": "dissolved",
    "S": "deleted",
}

#: ``nature``: the form of the record. ``D`` (a declared association) is
#: 99.7 % of the file; the other letters (``U``, ``R``, ``A``, ``B``, ``G``,
#: ``I``) are undocumented and are not glossed.
NATURES = {
    "D": "declared association",
}

#: ``groupement``: simple association, union or federation.
GROUPEMENTS = {
    "S": "simple association",
    "U": "union of associations",
    "F": "federation of associations",
}

#: ``W`` then nine characters: digits, with a letter in the third place for
#: the prefectures that number with one (``W9G2003456``, Paris; 128,706 of
#: the 1 Sept 2026 file's records).
_RNA_ID = re.compile(r"^W[0-9A-Z]{9}$")


def normalise_rna(value: Any) -> str:
    """An RNA number as the register writes it: ``W`` and nine characters.

    Sirene writes it in either case and sometimes padded with blanks
    (``" w922001074"``). ``ValueError`` for anything else — the old
    pre-2009 numbers (``0711003573``, the ``id_ex`` column) are not keys.
    """
    text = re.sub(r"\s", "", str(value or "")).upper()
    if not _RNA_ID.match(text):
        raise ValueError(f"not an RNA number: {value!r}")
    return text


def iso_day(value: Any) -> str | None:
    """A waldec date (``2007-09-17``) or None. ``0001-01-01`` is the file's
    "no date", and ``1900-01-01`` the placeholder Sirene uses too (357 waldec
    records, Sept 2026) — neither is a date."""
    text = str(value or "").strip()[:10]
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", text) or text in ("0001-01-01", "1900-01-01"):
        return None
    return text


# ---------------------------------------------------------------------------
# The index
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``RNA_DB_FILE`` or the data-root default."""
    from ..cache import data_root

    configured = get_settings().rna_db_file
    return Path(configured) if configured else data_root() / "rna.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().rna_db_file:
            logger.warning("rna: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("rna: index at %s has an unexpected schema; ignoring", path)
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
    return _shared_conn() is not None


def read_meta() -> dict[str, str]:
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def association(rna: str) -> dict[str, Any] | None:
    """The waldec record for one RNA number, or None. Raises
    ``sqlite3.Error`` on a broken index and ``ValueError`` on a value that is
    not an RNA number."""
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM association WHERE id = ?", (normalise_rna(rna),)).fetchone()
    if row is None:
        return None
    record = {k: row[k] for k in row.keys() if row[k] not in (None, "")}
    leaked = DROPPED_COLUMNS & set(record)
    if leaked:  # pragma: no cover - the builder's allowlist makes this impossible
        raise ValueError(f"rna index carries dropped columns: {sorted(leaked)}")
    return record


def rnas_for_siren(siren: str) -> list[str] | None:
    """Every RNA number whose association a Sirene unit with this SIREN names,
    sorted; ``[]`` when none does; ``None`` when there is no index.

    The JOAFE reader's join (the JOAFE History ticket, 11 Nov 2026): the index
    keeps the naming unit's SIREN beside each association, so SIREN → RNA is
    a local read. An index built before ``association_siren`` answers by scan
    (about half a second) until the monthly rebuild. Raises ``sqlite3.Error``
    on a broken index.
    """
    conn = _shared_conn()
    if conn is None:
        return None
    rows = conn.execute(
        "SELECT id FROM association WHERE siren = ? ORDER BY id", (str(siren),)
    ).fetchall()
    return [str(r[0]) for r in rows if r[0]]


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed. Never raises — no index is a state (Sirene answers
    without the RNA's dates and purpose)."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.rna_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"rna": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"rna": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"rna": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("rna: asset check/download failed: %s", type(exc).__name__)
        return {"rna": f"failed: {type(exc).__name__}"}
