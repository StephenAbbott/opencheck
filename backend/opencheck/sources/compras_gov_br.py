"""Brazil — Compras.gov.br, the public procurement system's published awards.

The Secretaria de Gestão e Inovação (SEGES) publishes the procurement run on
Compras.gov.br as OCDS. The system is the federal government's, but any
public body may use it — 563 state bodies and 1,267 municipalities did in
2025 — so the file covers federal, state and municipal buyers alike. The
Open Contracting Partnership's Data Registry (publication 157) compiles the
releases into one ``full.jsonl.gz``, retrieved monthly. This adapter answers
one question from it: **which public bodies awarded this company what** —
awards won, never ownership.

What the file holds (measured on the retrieval of 27 Oct 2026)
---------------------------------------------------------------
920,868 contracting processes, 1,672,831 awards, Aug 2021 – Sep 2026. The
publication covers the tender and award stages only: **no contracts, no
award status** — an award is one supplier's result in a process, with the
items it won. Every value is in BRL; 9,521 awards carry zero.

Of 1,674,204 supplier entries, 1,602,489 (95.7%) carry a valid 14-digit
CNPJ (``BR-CNPJ``, the scheme on every supplier) and 64,667 (3.9%) an
11-digit CPF — a natural person's tax number, never read. 176,565 distinct
CNPJs; 172,139 distinct roots.

The join key is the CNPJ root
-----------------------------
A CNPJ is the company's eight-digit root, a four-digit establishment number
(``0001`` the head office) and two check digits. A branch is the same legal
person as its head office, and 72,702 award entries name a branch, so the
index is keyed on the **root** and records which establishments won
(Stephen, 12 Nov 2026, decision A: 347 LEIs reached against 289 on the exact
number).

GLEIF files a Brazilian company's CNPJ under the Receita Federal
(``RA000681``) and also under the state commercial boards (Juntas
Comerciais, ``RA000036``–``RA000062``), the CVM (``RA000822``) and the
Central Bank (``RA000823``). Any of them is accepted when ``registeredAs``
is a CNPJ that passes both check digits (decision B: +57 LEIs); a CPF filed
for a sole proprietor is not.

Personal data (decision D)
--------------------------
No supplier name, address or contact point is stored: an MEI (sole trader)
is named "NAME + CPF" by the Receita Federal, and the company's name comes
from GLEIF and the CNPJ register anyway. CPF suppliers are dropped at build
time. The object of each award is kept, truncated, with any CPF-shaped
number removed.

Values (decision E)
-------------------
Each award's published value, and a total said as OpenCheck's sum of
published award values — not money paid: the file has no contract stage.
Zero-value awards are counted, not hidden. The award value does not
consistently leave out a cancelled item, so it is shown as published.

Index, not live API
-------------------
The registry file is 963 MB gzipped, and the API's OCDS endpoint
(``/modulo-ocds/1_releases``) requires a ``buyerID`` — it cannot be searched
by supplier. The index is built weekly by
``.github/workflows/refresh-compras-gov-br-index.yml``
(``scripts/build_compras_gov_br_index.py``), published as the
``compras-gov-br-index`` release asset and downloaded at boot
(``warm_index``). Without it the source is not announced (``covers_lei``).
Per root it keeps totals, its five largest buyers, its establishments and
the ``LATEST_KEPT`` latest plus ``LARGEST_KEPT`` largest awards — not every
award.

Links (decision G): the Compras.gov.br portal home page. The per-process
public page is a single-page application whose address could not be
confirmed, so no per-award link is invented.

Licence: CC BY 4.0 (the registry's statement of the publication's licence).
"""

from __future__ import annotations

import gzip
import heapq
import json
import logging
import os
import re
import sqlite3
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from .. import provenance
from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.compras_gov_br import ComprasGovBrBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities and the CNPJ
# ---------------------------------------------------------------------------

#: Every Brazilian registration authority in GLEIF's list (re-checked
#: 12 Nov 2026): the Juntas Comerciais, the Receita Federal, the CVM and the
#: Central Bank. Each files a CNPJ for most of its records.
BR_RA_CODES: frozenset[str] = frozenset(
    {f"RA0000{n:02d}" for n in range(36, 63)} | {"RA000681", "RA000822", "RA000823"}
)

_W1 = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
_W2 = (6, *_W1)


def _digits(raw: Any) -> str:
    return re.sub(r"\D", "", str(raw or ""))


def _check_digit(body: str, weights: tuple[int, ...]) -> str:
    rest = sum(int(d) * w for d, w in zip(body, weights)) % 11
    return "0" if rest < 2 else str(11 - rest)


def cnpj_valid(cnpj: str) -> bool:
    """Fourteen digits, not all the same, and both check digits right.

    Always checked here, whatever the identifier-enforcement setting: the
    build and the join both depend on it.
    """
    if not re.fullmatch(r"\d{14}", cnpj or "") or len(set(cnpj)) == 1:
        return False
    return cnpj[12] == _check_digit(cnpj[:12], _W1) and cnpj[13] == _check_digit(cnpj[:13], _W2)


def candidate_cnpj(registered_at: str, registered_as: str) -> str | None:
    """The CNPJ GLEIF's (registeredAt, registeredAs) pair names, or None."""
    if registered_at not in BR_RA_CODES:
        return None
    value = _digits(registered_as)
    return value if cnpj_valid(value) else None


def format_cnpj(cnpj: str) -> str:
    """``00000000000191`` → ``00.000.000/0001-91``, the Receita's printed form."""
    if len(cnpj) != 14:
        return cnpj
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


def format_root(root: str) -> str:
    """``00000000`` → ``00.000.000``."""
    return f"{root[:2]}.{root[2:5]}.{root[5:8]}" if len(root) == 8 else root


# ---------------------------------------------------------------------------
# Where the data is
# ---------------------------------------------------------------------------

REGISTRY_URL = "https://data.open-contracting.org/en/publication/157"
FULL_FILE_URL = "https://data.open-contracting.org/en/publication/157/download?name=full.jsonl.gz"
#: Decision G: the portal's home page, not an unconfirmed per-process address.
PORTAL_URL = "https://www.gov.br/compras/pt-br"
LICENSE_ID = "CC-BY-4.0"

#: Bumped whenever the index tables change shape.
INDEX_SCHEMA_VERSION = "1"

TOP_BUYERS = 5
LATEST_KEPT = 8
LARGEST_KEPT = 4
ESTABLISHMENTS_KEPT = 10
OBJECT_CHARS = 140
BASIS_CHARS = 160

#: The award's ``title`` as filed → a one-letter code in the index.
_PROCEDURES = {
    "Edital": "E",
    "Ato que autoriza a Contratação Direta": "D",
    "Aviso de Contratação Direta": "A",
    "Edital de Chamamento Público": "C",
}
PROCEDURE_WORDS = {
    "E": "Competitive procurement (edital)",
    "D": "Direct contracting (authorising act)",
    "A": "Direct contracting (notice)",
    "C": "Public call (chamamento público)",
}
#: ``BRA-ESFERA`` classification → the buyer's sphere of government.
SPHERE_WORDS = {"F": "Federal", "E": "State", "M": "Municipal"}

#: A CPF written either way, removed from free text before it is stored.
_CPF_TEXT = re.compile(r"\b\d{3}\.?\d{3}\.?\d{3}-?\d{2}\b")


def _amount(value: Any) -> int | None:
    """``1249027.65`` → whole reais, half up. Unreadable → None."""
    try:
        return int(Decimal(str(value)).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError, TypeError):
        return None


def _day(stamp: Any) -> str | None:
    """Award dates are a calendar day stored as midnight UTC: trim them."""
    text = str(stamp or "").strip()
    return text[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", text) else None


def clean_text(text: Any, limit: int) -> str:
    """Whitespace folded, any CPF-shaped number removed, cut to ``limit``."""
    folded = " ".join(str(text or "").split())
    folded = _CPF_TEXT.sub("[number removed]", folded)
    return folded if len(folded) <= limit else folded[: limit - 1].rstrip() + "…"


def legal_basis(rationale: Any) -> str:
    """``amparoLegalNome`` from ``procurementMethodRationale``, or ''.

    The field reads ``amparoLegalCodigo:1; amparoLegalNome: Lei 14.133/2021,
    Art. 28, I; amparoLegalDescricao: …``.
    """
    match = re.search(r"amparoLegalNome:\s*([^;]+)", str(rationale or ""))
    return match.group(1).strip()[:BASIS_CHARS] if match else ""


# ---------------------------------------------------------------------------
# Index build
# ---------------------------------------------------------------------------


class IndexBuildError(RuntimeError):
    """The file is not what the index builder understands."""


@dataclass
class _Supplier:
    awards: int = 0
    value: int = 0
    zero_value: int = 0
    direct: int = 0
    first: str = "9999-99-99"
    last: str = "0000-00-00"
    # buyer id → [awards, value]
    buyers: dict[int, list[int]] = field(default_factory=dict)
    # establishment (6-digit tail) → awards
    establishments: dict[str, int] = field(default_factory=dict)
    # (rank, award key, sequence, record): the sequence breaks ties.
    latest: list[tuple[str, str, int, tuple[Any, ...]]] = field(default_factory=list)
    largest: list[tuple[int, str, int, tuple[Any, ...]]] = field(default_factory=list)


def _keep(heap: list[Any], entry: Any, size: int) -> None:
    if len(heap) < size:
        heapq.heappush(heap, entry)
    elif entry > heap[0]:
        heapq.heapreplace(heap, entry)


def iter_releases(path: Path | str) -> Iterator[dict[str, Any]]:
    """The compiled releases in a registry ``.jsonl`` or ``.jsonl.gz``."""
    if str(path) == "-":
        import sys

        yield from _lines(sys.stdin)
        return
    path = Path(path)
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
        yield from _lines(fh)


def _lines(fh: Iterable[str]) -> Iterator[dict[str, Any]]:
    for line in fh:
        line = line.strip()
        if line:
            yield json.loads(line)


def _sphere(party: dict[str, Any]) -> str:
    for cls in (party.get("details") or {}).get("classifications") or []:
        if cls.get("scheme") == "BRA-ESFERA":
            code = str(cls.get("id") or "").rsplit("-", 1)[-1]
            if code in SPHERE_WORDS:
                return code
    return ""


def build_index(
    releases: Iterable[dict[str, Any]],
    out_path: Path | str,
    *,
    source_meta: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build the SQLite index from compiled releases.

    Written beside ``out_path`` and renamed into place, so a reader never sees
    a half-built file. Returns the ``meta`` table.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    counts = {
        k: 0
        for k in (
            "releases", "awards", "supplier_entries", "supplier_entries_cnpj",
            "supplier_entries_cpf", "supplier_entries_other", "awards_without_cnpj",
            "awards_shared", "awards_zero_value",
        )
    }
    suppliers: dict[str, _Supplier] = {}
    buyers: dict[str, int] = {}
    buyer_rows: dict[int, tuple[str, str, str, str]] = {}
    bases: dict[str, int] = {}
    data_to = ""

    def buyer_id(party: dict[str, Any] | None, fallback: dict[str, Any]) -> int | None:
        source = party or fallback
        if not source:
            return None
        cnpj = _digits((source.get("identifier") or {}).get("id")) or _digits(
            str(source.get("id") or "").replace("BR-CNPJ-", "")
        )
        name = " ".join(str(source.get("name") or "").split())
        uasg = ""
        for extra in (party or {}).get("additionalIdentifiers") or []:
            ident = str(extra.get("id") or "").strip()
            if ident and ident != "0":
                uasg = ident
                break
        key = f"{cnpj}|{uasg}" if cnpj else f"|{name}"
        if key not in buyers:
            buyers[key] = len(buyers) + 1
        bid = buyers[key]
        if name or bid not in buyer_rows:
            buyer_rows[bid] = (name, cnpj, uasg, _sphere(party or {}))
        return bid

    for release in releases:
        counts["releases"] += 1
        parties = release.get("parties") or []
        buyer_party = next(
            (p for p in parties if "buyer" in (p.get("roles") or [])), None
        )
        buyer = buyer_id(buyer_party, release.get("buyer") or {})
        tender = release.get("tender") or {}
        process = str(tender.get("id") or release.get("ocid") or "").strip()
        basis_text = legal_basis(tender.get("procurementMethodRationale"))
        basis = bases.setdefault(basis_text, len(bases) + 1) if basis_text else None
        tender_object = tender.get("description")
        for award in release.get("awards") or []:
            counts["awards"] += 1
            sup_cnpjs: list[str] = []
            for sup in award.get("suppliers") or []:
                counts["supplier_entries"] += 1
                ident = _digits(str(sup.get("id") or "").replace("BR-CNPJ-", ""))
                if cnpj_valid(ident):
                    counts["supplier_entries_cnpj"] += 1
                    if ident not in sup_cnpjs:
                        sup_cnpjs.append(ident)
                elif len(ident) == 11:
                    counts["supplier_entries_cpf"] += 1  # a person: never read
                else:
                    counts["supplier_entries_other"] += 1
            if not sup_cnpjs:
                counts["awards_without_cnpj"] += 1
                continue
            day = _day(award.get("date"))
            if day and day > data_to:
                data_to = day
            value = _amount((award.get("value") or {}).get("amount"))
            procedure = _PROCEDURES.get(str(award.get("title") or "").strip(), "")
            roots = list(dict.fromkeys(c[:8] for c in sup_cnpjs))
            shared = len(roots) > 1
            counts["awards_shared"] += shared
            counts["awards_zero_value"] += value == 0
            key = f"{process}/{award.get('id') or ''}"
            obj = clean_text(award.get("description") or tender_object, OBJECT_CHARS)
            for root in roots:
                tails = [c[8:] for c in sup_cnpjs if c[:8] == root]
                sup = suppliers.setdefault(root, _Supplier())
                sup.awards += 1
                sup.value += value or 0
                sup.zero_value += value == 0
                sup.direct += procedure in ("A", "D")
                if day:
                    sup.first = min(sup.first, day)
                    sup.last = max(sup.last, day)
                for tail in tails:
                    sup.establishments[tail] = sup.establishments.get(tail, 0) + 1
                if buyer is not None:
                    tally = sup.buyers.setdefault(buyer, [0, 0])
                    tally[0] += 1
                    tally[1] += value or 0
                record = (
                    day, process, buyer, obj, value, procedure or None, basis,
                    tails[0], len(roots) - 1,
                )
                seq = counts["awards"]  # a tie-break, so a record is never compared
                _keep(sup.latest, (day or "", key, seq, record), LATEST_KEPT)
                _keep(sup.largest, (value or 0, key, seq, record), LARGEST_KEPT)

    if counts["supplier_entries_cnpj"] < 1:
        raise IndexBuildError("no supplier carries a valid CNPJ — not the Compras.gov.br file shape")

    tmp = out_path.with_name(out_path.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(str(tmp))
    conn.executescript(_SCHEMA)
    buyers_used: set[int] = set()
    bases_used: set[int] = set()
    for root, sup in suppliers.items():
        key_root = int(root)
        conn.execute(
            "INSERT INTO supplier VALUES (?,?,?,?,?,?,?,?,?)",
            (
                key_root, sup.awards, sup.value, len(sup.buyers),
                sup.first if sup.first != "9999-99-99" else None,
                sup.last if sup.last != "0000-00-00" else None,
                sup.zero_value, sup.direct, len(sup.establishments),
            ),
        )
        ranked = sorted(sup.buyers.items(), key=lambda kv: (-kv[1][1], -kv[1][0], kv[0]))
        for bid, (n, total) in ranked[:TOP_BUYERS]:
            buyers_used.add(bid)
            conn.execute("INSERT INTO supplier_buyer VALUES (?,?,?,?)", (key_root, bid, n, total))
        est = sorted(sup.establishments.items(), key=lambda kv: (-kv[1], kv[0]))
        for tail, n in est[:ESTABLISHMENTS_KEPT]:
            conn.execute("INSERT INTO establishment VALUES (?,?,?)", (key_root, tail, n))
        kept: dict[str, tuple[Any, ...]] = {}
        for _rank, award_key, _seq, row in (*sup.latest, *sup.largest):
            kept[award_key] = row
        for award_key, row in kept.items():
            day, process, bid, obj, value, procedure, basis, tail, others = row
            if bid is not None:
                buyers_used.add(bid)
            if basis is not None:
                bases_used.add(basis)
            conn.execute(
                "INSERT INTO award VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (key_root, award_key, process, day, bid, obj, value, procedure, basis, tail, others),
            )
    conn.executemany(
        "INSERT INTO buyer VALUES (?,?,?,?,?)",
        [(b, *buyer_rows.get(b, ("", "", "", ""))) for b in sorted(buyers_used)],
    )
    conn.executemany(
        "INSERT INTO legal_basis VALUES (?,?)",
        [(i, t) for t, i in sorted(bases.items(), key=lambda kv: kv[1]) if i in bases_used],
    )
    stored = conn.execute("SELECT COUNT(*) FROM supplier").fetchone()[0]
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "build_seconds": f"{time.monotonic() - started:.1f}",
        "data_to": data_to,
        "suppliers": str(stored),
        **{k: str(v) for k, v in counts.items()},
        **(source_meta or {}),
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", list(meta.items()))
    conn.commit()
    conn.execute("VACUUM")
    conn.close()
    os.replace(tmp, out_path)
    logger.info("compras_gov_br: index built at %s: %s suppliers", out_path, stored)
    return meta


_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE supplier (
    root INTEGER PRIMARY KEY,
    awards INTEGER NOT NULL,
    value_brl INTEGER NOT NULL,
    buyers INTEGER NOT NULL,
    first_date TEXT,
    last_date TEXT,
    zero_value INTEGER NOT NULL,
    direct INTEGER NOT NULL,
    establishments INTEGER NOT NULL
);
CREATE TABLE buyer (id INTEGER PRIMARY KEY, name TEXT, cnpj TEXT, uasg TEXT, sphere TEXT);
CREATE TABLE supplier_buyer (
    root INTEGER NOT NULL,
    buyer INTEGER NOT NULL,
    awards INTEGER NOT NULL,
    value_brl INTEGER NOT NULL,
    PRIMARY KEY (root, buyer)
) WITHOUT ROWID;
CREATE TABLE establishment (
    root INTEGER NOT NULL,
    tail TEXT NOT NULL,
    awards INTEGER NOT NULL,
    PRIMARY KEY (root, tail)
) WITHOUT ROWID;
CREATE TABLE award (
    root INTEGER NOT NULL,
    key TEXT NOT NULL,
    process TEXT,
    awarded TEXT,
    buyer INTEGER,
    object TEXT,
    value_brl INTEGER,
    procedure TEXT,
    basis INTEGER,
    establishment TEXT,
    other_suppliers INTEGER NOT NULL,
    PRIMARY KEY (root, key)
) WITHOUT ROWID;
CREATE TABLE legal_basis (id INTEGER PRIMARY KEY, text TEXT);
"""


# ---------------------------------------------------------------------------
# Where the index lives, and getting it onto the host
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``COMPRAS_GOV_BR_DB_FILE`` or the data-root default."""
    from ..cache import data_root

    configured = get_settings().compras_gov_br_db_file
    return Path(configured) if configured else data_root() / "compras_gov_br.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().compras_gov_br_db_file:
            logger.warning("compras_gov_br: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("compras_gov_br: index at %s has an unexpected schema; ignoring", path)
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


def _moment(text: str | None) -> datetime | None:
    try:
        return datetime.fromisoformat(str(text or "")).replace(tzinfo=UTC) if text else None
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """The registry's retrieval day is the cut; the build is our clock."""
    meta = read_meta() if meta is None else meta
    cut = provenance.parse_moment(meta.get("source_last_modified")) or _moment(meta.get("data_to"))
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut,
        detail="Compras.gov.br awards (OCDS), via the OCP Data Registry",
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.compras_gov_br_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"compras_gov_br": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"compras_gov_br": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"compras_gov_br": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("compras_gov_br: asset check/download failed: %s", exc)
        return {"compras_gov_br": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# Reading one supplier
# ---------------------------------------------------------------------------


def supplier_record(root: str) -> dict[str, Any] | None:
    """Everything the index holds for one CNPJ root, or None."""
    conn = _shared_conn()
    if conn is None:
        return None
    key = int(root)
    row = conn.execute("SELECT * FROM supplier WHERE root = ?", (key,)).fetchone()
    if row is None:
        return None
    supplier = dict(row)
    supplier["root"] = root
    award_rows = conn.execute(
        "SELECT a.*, l.text AS basis_text FROM award a"
        " LEFT JOIN legal_basis l ON l.id = a.basis WHERE a.root = ?",
        (key,),
    ).fetchall()
    buyer_rows = conn.execute(
        "SELECT * FROM supplier_buyer WHERE root = ? ORDER BY value_brl DESC, awards DESC",
        (key,),
    ).fetchall()
    est_rows = conn.execute(
        "SELECT * FROM establishment WHERE root = ? ORDER BY awards DESC, tail", (key,)
    ).fetchall()
    wanted = {r["buyer"] for r in (*award_rows, *buyer_rows) if r["buyer"] is not None}
    info: dict[int, sqlite3.Row] = {}
    if wanted:
        marks = ",".join("?" * len(wanted))
        info = {
            r["id"]: r
            for r in conn.execute(f"SELECT * FROM buyer WHERE id IN ({marks})", list(wanted))
        }

    def buyer_fields(bid: int | None) -> dict[str, str]:
        b = info.get(bid) if bid is not None else None
        if b is None:
            return {"name": "", "sphere": "", "uasg": ""}
        return {
            "name": b["name"] or "",
            "sphere": SPHERE_WORDS.get(b["sphere"] or "", ""),
            "uasg": b["uasg"] or "",
        }

    buyers = [
        {**buyer_fields(r["buyer"]), "awards": r["awards"], "value_brl": r["value_brl"]}
        for r in buyer_rows
    ]
    establishments = [
        {
            "cnpj": format_cnpj(f"{root}{r['tail']}"),
            "head_office": r["tail"][:4] == "0001",
            "awards": r["awards"],
        }
        for r in est_rows
    ]
    awards = []
    for r in award_rows:
        b = buyer_fields(r["buyer"])
        awards.append({
            "key": r["key"],
            "process": r["process"] or "",
            "awarded": r["awarded"],
            "buyer": b["name"],
            "buyer_sphere": b["sphere"],
            "buyer_uasg": b["uasg"],
            "object": r["object"] or "",
            "value_brl": r["value_brl"],
            "procedure": PROCEDURE_WORDS.get(r["procedure"] or "", ""),
            "legal_basis": r["basis_text"] or "",
            "establishment": format_cnpj(f"{root}{r['establishment']}") if r["establishment"] else "",
            "other_suppliers": r["other_suppliers"],
        })
    awards.sort(key=lambda a: (a["awarded"] or "", a["key"]), reverse=True)
    return {
        "supplier": supplier,
        "buyers": buyers,
        "establishments": establishments,
        "awards": awards,
    }


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class ComprasGovBrAdapter(SourceAdapter):
    """Compras.gov.br awards, via the CNPJ root GLEIF's filing names."""

    id = "compras_gov_br"

    # No ``lookup_derivers``: procurement is reached the way TED, ChileCompra
    # and AusTender are, from ``_dispatch`` via ``fetch_by_identifiers``.
    # ``cnpj_brazil`` owns RA000681 as the register.

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Compras.gov.br — Brazilian public procurement",
            homepage=PORTAL_URL,
            description=(
                "Awards published by Compras.gov.br, the procurement system "
                "federal, state and municipal bodies in Brazil use, from the "
                "Open Contracting Partnership's Data Registry copy, rebuilt "
                "weekly. For a company (all its establishments, by CNPJ root): "
                "its latest and largest awards with buyer, sphere of "
                "government, value, procedure and legal basis, plus totals "
                "since 2021. Awards won, not ownership."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains Compras.gov.br public procurement data published by "
                "the Secretaria de Gestão e Inovação (Ministério da Gestão e da "
                "Inovação em Serviços Públicos), licensed under CC BY 4.0, via "
                "the Open Contracting Partnership's Data Registry."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=False,
            # None, like every non-register source: knowability rows are about
            # what a register publishes on owners, not procurement.
            country=None,
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file (the ChileCompra rule)."""
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Not name-searchable: reached from the CNPJ GLEIF files."""
        return []

    def _bundle(self, cnpj: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "cnpj": cnpj,
            "cnpj_display": format_cnpj(cnpj) if cnpj else "",
            "root": cnpj[:8],
            "root_display": format_root(cnpj[:8]) if cnpj else "",
            "legal_name": legal_name or "",
            "supplier": None,
            "buyers": [],
            "establishments": [],
            "awards": [],
            "data_to": None,
            "link": PORTAL_URL,
            "registry_url": REGISTRY_URL,
            "not_found": False,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """What Compras.gov.br's awards say about one company (by any of its
        CNPJs — the root is what is read)."""
        cnpj = _digits(hit_id)
        if not cnpj_valid(cnpj):
            return self._bundle("", legal_name)
        if _shared_conn() is None:
            return self._bundle(cnpj, legal_name)
        meta = read_meta()
        declare_snapshot(meta)
        extra = {"data_to": meta.get("data_to") or None}
        found = supplier_record(cnpj[:8])
        if found is None:
            return self._bundle(cnpj, legal_name, not_found=True, is_stub=False, **extra)
        bundle = self._bundle(cnpj, legal_name, is_stub=False, **extra, **found)
        validate_raw(self.id, ComprasGovBrBundle, bundle)
        return bundle

    @staticmethod
    def applies_to(registered_at: str, registered_as: str) -> bool:
        """A Brazilian registration authority filing a valid CNPJ.

        Asked before dispatch, so a company outside Brazil is never
        announced as checked against Compras.gov.br.
        """
        return candidate_cnpj(registered_at, registered_as) is not None

    async def fetch_by_identifiers(
        self,
        lei: str,
        registered_as: str,
        registered_at: str,
        *,
        legal_name: str = "",
    ) -> dict[str, Any] | None:
        """The pipeline's entry point (the TED / ChileCompra shape)."""
        cnpj = candidate_cnpj(registered_at, registered_as)
        if cnpj is None:
            return None
        return await self.fetch(cnpj, legal_name=legal_name)
