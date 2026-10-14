"""Institutional shareholders from SEC Form 13F (Phase 334).

Reads the index ``scripts/build_form13f_index.py`` builds (Phase 333) and
answers one question for a lookup: which institutional investment managers
reported a position in this company's securities on Form 13F, and how large.

**This is not beneficial ownership, and nothing here is allowed to become it**
(Stephen, 14 Oct 2026). A 13F filer is an investment manager reporting the
positions it has discretion over for funds and clients. So this module:

* is not a source — not in ``REGISTRY``, no hit, no BODS statement, no place in
  the ownership graph, not counted in coverage;
* is read by no risk signal, no related-party screen, no verdict and no AI
  narrative (``tests/test_form13f.py`` fails if one of them imports it);
* never totals a list: positions overlap, because a manager's report can
  include another manager that also reported the same shares;
* never repairs a figure: a value still reported in thousands is not shown,
  only its shares; a position larger than the shares outstanding is left out
  and counted.

It rides one lookup event, ``form13f``, built after the sources settle (the
second way in to a company's CUSIP is its own Schedule 13D/13G filings, read
by the SEC EDGAR source) and frozen into the run, so a saved report shows the
quarter of its own day — the PermID listing arrangement (Phase 236).

How a company reaches its securities
------------------------------------

13F rows carry a CUSIP and the issuer's name — no LEI, no CIK.

1. **GLEIF's ISIN file** (the Phase 258 table): a US ISIN is ``US`` + CUSIP +
   a check digit. It also maps an American depositary receipt to the foreign
   issuer (Novo Nordisk, Shell).
2. **The SEC EDGAR source's own statements**: its mapper puts the issuer's
   CUSIP, read off its Schedule 13D/13G filings, on the issuer's entity
   statement (scheme ``CUSIP``). This reaches Chevron, which GLEIF lists no
   ISIN for, and issuers incorporated outside the US whose CUSIP starts with
   a letter (Accenture, Medtronic).

Matching on an issuer's name or on the six-character CUSIP issuer prefix was
measured and rejected (Phase 333 investigation).

The percentage
--------------

Computed only when every gate holds, and labelled as OpenCheck's arithmetic:

* the security is not an ADR (one ADR can stand for more or fewer shares);
* the company has one class of shares among the matched securities
  (Berkshire's A and B shares are two classes, so neither gets a percentage);
* the SEC's frames carry the issuer's undimensioned
  ``dei:EntityCommonStockSharesOutstanding``, dated within
  ``PERCENT_WINDOW_DAYS`` of the 13F period — a multi-class issuer reports it
  per class and is absent, which is the intended outcome.

Under those gates Berkshire Hathaway's 24,669,778 Moody's shares are 14.24%
of 173,200,000 — the figure commercial vendors show.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import get_settings

log = logging.getLogger(__name__)

#: The index schemas this reader understands (``meta.schema_version``).
READABLE_SCHEMAS = frozenset({"1"})
#: Positions per security the payload carries — the section's twenty.
TOP = 20
#: How far the shares-outstanding fact may sit from the 13F period.
PERCENT_WINDOW_DAYS = 100
#: The most US ISINs read for one LEI (debt issuers hold thousands).
MAX_US_ISINS = 20_000
#: A security is listed only when at least this many managers reported it.
#: Below it sit preferreds, notes and bonds a handful of filers entered as
#: shares (JPMorgan: twenty such CUSIPs of one to nine managers each).
MIN_MANAGERS = 10

SOURCE_URL = "https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets"
ATTRIBUTION = (
    "Contains data from the SEC's Form 13F data sets (U.S. Securities and Exchange "
    "Commission), public domain."
)
CAVEAT = (
    "Reported by the filers and not verified by the SEC; no substitute for the "
    "filings themselves."
)
#: The one sentence allowed to name what 13F is not. Every other sentence this
#: module writes is held to ``BANNED_VOCABULARY``.
DISCLAIMER = (
    "Form 13F lists the positions institutional investment managers have discretion "
    "over for funds and clients. It is not a record of beneficial ownership: it never "
    "enters the ownership graph and no risk signal reads it."
)
STANDING_NOTES = (
    "Positions overlap — a manager's report can include another manager that also "
    "reported the same shares — so they are never added together.",
    "Long positions only, as at the quarter end and filed up to 45 days later. A "
    "manager may be allowed to leave a position out under confidential treatment, so "
    "a manager missing here says nothing.",
    "A manager's Form 13F figure and its Schedule 13D/13G figure can differ: they are "
    "dated differently and filed under different rules.",
)
THOUSANDS_NOTE = (
    "Value reported in thousands of dollars, as before 2023 — the shares are shown "
    "and the value is not."
)
ADR_NOTE = (
    "American depositary shares traded in the US. No percentage is computed: one "
    "depositary share can stand for more or fewer than one underlying share."
)
#: Words no sentence here may use (``DISCLAIMER`` excepted): each would read a
#: manager's reported position as ownership of the company.
BANNED_VOCABULARY = ("owner", "owns", "owned", "beneficial", "stake", "control")

_ADR_RE = re.compile(r"\b(ADRS?|ADSS?|DEP|DEPOSITA?O?RY|SPONSORED)\b")
_CUSIP_RE = re.compile(r"^[0-9A-Z]{9}$")


# ---------------------------------------------------------------------------
# The file
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``OPENCHECK_FORM13F_DB_FILE`` or the data-root default — one function,
    so the reader and the boot download agree."""
    from .cache import data_root

    configured = get_settings().form13f_db_file
    return Path(configured) if configured else data_root() / "form13f.sqlite"


_SHARED: dict[str, Any] = {}


def reset_connection() -> None:
    """Forget the cached connection (after a download, and in tests). Not
    closed: a reader in another thread may be mid-query."""
    _SHARED.clear()


def _connect() -> tuple[sqlite3.Connection, dict[str, str]] | None:
    cached = _SHARED.get("conn")
    if cached is not None:
        return cached, _SHARED["meta"]
    path = db_path()
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
        meta = {str(k): str(v or "") for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error as exc:
        log.warning("form13f: index at %s cannot be read: %s", path, exc)
        return None
    if meta.get("schema_version") not in READABLE_SCHEMAS:
        log.warning("form13f: index at %s has schema %r; ignoring", path, meta.get("schema_version"))
        conn.close()
        return None
    _SHARED["conn"], _SHARED["meta"] = conn, meta
    return conn, meta


def status() -> dict[str, Any]:
    """What ``/signalstats`` and the operator see: present or not, the period."""
    got = _connect()
    if got is None:
        return {"available": False}
    _, meta = got
    return {
        "available": True,
        "period": meta.get("period"),
        "filed_through": meta.get("filed_through"),
        "built_at": meta.get("built_at"),
        "managers": meta.get("managers"),
        "securities": meta.get("securities"),
    }


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state: the
    lookup simply carries no ``form13f`` event."""
    from .entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    url = get_settings().form13f_db_url
    path = db_path()
    if not url:
        state = "present" if path.exists() else "absent"
        return {"form13f": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"form13f": f"already present: {path}"}
            outcome = "replaced"
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"form13f": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        log.warning("form13f: asset check/download failed: %s", exc)
        return {"form13f": f"failed: {exc}"}


# ---------------------------------------------------------------------------
# From a company to its CUSIPs
# ---------------------------------------------------------------------------


def cusips_from_isins(isins: Iterable[str]) -> list[str]:
    """The CUSIP inside each US ISIN (``US`` + 9 + check digit)."""
    out: list[str] = []
    for isin in isins:
        isin = (isin or "").strip().upper()
        if len(isin) == 12 and isin.startswith("US") and _CUSIP_RE.match(isin[2:11]):
            out.append(isin[2:11])
    return out


def cusips_from_bods(bods: Iterable[dict[str, Any]]) -> list[str]:
    """The issuer CUSIPs the SEC EDGAR source read off the company's own
    Schedule 13D/13G filings (scheme ``CUSIP`` on its entity statement)."""
    from .bods.source_ids import source_ids_of

    out: list[str] = []
    for stmt in bods:
        if stmt.get("recordType") != "entity" or "sec_edgar" not in source_ids_of(stmt):
            continue
        for ident in (stmt.get("recordDetails") or {}).get("identifiers") or []:
            if (ident.get("scheme") or "").upper() == "CUSIP":
                cusip = (ident.get("id") or "").strip().upper()
                if _CUSIP_RE.match(cusip):
                    out.append(cusip)
    return out


def subject_cusips(lei: str | None, bods: Iterable[dict[str, Any]]) -> dict[str, list[str]]:
    """Every CUSIP the run can tie to the company, with how each was reached
    (``gleif_isin``, ``sec_edgar``), in first-seen order."""
    from . import isin_index

    found: dict[str, list[str]] = {}
    if lei and ":" not in lei:
        isins = isin_index.isins_with_prefix(lei, "US", limit=MAX_US_ISINS) or []
        for cusip in cusips_from_isins(isins):
            found.setdefault(cusip, []).append("gleif_isin")
    for cusip in cusips_from_bods(bods):
        via = found.setdefault(cusip, [])
        if "sec_edgar" not in via:
            via.append("sec_edgar")
    return found


# ---------------------------------------------------------------------------
# The payload
# ---------------------------------------------------------------------------


def _day(value: str | None) -> date | None:
    try:
        return date.fromisoformat((value or "")[:10])
    except ValueError:
        return None


def is_equity_cusip(cusip: str) -> bool:
    """A CUSIP whose issue number (characters 7–8) is two digits — an equity
    issue; a letter there marks a debt issue."""
    return len(cusip) == 9 and cusip[6:8].isdigit()


def is_adr(title: str | None, issuer: str | None = None) -> bool:
    return bool(_ADR_RE.search((title or "").upper()) or _ADR_RE.search((issuer or "").upper()))


def _millions(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:,.2f} billion"
    return f"{n / 1_000_000:,.1f} million" if n >= 1_000_000 else f"{n:,}"


def _human_day(d: date) -> str:
    months = ["Jan", "Feb", "Mar", "Apr", "May", "June", "July", "Aug", "Sept", "Oct", "Nov", "Dec"]
    return f"{d.day} {months[d.month - 1]} {d.year}"


def percent_basis(shares: int, as_of: date, legal_name: str | None) -> str:
    """The sentence a computed percentage always travels with."""
    whose = f"{legal_name}'s" if legal_name else "the company's"
    return (
        f"Of {_millions(shares)} shares outstanding on {whose} 10-Q/10-K cover, "
        f"{_human_day(as_of)} — computed by OpenCheck."
    )


def build_payload(
    lei: str | None,
    bods: Iterable[dict[str, Any]],
    *,
    issuer_cik: str | None = None,
    legal_name: str | None = None,
    today: date | None = None,
) -> dict[str, Any] | None:
    """The ``form13f`` event for a run, or ``None`` when there is nothing to
    say: no index, or no security of the company's in it.

    ``status`` is ``found`` (positions listed) or ``out_of_date`` (the index's
    period is older than ``form13f_max_age_days``: the section says so and
    lists nothing).
    """
    got = _connect()
    if got is None:
        return None
    conn, meta = got
    bods = list(bods)
    matched = subject_cusips(lei, bods)
    if not matched:
        return None
    cusips = list(matched)
    try:
        rows = conn.execute(
            f"SELECT cusip, issuer_name, title_of_class, positions, excluded_price, "
            f"excluded_name FROM securities WHERE cusip IN ({','.join('?' * len(cusips))})",
            cusips,
        ).fetchall()
    except sqlite3.Error as exc:
        log.warning("form13f: read failed: %s", exc)
        return None
    if not rows:
        return None

    by_cusip = {r[0]: r for r in rows}
    # Equity only — a CUSIP's two-character issue number is digits for an
    # equity issue and carries a letter for debt — and only what enough
    # managers reported; the rest is counted, never silently dropped.
    held = [c for c in cusips if c in by_cusip]
    ordered = [c for c in held if is_equity_cusip(c) and int(by_cusip[c][3]) >= MIN_MANAGERS]
    not_listed = len(held) - len(ordered)
    if not ordered:
        return None
    period = _day(meta.get("period"))
    today = today or datetime.now(timezone.utc).date()
    base: dict[str, Any] = {
        "period": meta.get("period"),
        "filed_through": meta.get("filed_through"),
        "built_at": meta.get("built_at"),
        "attribution": ATTRIBUTION,
        "caveat": CAVEAT,
        "disclaimer": DISCLAIMER,
        "notes": list(STANDING_NOTES),
        "source_url": SOURCE_URL,
    }
    if period is None or (today - period).days > get_settings().form13f_max_age_days:
        return {
            **base,
            "status": "out_of_date",
            "securities": [],
            "line": (
                f"The newest Form 13F data OpenCheck holds is for the quarter ending "
                f"{meta.get('period') or 'an unknown date'}, too old to show."
            ),
        }

    base["securities_not_listed"] = not_listed
    base["not_listed_note"] = (
        (
            f"1 more of the company's securities appears in Form 13F reports from fewer "
            f"than {MIN_MANAGERS} managers, or as debt, and is not listed."
            if not_listed == 1
            else f"{not_listed} more of the company's securities appear in Form 13F reports "
            f"from fewer than {MIN_MANAGERS} managers, or as debt, and are not listed."
        )
        if not_listed else None
    )
    classes = [c for c in ordered if not is_adr(by_cusip[c][2], by_cusip[c][1])]
    outstanding: tuple[int, date, str | None, str | None] | None = None
    if issuer_cik and len(classes) == 1:
        cik = str(issuer_cik).lstrip("0")
        fact = conn.execute(
            "SELECT shares, as_of, accession, entity_name FROM shares_outstanding WHERE cik = ?",
            (cik,),
        ).fetchone()
        as_of = _day(fact[1]) if fact else None
        if fact and as_of and abs((as_of - period).days) <= PERCENT_WINDOW_DAYS and fact[0] > 0:
            outstanding = (int(fact[0]), as_of, fact[2], fact[3])

    securities: list[dict[str, Any]] = []
    for cusip in ordered:
        _, issuer, title, kept, ex_price, ex_name = by_cusip[cusip]
        adr = is_adr(title, issuer)
        base_shares = outstanding if (outstanding and cusip in classes) else None
        positions: list[dict[str, Any]] = []
        over = 0
        for r in conn.execute(
            "SELECT p.rank, p.cik, p.shares, p.value, p.value_check, p.discretion, "
            "m.name, m.filed, m.amended, m.confidential_omitted, m.report_type, "
            "m.included_managers FROM positions p JOIN managers m USING (cik) "
            "WHERE p.cusip = ? ORDER BY p.rank",
            (cusip,),
        ):
            rank, cik, shares, value, check, discretion, name, filed, amended, conf, rtype, incl = r
            if base_shares and shares > base_shares[0]:
                over += 1  # cannot be right; left out and counted, never repaired
                continue
            if len(positions) >= TOP:
                continue
            thousands = check == "thousands"
            positions.append({
                "manager": name,
                "manager_cik": cik,
                "shares": shares,
                "value": None if thousands else value,
                "value_note": THOUSANDS_NOTE if thousands else None,
                "value_check": check,
                "percent": (
                    round(100 * shares / base_shares[0], 2) if base_shares else None
                ),
                "discretion": discretion,
                "filed": filed,
                "amended": bool(amended),
                "confidential_treatment": bool(conf),
                "report_type": rtype,
                "included_managers": incl,
            })
        reported = int(kept) - over
        securities.append({
            "cusip": cusip,
            "issuer_name": issuer,
            "title_of_class": title,
            "is_adr": adr,
            "adr_note": ADR_NOTE if adr else None,
            "matched_via": matched[cusip],
            "managers_reported": reported,
            "managers_shown": len(positions),
            "managers_more": max(reported - len(positions), 0),
            "left_out": int(ex_price) + int(ex_name) + over,
            "shares_outstanding": (
                {
                    "shares": base_shares[0],
                    "as_of": base_shares[1].isoformat(),
                    "accession": base_shares[2],
                    "basis": percent_basis(
                        base_shares[0], base_shares[1], base_shares[3] or legal_name
                    ),
                }
                if base_shares else None
            ),
            "positions": positions,
        })
    securities.sort(key=lambda s: (s["is_adr"], -s["managers_reported"]))
    return {
        **base,
        "status": "found",
        "securities": securities,
        "line": summary_line(securities, meta.get("period")),
    }


def summary_line(securities: list[dict[str, Any]], period: str | None) -> str:
    """One sentence for the MCP field and the reports' first line."""
    if not securities:
        return ""
    first = securities[0]
    top = (first.get("positions") or [None])[0]
    period_day = _day(period)
    when = f"as at {_human_day(period_day)}" if period_day else "for the latest quarter"
    count = first.get("managers_reported") or 0
    lead = (
        f"{count:,} institutional investment manager{'s' if count != 1 else ''} reported a "
        f"position in {first.get('issuer_name') or first['cusip']} "
        f"({first.get('title_of_class') or 'shares'}) on Form 13F, {when}"
    )
    if top:
        pct = f", {top['percent']:.2f}%" if top.get("percent") is not None else ""
        lead += f"; the largest is {top['manager']} with {top['shares']:,} shares{pct}"
    more = len(securities) - 1
    if more:
        lead += (
            ". 1 more of the company's securities is listed" if more == 1
            else f". {more} more of the company's securities are listed"
        )
    return lead + "."


def sentences(payload: dict[str, Any] | None) -> list[str]:
    """Every sentence a payload carries, for the vocabulary test."""
    if not payload:
        return []
    out = [
        payload.get("line") or "",
        payload.get("not_listed_note") or "",
        *(payload.get("notes") or []),
        payload.get("caveat") or "",
    ]
    for sec in payload.get("securities") or []:
        out += [sec.get("adr_note") or "", (sec.get("shares_outstanding") or {}).get("basis") or ""]
        out += security_footer(sec) + [security_heading(sec)]
        out += [p.get("value_note") or "" for p in sec.get("positions") or []]
    return [s for s in out if s]


# ---------------------------------------------------------------------------
# The reports' rows (PDF/HTML and Markdown share them)
# ---------------------------------------------------------------------------

REPORT_HEADING = "Institutional shareholders (Form 13F)"
REPORT_COLUMNS = ("Manager", "Shares", "Value (USD)", "% of shares outstanding")


def report_rows(security: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    """One security's positions as display strings: a value reported in
    thousands is shown as not shown, a percentage only where computed."""
    rows = []
    for p in security.get("positions") or []:
        value = (
            "not shown (reported in thousands)" if p.get("value_note")
            else (f"{p['value']:,}" if p.get("value") is not None else "—")
        )
        pct = f"{p['percent']:.2f}%" if p.get("percent") is not None else "—"
        rows.append((p.get("manager") or "", f"{p.get('shares') or 0:,}", value, pct))
    return rows


def security_heading(security: dict[str, Any]) -> str:
    title = security.get("title_of_class") or "shares"
    kind = " — American depositary shares" if security.get("is_adr") else ""
    return f"{security.get('issuer_name') or ''} ({title}), CUSIP {security.get('cusip')}{kind}"


def security_footer(security: dict[str, Any]) -> list[str]:
    """The sentences under a security's table."""
    out = []
    shown, total = security.get("managers_shown") or 0, security.get("managers_reported") or 0
    out.append(f"{total:,} managers reported a position; the {shown} largest by shares are shown.")
    basis = (security.get("shares_outstanding") or {}).get("basis")
    if basis:
        out.append(basis)
    if security.get("adr_note"):
        out.append(security["adr_note"])
    if security.get("left_out"):
        out.append(
            f"{security['left_out']} reported positions were left out because their figures "
            "cannot all be right (for example shares and value transposed)."
        )
    return out
