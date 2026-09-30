#!/usr/bin/env python3
"""Harvest and build the committed, LEI-keyed CIPA Botswana index.

Botswana's Companies and Intellectual Property Authority (CIPA) runs the
company register and the public beneficial ownership register on Foster
Moore's Verne / Catalyst platform (``www.cipa.co.bw``). There is no API, no
bulk file and no stated licence, so — like ``cac_nigeria`` — OpenCheck serves a
**curated, LEI-anchored example set** harvested offline and committed.

``harvest`` (the only command)
    Lists every LEI that GLEIF files under CIPA's Register of Companies
    (``RA000035``), plus the NBFIRA records (``RA000821``) that file a CIPA UIN,
    finds each company on the register by the number GLEIF files (the UIN, or
    an old company number such as ``CO1988/1163``), reads its public page and
    writes ``opencheck/data/cipa_botswana.json`` keyed by LEI. One company at a
    time, ``--pace`` seconds apart (default 2). Unlike the CAC set there is no
    separate build step: the register's names need no normalisation table.

How the register works (verified 29-30 Sept 2026)
-------------------------------------------------
Every page embeds its whole state as JSON (``var viewTree = {...}``) and every
action is a JSON ``POST /{app}/ui/{txId}`` carrying Catalyst commands.

* ``GET /master/ui/start/CIPARegisterSearch`` sets the session cookie and
  returns a service-transaction id (``Catalyst.useServiceTransactionId``).
* Search: ``view-node-set-attribute-value`` on the ``NameOrNumber`` field,
  then ``view-node-button-click`` on the Search button. Node ids are random
  per session, so nodes are found by ``attribute`` / ``nodetype`` / ``domain``.
  A search matches the UIN, the old company number and names (3 characters
  minimum; only the first 500 results of any search can be reached).
* Clicking a result answers ``{"redirect": ".../entityView/{id}?dom=Company"}``,
  a URL that only works inside the same session — never store it.
* Filings: ``view-node-fire-event`` ``ui-tabsSelect`` on the Filings tab, then
  ``ui-get-page`` on the ``completed-filing-history`` node; the transactions
  arrive in its ``widgetData``.

What is copied — an allowlist
-----------------------------
The page publishes addresses for every director, shareholder and beneficial
owner, uploaded documents and their download keys. The harvester copies an
**allowlist** of attributes per record (``_ALLOWED``) and never walks into an
address or document record, so a field CIPA adds later cannot reach the
repository by default. Stephen's decision (30 Sept 2026): beneficial owners,
shareholders and directors, **name and role**; no beneficial owner's address.
Secretaries and auditors are not read.

Usage::

    uv run python scripts/build_cipa_botswana_index.py harvest [--only UIN ...]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

DATA = Path(__file__).resolve().parent.parent / "opencheck" / "data"
INDEX_PATH = DATA / "cipa_botswana.json"

BASE = "https://www.cipa.co.bw"
SEARCH_START = f"{BASE}/master/ui/start/CIPARegisterSearch"
GLEIF = "https://api.gleif.org/api/v1/lei-records"
#: CIPA's Register of Companies, and NBFIRA — a handful of whose records file a
#: CIPA UIN (Stanbic Financial Services). Verified against GLEIF 29 Sept 2026.
RA_CODES = ("RA000035", "RA000821")
USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; research harvest)"

#: A CIPA number GLEIF can file: the UIN, or an old company number.
_UIN = re.compile(r"^BW\d{11}$")
_OLD = re.compile(r"^C[O0]\d{4}/\d+$", re.I)

#: The only attributes copied, per party record. Anything else — addresses,
#: documents, contact details — is never read.
_NAME_ATTRS = ("FirstName", "MiddleNames", "LastName")
_ALLOWED = {
    "Director": {
        "FirstName", "MiddleNames", "LastName", "Nationality",
        "StartDate", "EndDate", "HasNominator",
    },
    "Shareholder": {
        "FirstName", "MiddleNames", "LastName", "Nationality", "Name",
        "CountryOfRegistration", "SourceBusinessIdentifier",
        "StartDate", "EndDate", "HasNominator",
    },
    "BeneficialOwner": {
        "FirstName", "MiddleNames", "LastName", "Nationalities", "Name",
        "CountryOfRegistration", "EntityType", "SubType", "OriginalCompanyNumber",
        "SourceBusinessIdentifier", "StartDate", "EndDate",
        "Type", "Exact", "VotingExact", "OtherReason",
    },
}
#: Records never walked into: they hold addresses and uploaded documents.
_SKIP_DOMAIN = re.compile(r"Address|Document", re.I)


# ----------------------------------------------------------------------
# Reading a company page (pure — tested without the network)
# ----------------------------------------------------------------------


def iso_date(text: Any) -> str | None:
    """``'23 June 1969'`` -> ``'1969-06-23'``; anything else -> None."""
    s = " ".join(str(text or "").split())
    if not s:
        return None
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _join_name(parts: dict[str, Any]) -> str:
    return " ".join(
        " ".join(str(parts.get(k) or "").split()) for k in _NAME_ATTRS if parts.get(k)
    ).strip()


def _walk(vt: dict[str, Any], node_id: str, path: tuple[str, ...] = ()) -> Iterable[tuple[tuple[str, ...], dict[str, Any]]]:
    """Yield (domain path, attribute node) for every attribute under a node,
    never entering an address or document record."""
    node = vt.get(node_id)
    if not isinstance(node, dict):
        return
    dom = node.get("domain")
    if dom and _SKIP_DOMAIN.search(dom):
        return
    here = path + ((dom,) if dom else ())
    if node.get("nodetype") == "attribute":
        yield here, node
    for child in node.get("children") or []:
        yield from _walk(vt, child, here)


def _records(vt: dict[str, Any], domain: str) -> list[str]:
    return [k for k, v in vt.items() if isinstance(v, dict) and v.get("domain") == domain]


def _attr(vt: dict[str, Any], name: str) -> Any:
    for v in vt.values():
        if isinstance(v, dict) and v.get("nodetype") == "attribute" and v.get("attribute") == name:
            return v.get("attributeValue")
    return None


def _party(vt: dict[str, Any], rec_id: str, domain: str) -> dict[str, Any]:
    """One party record, reduced to its allowlisted attributes.

    Dates can sit at two levels: the role record carries the role's start and
    (for a former holder) end; the inner party record repeats the start. The
    outer value wins. A nominator's name sits under a ``Nominator`` record and
    is kept apart from the party's own name.
    """
    allowed = _ALLOWED[domain]
    out: dict[str, Any] = {}
    name: dict[str, Any] = {}
    nominator: dict[str, Any] = {}
    interests: list[dict[str, Any]] = []
    kind = None
    for path, node in _walk(vt, rec_id):
        attr = node.get("attribute")
        if attr not in allowed:
            continue
        val = node.get("attributeValue")
        if val in (None, ""):
            continue
        inner = path[1:]  # below the role record itself
        if inner and kind is None:
            kind = inner[0]
        if "Nominator" in inner:
            if attr in _NAME_ATTRS:
                nominator.setdefault(attr, val)
            continue
        if "Interest" in inner:
            idx = _interest_index(vt, rec_id, node["id"])
            while len(interests) <= idx:
                interests.append({})
            interests[idx].setdefault(attr, val)
            continue
        if attr in _NAME_ATTRS:
            name.setdefault(attr, val)
        elif attr in ("StartDate", "EndDate"):
            # Outer (role-level) dates first: a shallower path wins.
            if len(inner) == 0 or attr not in out:
                out[attr] = val
        else:
            out.setdefault(attr, val)
    rec: dict[str, Any] = {"kind": kind}
    full = _join_name(name)
    if full:
        rec["name"] = full
    elif out.get("Name"):
        rec["name"] = " ".join(str(out["Name"]).split())
    for src, dst in (
        ("Nationality", "nationality"),
        ("CountryOfRegistration", "country"),
        ("SourceBusinessIdentifier", "uin"),
        ("OriginalCompanyNumber", "company_number"),
        ("EntityType", "entity_type"),
        ("SubType", "sub_type"),
    ):
        if out.get(src):
            rec[dst] = str(out[src]).strip()
    if out.get("Nationalities"):
        rec["nationalities"] = [
            c.strip() for c in str(out["Nationalities"]).split(",") if c.strip()
        ]
    if rec.get("uin") and not _UIN.match(rec["uin"]):
        # OtherEntity carries a stray internal number ("147"), not a UIN.
        rec.pop("uin")
    rec["start"] = iso_date(out.get("StartDate"))
    rec["end"] = iso_date(out.get("EndDate"))
    if str(out.get("HasNominator")).lower() == "true":
        rec["nominee"] = True
        nom = _join_name(nominator)
        if nom:
            rec["nominator"] = nom
    if interests:
        rec["interests"] = [
            {
                "type": i.get("Type"),
                "share": _num(i.get("Exact")),
                "voting": _num(i.get("VotingExact")),
                "other_reason": " ".join(str(i.get("OtherReason") or "").split()) or None,
            }
            for i in interests
            if i.get("Type")
        ]
    return rec


def _interest_index(vt: dict[str, Any], rec_id: str, attr_id: str) -> int:
    """Which ``Interest`` record (in document order) holds this attribute."""
    order: list[str] = []

    def walk(n: str, owner: str | None) -> str | None:
        v = vt.get(n)
        if not isinstance(v, dict):
            return None
        if v.get("domain") == "Interest":
            order.append(n)
            owner = n
        if n == attr_id:
            return owner
        for c in v.get("children") or []:
            found = walk(c, owner)
            if found is not None:
                return found
        return None

    owner = walk(rec_id, None)
    return order.index(owner) if owner in order else 0


def _num(v: Any) -> float | int | None:
    try:
        f = float(str(v).strip())
    except (TypeError, ValueError):
        return None
    return int(f) if f.is_integer() else f


def _allocations(vt: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Shares held, keyed by shareholder name as the allocation labels it."""
    out: dict[str, dict[str, Any]] = {}
    for rec in _records(vt, "ShareAllocation"):
        holder = shares = pct = None
        for _path, node in _walk(vt, rec):
            a = node.get("attribute")
            if a == "ShareholderIds" and holder is None:
                holder = (node.get("kv") or {}).get("ui-readonly-value")
            elif a == "NumberOfShares" and shares is None:
                shares = _num(node.get("attributeValue"))
            elif a == "PercentageAllocated" and pct is None:
                pct = _num(node.get("attributeValue"))
        if not holder:
            continue
        # A Botswana company is labelled "Name (BW00003761376)" here and
        # "Name" on its shareholder record.
        key = _holder_key(holder)
        slot = out.setdefault(key, {"shares": 0, "percentage": None})
        slot["shares"] += shares or 0
        if pct is not None:
            slot["percentage"] = (slot["percentage"] or 0) + pct
    return out


def _holder_key(name: Any) -> str:
    text = re.sub(r"\s*\(BW\d{11}\)\s*$", "", str(name or ""))
    return " ".join(text.split()).lower()


def read_company(vt: dict[str, Any]) -> dict[str, Any]:
    """Reduce one company page's view tree to the allowlisted record."""
    total = _num(_attr(vt, "TotalShare"))
    allocations = _allocations(vt)
    shareholders = []
    for rec_id in _records(vt, "Shareholder"):
        sh = _party(vt, rec_id, "Shareholder")
        alloc = allocations.get(_holder_key(sh.get("name")))
        if alloc:
            sh["shares"] = alloc["shares"] or None
            pct = alloc["percentage"]
            if pct is None and total and alloc["shares"]:
                pct = round(100 * alloc["shares"] / total, 2)
            sh["percentage"] = pct
        shareholders.append(sh)
    suffix = _attr(vt, "Suffix")
    return {
        "company": _company_name(vt),
        "uin": _attr(vt, "CompanyNumber"),
        "old_number": _attr(vt, "OldCompanyNumber"),
        "entity_type": _attr(vt, "EntityType"),
        "sub_type": _attr(vt, "SubType"),
        "suffix": suffix,
        "status": _status(vt),
        "status_since": _status_since(vt),
        "incorporated": iso_date(_attr(vt, "RegistrationDate")),
        "reregistered": iso_date(_attr(vt, "ReregistrationDate")),
        "annual_return_filed": iso_date(_attr(vt, "LastRenewalDate")),
        "total_shares": total,
        "directors": [_party(vt, r, "Director") for r in _records(vt, "Director")],
        "shareholders": shareholders,
        "beneficial_owners": [
            _party(vt, r, "BeneficialOwner") for r in _records(vt, "BeneficialOwner")
        ],
    }


def _company_name(vt: dict[str, Any]) -> str | None:
    # The heading box ("Name (BW00000466545)") sits under the entity-heading.
    for v in vt.values():
        if isinstance(v, dict) and v.get("widget") == "entity-heading":
            for child in v.get("children") or []:
                c = vt.get(child) or {}
                label = ((c.get("text") or {}).get("label") or "").strip()
                m = re.match(r"^(.*)\s+\((BW\d+)\)$", label)
                if m:
                    return m.group(1).strip()
    return None


def _status(vt: dict[str, Any]) -> str | None:
    for rec in _records(vt, "Status"):
        for _p, node in _walk(vt, rec):
            if node.get("attribute") == "Status":
                return node.get("attributeValue")
    return _attr(vt, "Status")


def _status_since(vt: dict[str, Any]) -> str | None:
    for rec in _records(vt, "Status"):
        for _p, node in _walk(vt, rec):
            if node.get("attribute") == "StartDate":
                return iso_date(node.get("attributeValue"))
    return None


def read_filings(widget_data: dict[str, Any] | None) -> list[dict[str, Any]]:
    """The completed filings, newest first: date and filing names only."""
    out = []
    for t in (widget_data or {}).get("transactions") or []:
        when = ((t.get("transactionDate") or {}).get("value") or "")[:10]
        names = [
            f.get("filingName") or f.get("name")
            for f in t.get("filings") or []
            if f.get("filingName") or f.get("name")
        ]
        out.append({
            "date": when or None,
            "service": t.get("name") or t.get("serviceTitle"),
            "filings": names,
        })
    out.sort(key=lambda f: f.get("date") or "", reverse=True)
    return out


# ----------------------------------------------------------------------
# The network half
# ----------------------------------------------------------------------


def _client():
    import httpx

    return httpx.Client(
        headers={"User-Agent": USER_AGENT},
        timeout=60.0,
        follow_redirects=True,
    )


def _page_state(html: str) -> tuple[str, dict[str, Any]]:
    tx = re.search(r'useServiceTransactionId = "([^"]+)"', html)
    vt = re.search(r"var viewTree = (\{.*?\});\n", html, flags=re.S)
    if not tx or not vt:
        raise RuntimeError("page carries no Catalyst state — has the platform changed?")
    return tx.group(1), json.loads(vt.group(1))


def _node(vt: dict[str, Any], **match: Any) -> str:
    for k, v in vt.items():
        if isinstance(v, dict) and all(v.get(a) == b for a, b in match.items()):
            return k
    raise RuntimeError(f"no node matching {match}")


def _post(client, app: str, tx: str, commands: list[dict[str, Any]]) -> dict[str, Any]:
    r = client.post(
        f"{BASE}/{app}/ui/{tx}",
        json={"returnRootHtmlOnChange": False, "returnChangesOnly": False, "commands": commands},
        headers={"X-Requested-With": "XMLHttpRequest"},
    )
    r.raise_for_status()
    return r.json()


def _norm_number(s: str) -> str:
    s = re.sub(r"\s+", "", s or "").upper()
    return re.sub(r"^C0", "CO", s)


def find_company(client, number: str) -> tuple[str, dict[str, Any]] | None:
    """Search the register for ``number`` and open the one company it names.

    A UIN is taken from the result label; an old number can match several
    companies (``CO2016/3896`` returns three), so each candidate is opened and
    kept only if its own old number equals the query.
    """
    query = _norm_number(number)
    r = client.get(SEARCH_START)
    r.raise_for_status()
    tx, vt = _page_state(r.text)
    state = _post(client, "master", tx, [
        {"type": "view-node-set-attribute-value", "id": _node(vt, attribute="NameOrNumber"), "value": query},
        {"type": "view-node-button-click", "id": _node(vt, nodetype="button")},
    ])["state"]
    hits = []
    for k, v in state.items():
        if isinstance(v, dict) and v.get("nodetype") == "button":
            m = re.search(r"\((BW\d{11})\)\s*$", (v.get("text") or {}).get("label") or "")
            if m:
                hits.append((k, m.group(1)))
    if _UIN.match(query):
        hits = [h for h in hits if h[1] == query]
    for button, _uin in hits[:5]:
        redirect = _post(client, "master", tx, [{"type": "view-node-button-click", "id": button}]).get("redirect")
        if not redirect:
            continue
        page = client.get(redirect)
        page.raise_for_status()
        ptx, pvt = _page_state(page.text)
        if _UIN.match(query) or _norm_number(_attr(pvt, "OldCompanyNumber") or "") == query:
            return ptx, pvt
    return None


def fetch_filings(client, tx: str, vt: dict[str, Any]) -> dict[str, Any] | None:
    try:
        tabs = _node(vt, widget="tabs")
        filings_tab = vt[tabs]["children"][-1]
        state = _post(client, "companies", tx, [
            {"type": "view-node-fire-event", "id": filings_tab, "name": "ui-tabsSelect"},
        ])["state"]
        widget = _node(state, widget="completed-filing-history")
        state = _post(client, "companies", tx, [
            {"type": "view-node-fire-event", "id": widget, "name": "ui-get-page"},
        ])["state"]
    except Exception as exc:  # noqa: BLE001 — filings are optional
        print(f"    filings unavailable: {exc}", file=sys.stderr)
        return None
    for v in state.values():
        if isinstance(v, dict) and v.get("widgetData", {}).get("transactions") is not None:
            return v["widgetData"]
    return None


def gleif_seed() -> list[dict[str, Any]]:
    """Every LEI GLEIF files under CIPA's RA codes with a CIPA-shaped number."""
    import httpx

    out = []
    for ra in RA_CODES:
        page = 1
        while True:
            r = httpx.get(GLEIF, params={
                "filter[entity.registeredAt]": ra,
                "page[size]": 200,
                "page[number]": page,
            }, timeout=60.0)
            r.raise_for_status()
            data = r.json()
            for rec in data.get("data") or []:
                a = rec["attributes"]
                number = (a["entity"].get("registeredAs") or "").strip()
                if not (_UIN.match(number) or _OLD.match(_norm_number(number))):
                    continue
                if a["registration"]["status"] == "ANNULLED":
                    continue
                out.append({
                    "lei": rec["id"],
                    "gleif_name": a["entity"]["legalName"]["name"],
                    "registered_as": number,
                    "ra_code": ra,
                    "lei_status": a["registration"]["status"],
                })
            pag = data["meta"]["pagination"]
            if pag["currentPage"] >= pag["lastPage"]:
                break
            page += 1
    return out


def harvest(pace: float, only: list[str]) -> dict[str, Any]:
    seed = gleif_seed()
    if only:
        seed = [s for s in seed if s["registered_as"] in only or s["lei"] in only]
    print(f"{len(seed)} LEIs filed under {', '.join(RA_CODES)} with a CIPA number")
    companies, unresolved = [], []
    for s in seed:
        with _client() as client:
            try:
                found = find_company(client, s["registered_as"])
            except Exception as exc:  # noqa: BLE001
                print(f"  {s['registered_as']}: error {exc}")
                unresolved.append({**s, "reason": f"error: {type(exc).__name__}"})
                time.sleep(pace)
                continue
            if found is None:
                print(f"  {s['registered_as']}: not found or ambiguous")
                unresolved.append({**s, "reason": "no single company on the register"})
                time.sleep(pace)
                continue
            tx, vt = found
            rec = read_company(vt)
            rec["filings"] = read_filings(fetch_filings(client, tx, vt))
        rec.update({k: s[k] for k in ("lei", "lei_status", "registered_as", "ra_code", "gleif_name")})
        companies.append(rec)
        print(
            f"  {s['registered_as']} -> {rec['uin']} {rec['company']}: "
            f"{len(rec['beneficial_owners'])} BO, {len(rec['shareholders'])} shareholders, "
            f"{len(rec['directors'])} directors"
        )
        time.sleep(pace)
    raw = {
        "meta": {
            "register": "Companies and Intellectual Property Authority (CIPA), Botswana",
            "source_url": BASE,
            "ra_codes": list(RA_CODES),
            "harvested": date.today().isoformat(),
            "note": (
                "Curated example set read from CIPA's public register pages "
                "(beneficial owners, shareholders and directors: name and role, "
                "nationality or country, dates, shares). No addresses, documents "
                "or contact details."
            ),
        },
        "companies": companies,
        "unresolved": unresolved,
    }
    return raw


# ----------------------------------------------------------------------
# The index — keyed by LEI
# ----------------------------------------------------------------------


def build(raw: dict[str, Any]) -> dict[str, Any]:
    index: dict[str, Any] = {}
    for rec in raw.get("companies") or []:
        lei = str(rec.get("lei") or "").strip().upper()
        if len(lei) != 20 or not rec.get("uin"):
            continue
        index[lei] = rec
    meta = dict(raw.get("meta") or {})
    meta.update({
        "entities": len(index),
        "beneficial_owners": sum(len(r["beneficial_owners"]) for r in index.values()),
        "shareholders": sum(len(r["shareholders"]) for r in index.values()),
        "directors": sum(len(r["directors"]) for r in index.values()),
        "unresolved": raw.get("unresolved") or [],
    })
    out = {"meta": meta, "index": dict(sorted(index.items()))}
    INDEX_PATH.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {INDEX_PATH} — {meta['entities']} companies, {len(meta['unresolved'])} unresolved")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=("harvest",))
    ap.add_argument("--pace", type=float, default=2.0, help="seconds between companies")
    ap.add_argument("--only", nargs="*", default=[], help="restrict to these CIPA numbers or LEIs")
    args = ap.parse_args()
    build(harvest(args.pace, args.only))


if __name__ == "__main__":
    main()
