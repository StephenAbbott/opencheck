"""Build the Open DART crosswalk: Korean LEIs → DART ``corp_code``.

DART keys every filing on its own eight-digit ``corp_code``. GLEIF files a
Korean company's business registration number (사업자등록번호, ``RA000657``)
or corporate registration number (법인등록번호, ``RA000419``). Neither number
is in DART's bulk filer list (``corpCode.xml``), so the join is **name, then
number** (decision A on the Notion ticket "South Korea / Open DART disclosure
data", 10 Nov 2026 — an LEI crosswalk only, not the whole register):

1. Read every GLEIF record under the two authorities (GLEIF API, keyless).
2. Download ``corpCode.xml`` (one call) and index its Korean and English names.
3. For each LEI, the filers whose normalised name equals one of the LEI's
   names are candidates; each candidate's ``company.json`` is read, and the
   LEI joins only when the filer's **own** ``bizr_no`` / ``jurir_no`` equals the
   number GLEIF files. A name alone never joins.
4. Every candidate checked is kept (its numbers and names) in ``filer``, so the
   adapter can answer for any number it holds, and the next build reuses an
   answer whose ``modify_date`` has not moved (``--previous``).

The index holds no filing. ``dart_korea.fetch`` reads the filings live.

Usage
-----
    OPENDART_API_KEY=… python3 scripts/build_dart_korea_index.py \\
        --out dart_korea.sqlite [--previous old.sqlite] [--gzip] [--max-calls 4500]

Standard library only. ``.github/workflows/refresh-dart-korea-index.yml`` runs
it weekly and replaces the ``dart-korea-index`` release asset; the backend
downloads it at boot (``DART_KOREA_DB_URL``). The key is read from the
environment and never written anywhere.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import gzip
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.dart_korea import (  # noqa: E402
    API_BASE,
    INDEX_SCHEMA_VERSION,
    KR_BRN_RA_CODE,
    KR_CRN_RA_CODE,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_dart_korea_index")

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"
GLEIF_API = "https://api.gleif.org/api/v1/lei-records"
RA_CODES = (KR_BRN_RA_CODE, KR_CRN_RA_CODE)
#: Registration statuses kept: an annulled or duplicate LEI is no company.
SKIP_REGISTRATION = frozenset({"ANNULLED", "DUPLICATE"})
#: More candidates than this for one LEI is a generic name; the first ones
#: (by name match order) are still checked.
MAX_CANDIDATES = 8
#: Below this many filers ``corpCode.xml`` is not DART's list.
MIN_FILERS = 50_000
#: Below this many GLEIF records the GLEIF read went wrong.
MIN_GLEIF = 500

#: Korean legal-form tokens removed before names are compared.
_KO_FORMS = (
    "유한책임회사", "주식회사", "유한회사", "합자회사", "합명회사", "사단법인", "재단법인",
    "(주)", "㈜", "(유)", "(사)", "(재)",
)
_EN_FORMS = re.compile(
    r"\b(co|company|corp|corporation|inc|incorporated|ltd|limited|llc|plc|"
    r"jusikhoesa|chusik|hoesa)\b",
    re.IGNORECASE,
)

SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE filer (
  corp_code TEXT PRIMARY KEY, corp_name TEXT, corp_name_eng TEXT,
  stock_code TEXT, corp_cls TEXT, jurir_no TEXT, bizr_no TEXT, modify_date TEXT
);
CREATE TABLE crosswalk (
  lei TEXT PRIMARY KEY, ra_code TEXT NOT NULL, registered_as TEXT NOT NULL,
  corp_code TEXT NOT NULL, registration_status TEXT
);
CREATE INDEX filer_bizr ON filer (bizr_no);
CREATE INDEX filer_jurir ON filer (jurir_no);
CREATE INDEX crosswalk_corp ON crosswalk (corp_code);
"""


def _get(url: str, *, timeout: float = 120.0, attempts: int = 4) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 — fixed https hosts
                return response.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == attempts - 1:
                # Never the URL: a DART URL carries the key.
                raise RuntimeError(f"request failed: {type(exc).__name__}") from None
            time.sleep(2 ** (attempt + 1))
    raise RuntimeError("unreachable")


def ko_key(name: str) -> str:
    text = unicodedata.normalize("NFKC", str(name or ""))
    for form in _KO_FORMS:
        text = text.replace(unicodedata.normalize("NFKC", form), "")
    return re.sub(r"[\s\W_]+", "", text).casefold()


def en_key(name: str) -> str:
    text = unicodedata.normalize("NFKC", str(name or "")).casefold().replace("&", " and ")
    text = _EN_FORMS.sub(" ", text)
    return re.sub(r"[\W_]+", "", text)


def digits(value: object) -> str:
    return re.sub(r"\D", "", str(value or ""))


# ---------------------------------------------------------------------------
# GLEIF
# ---------------------------------------------------------------------------


def gleif_records() -> list[dict]:
    """Every LEI record under the two Korean authorities: LEI, RA code, the
    number's digits, registration status and every name (legal, other,
    transliterated)."""
    records: list[dict] = []
    for ra in RA_CODES:
        page = 1
        while True:
            query = urllib.parse.urlencode(
                {"filter[entity.registeredAt]": ra, "page[size]": 200, "page[number]": page}
            )
            body = json.loads(_get(f"{GLEIF_API}?{query}"))
            for item in body.get("data") or []:
                attrs = item.get("attributes") or {}
                entity = attrs.get("entity") or {}
                status = (attrs.get("registration") or {}).get("status")
                if status in SKIP_REGISTRATION:
                    continue
                names = [(entity.get("legalName") or {}).get("name") or ""]
                names += [o.get("name") or "" for o in entity.get("otherNames") or []]
                names += [o.get("name") or "" for o in entity.get("transliteratedOtherNames") or []]
                records.append({
                    "lei": item.get("id"),
                    "ra_code": ra,
                    "registered_as": entity.get("registeredAs") or "",
                    "number": digits(entity.get("registeredAs")),
                    "registration_status": status,
                    "names": [n for n in names if n],
                })
            last = ((body.get("meta") or {}).get("pagination") or {}).get("lastPage") or 1
            if page >= last:
                break
            page += 1
            time.sleep(0.3)
        log.info("GLEIF %s: %d records so far", ra, len(records))
    return records


# ---------------------------------------------------------------------------
# DART
# ---------------------------------------------------------------------------


def dart_json(endpoint: str, key: str, **params: str) -> dict:
    query = urllib.parse.urlencode({"crtfc_key": key, **params})
    body = json.loads(_get(f"{API_BASE}/{endpoint}.json?{query}", timeout=60.0))
    if not isinstance(body, dict):
        raise RuntimeError(f"{endpoint}: unexpected shape")
    return body


def corp_codes(key: str, local_zip: Path | None = None) -> list[dict]:
    """``corpCode.xml``: a ZIP holding one XML of every filer. ``local_zip``
    reads a copy already downloaded (the download is slow from a datacentre)."""
    if local_zip is not None:
        payload = local_zip.read_bytes()
    else:
        payload = _get(f"{API_BASE}/corpCode.xml?{urllib.parse.urlencode({'crtfc_key': key})}", timeout=900.0)
    return parse_corp_codes(payload)


def parse_corp_codes(payload: bytes) -> list[dict]:
    if not payload.startswith(b"PK"):
        # Open DART answers an error as JSON or XML with a status, not a ZIP.
        raise RuntimeError(f"corpCode.xml is not a ZIP: {payload[:200]!r}")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        xml = archive.read(archive.namelist()[0])
    rows = []
    for node in ElementTree.fromstring(xml).iter("list"):
        row = {child.tag: (child.text or "").strip() for child in node}
        if row.get("corp_code"):
            rows.append(row)
    return rows


class Budget:
    def __init__(self, cap: int) -> None:
        self.cap = cap
        self.sent = 0

    def take(self) -> bool:
        if self.sent >= self.cap:
            return False
        self.sent += 1
        return True


def read_profile(key: str, corp_code: str) -> dict | None:
    """``company.json`` for one filer: its numbers and names, or None for an
    answer that is not a profile. Raises on a key or quota refusal."""
    body = dart_json("company", key, corp_code=corp_code)
    status = str(body.get("status") or "")
    if status == "000":
        return body
    if status == "013":
        return None
    if status in ("010", "011", "012", "020", "901", "800"):
        raise RuntimeError(f"Open DART refused: status {status} ({body.get('message')})")
    log.warning("company.json %s: status %s", corp_code, status)
    return None


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------


def previous_filers(path: Path | None) -> dict[str, dict]:
    if not path or not path.exists():
        return {}
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return {row["corp_code"]: dict(row) for row in conn.execute("SELECT * FROM filer")}
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def build(
    out: Path, *, key: str, previous: Path | None, max_calls: int, corpcode_zip: Path | None = None
) -> dict:
    started = datetime.now(UTC)
    records = gleif_records()
    if len(records) < MIN_GLEIF:
        raise SystemExit(f"Refusing: only {len(records)} GLEIF records under {RA_CODES}")
    filers = corp_codes(key, corpcode_zip)
    if len(filers) < MIN_FILERS:
        raise SystemExit(f"Refusing: corpCode.xml lists only {len(filers)} filers")
    log.info("corpCode.xml: %d filers", len(filers))

    by_name: dict[str, list[str]] = {}
    filer_rows = {f["corp_code"]: f for f in filers}
    for f in filers:
        for k in {ko_key(f.get("corp_name")), en_key(f.get("corp_eng_name"))} - {""}:
            by_name.setdefault(k, []).append(f["corp_code"])

    known = previous_filers(previous)
    checked: dict[str, dict] = {}
    budget = Budget(max_calls)
    stopped_on_budget = False

    def _profile(code: str) -> dict | None:
        prior = known.get(code)
        listed = filer_rows.get(code) or {}
        if prior and prior.get("modify_date") == listed.get("modify_date"):
            return prior
        if not budget.take():
            return None
        body = read_profile(key, code)
        if body is None:
            return None
        return {
            "corp_code": code,
            "corp_name": body.get("corp_name") or listed.get("corp_name"),
            "corp_name_eng": body.get("corp_name_eng") or listed.get("corp_eng_name"),
            "stock_code": (body.get("stock_code") or "").strip() or None,
            "corp_cls": body.get("corp_cls") or None,
            "jurir_no": digits(body.get("jurir_no")) or None,
            "bizr_no": digits(body.get("bizr_no")) or None,
            "modify_date": listed.get("modify_date"),
        }

    candidates: dict[str, list[str]] = {}
    for record in records:
        seen: list[str] = []
        for name in record["names"]:
            for k in (ko_key(name), en_key(name)):
                for code in by_name.get(k, []):
                    if code not in seen:
                        seen.append(code)
        candidates[record["lei"]] = seen[:MAX_CANDIDATES]
    wanted = sorted({c for codes in candidates.values() for c in codes})
    log.info("%d LEIs have a name candidate; %d distinct filers to check", sum(1 for v in candidates.values() if v), len(wanted))

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for code, profile in zip(wanted, pool.map(_profile, wanted)):
            if profile is not None:
                checked[code] = profile
            elif budget.sent >= budget.cap and code not in known:
                stopped_on_budget = True

    crosswalk = []
    for record in records:
        column = "bizr_no" if record["ra_code"] == KR_BRN_RA_CODE else "jurir_no"
        match = next(
            (code for code in candidates[record["lei"]]
             if (checked.get(code) or {}).get(column) and checked[code][column] == record["number"]),
            None,
        )
        if match:
            crosswalk.append((record["lei"], record["ra_code"], record["registered_as"], match, record["registration_status"]))

    tmp = out.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    conn = sqlite3.connect(tmp)
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT INTO filer VALUES (:corp_code, :corp_name, :corp_name_eng, :stock_code, :corp_cls, :jurir_no, :bizr_no, :modify_date)",
        list(checked.values()),
    )
    conn.executemany("INSERT INTO crosswalk VALUES (?, ?, ?, ?, ?)", crosswalk)
    issued = {r["lei"] for r in records if r["registration_status"] == "ISSUED"}
    meta = {
        "schema_version": INDEX_SCHEMA_VERSION,
        "built_at": started.isoformat(timespec="seconds").replace("+00:00", "Z"),
        "gleif_records": str(len(records)),
        "gleif_issued": str(len(issued)),
        "corpcode_filers": str(len(filers)),
        "filers_checked": str(len(checked)),
        "leis_matched": str(len(crosswalk)),
        "issued_matched": str(sum(1 for row in crosswalk if row[0] in issued)),
        "dart_calls": str(budget.sent),
        "complete": "false" if stopped_on_budget else "true",
    }
    conn.executemany("INSERT INTO meta VALUES (?, ?)", sorted(meta.items()))
    conn.commit()
    conn.close()
    os.replace(tmp, out)
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--previous", type=Path, default=None, help="the last build, to reuse answers")
    parser.add_argument("--max-calls", type=int, default=4500, help="company.json calls at most (daily cap 20,000 a key)")
    parser.add_argument("--corpcode-zip", type=Path, default=None, help="a corpCode.xml ZIP already downloaded")
    parser.add_argument("--gzip", action="store_true")
    args = parser.parse_args(argv)
    key = os.environ.get("OPENDART_API_KEY", "").strip()
    if not key:
        parser.error("OPENDART_API_KEY is not set")
    meta = build(args.out, key=key, previous=args.previous, max_calls=args.max_calls, corpcode_zip=args.corpcode_zip)
    log.info("built %s: %s", args.out, json.dumps(meta, sort_keys=True))
    if args.gzip:
        with open(args.out, "rb") as src, gzip.open(f"{args.out}.gz", "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
