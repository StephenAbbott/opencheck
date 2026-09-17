"""Serbia — APR, the Business Registers Agency's company register (monthly open data).

The Agencija za privredne registre (APR, Business Registers Agency) publishes
its register of companies (*Регистар привредних друштава*) through a keyless
open-data API at ``openapi.apr.gov.rs``. It is **bulk only**: one ``GET``
returns the whole register as a single JSON object — about 58 MB and 134,000
companies, cut on the last day of each month — and every query parameter and
per-company path tried (``?maticniBroj=``, ``?id=``, ``/companies/{mb}``) is
either ignored or answers 404. So this adapter keeps a local SQLite index over
the latest cut and answers from it, keyed on the **matični broj** — the
8-digit company registration number, which is what GLEIF files in
``registeredAs`` under ``RA000517``.

The payload is ``{"DatumPreseka": "2026-08-31", "Podaci": {"<matični broj>":
{...}, ...}}``: a cut date, then a map keyed on the registration number.

Why the index builds itself
---------------------------
``openapi.apr.gov.rs`` does not block datacentre networks: on 17 September
2026 a cloud container downloaded the full 58 MB in 15 s — the vantage point
that fails alongside Render for ``data.gov.ro``. So, as for Moldova, the index
is not a file somebody builds on a laptop and ships in. On a live deployment
the adapter downloads the register and builds the index in a background
thread at startup, and checks for a newer cut once the one it holds is more
than a month old. The response carries no ``ETag``, ``Last-Modified`` or
``Content-Length`` and is not compressed, so the check reads just the opening
bytes of the response — where ``DatumPreseka`` sits — and closes the
connection unless the cut is newer. ``APR_SERBIA_DB_FILE`` pins a location;
``APR_SERBIA_SYNC=false`` turns the download off and uses the file as found.

APR's server does not send its intermediate certificate
-------------------------------------------------------
``openapi.apr.gov.rs`` presents only its leaf certificate (``*.apr.gov.rs``,
issued by *SSL2BUY EMEA RSA Domain Validation Secure Server CA*), not the
intermediate that chains it to a trusted root — verified from a residential
connection with ``openssl s_client``: *unable to verify the first
certificate*. Browsers repair that silently by fetching the intermediate from
the certificate's AIA URL; Python's ``ssl`` does not, so a plain ``httpx.get``
fails verification everywhere. Verification is **not** turned off. The
intermediate is pinned below (:data:`APR_INTERMEDIATE_PEM`) and added to the
trusted roots for this adapter's requests only, so the chain still has to end
at *Sectigo Public Server Authentication Root R46* in the normal trust store.
The leaf expires on 13 November 2026; if the renewed certificate is issued by
a different intermediate the source-health probe goes red rather than the
index quietly ageing.

The JSON is read as a stream
----------------------------
``json.load`` peaks at about 210 MB per 58 MB file, on a host that has run
out of memory at 512 MB before. :func:`iter_register` walks the ``Podaci``
map one record at a time with ``json.JSONDecoder.raw_decode`` over a small
rolling buffer, so memory stays flat whatever the register's size.

Scope — what Stephen decided on 17 September 2026
-------------------------------------------------
* **The full register**, not only the ~300 Serbian LEI holders: 134,000 rows
  is a 35 MB index, and it builds without asking GLEIF anything.
* **Entity data only.** APR's financial-statements feed (assets, revenue,
  profit, headcount) is left for a later phase; so is the non-governmental
  organisations register, which no Serbian LEI record points at.
* **All four statuses APR publishes** are indexed: active, in liquidation, in
  bankruptcy and in forced liquidation. A company in insolvency is exactly
  what a due diligence check needs to see, so it is ``pending``, never
  dropped. A status APR has not published before fails the build loudly.
* **No name gate.** The matični broj on GLEIF's own record is an exact
  identifier, and GLEIF often holds the company in the other script or under
  a former name (PEOPLICITY → DATABLOOZ) — the Romanian CUI decision again.

What the register publishes, and what it does not
-------------------------------------------------
Business name as registered, municipality (code and name), status, founding
date, legal form and activity code. **No address** — the municipality is the
finest location published — and **no people**: no directors, members,
shareholders or beneficial owners. Every mapped record is one entity
statement. Deleted companies are not in the feed, and neither are
entrepreneurs (*предузетници*), who sit on a separate APR register.

Names are filed in Cyrillic or Latin, whichever the company registered. A
Cyrillic name gets its official Serbian Latin form (Gajica — a fixed
one-to-one letter table, so no guessing) as an alternate name.

Licence
-------
Serbian Open Data Portal licence, SODL 1.0 (``data.gov.rs/sr/terms``): reuse
for commercial and non-commercial purposes, copying, distribution and
adaptation, with attribution naming the publishing body, the download date
and address, and any changes made.
"""

from __future__ import annotations

import asyncio
import codecs
import json
import logging
import os
import re
import sqlite3
import ssl
import tempfile
import threading
import time
import unicodedata
from collections.abc import Iterator
from datetime import date, datetime, timezone
from pathlib import Path
from typing import IO, Any

from .. import degradation, provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.apr_serbia import AprSerbiaBundle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# GLEIF registration authorities
# ---------------------------------------------------------------------------
# All 304 GLEIF records with jurisdiction RS, 17 September 2026:
#
#   RA000517  Business Registers Agency (APR)       295   8-digit matični broj
#   RA999999  (no authority, no number)               6
#   RA000684  Securities Commission — fund numbers    2   "5/0-44-3581/4-10"
#   RA000518  APR register of entrepreneurs           1   sole trader
#
# Of the 146 ISSUED records, 141 resolve against the 31 August 2026 cut; the
# five that do not are two sole traders, the Chamber of Commerce and two
# RA999999 state bodies. Only RA000517 carries a number this register holds.
RS_APR_RA_CODE: str = "RA000517"
RS_RA_CODES: frozenset[str] = frozenset({RS_APR_RA_CODE})

RS_APR_SCHEME = "RS-APR"
RS_APR_SCHEME_NAME = "Matični broj — Business Registers Agency (Serbia)"

API_BASE = "https://openapi.apr.gov.rs/api/opendata"
COMPANIES_URL = f"{API_BASE}/companies"
DATASET_URL = "https://data.europa.eu/data/datasets/68000c424d29e8a004f93e04"
_HOMEPAGE = "https://www.apr.gov.rs/"

#: Bumped whenever the index tables change shape, so an old file is rebuilt.
INDEX_SCHEMA_VERSION = "1"

#: A cut older than this prompts a check for a newer one. APR cuts on the
#: last day of the month and publishes some days later.
REFRESH_AFTER_DAYS = 31

#: APR is asked at most this often per process.
_CHECK_INTERVAL_S = 12 * 3600

#: How long a lookup waits for a cold index build before degrading.
_BUILD_WAIT_S = 45.0

_LOOKUP_TIMEOUT_S = 60.0

#: A single register record is a few hundred bytes. Anything that has not
#: parsed by this size is a malformed file, not a record still arriving.
_MAX_RECORD_CHARS = 1 << 20

LICENSE_ID = "SODL-1.0"

COVERAGE_NOT_IN_REGISTER = (
    "No company under this registration number in the Business Registers "
    "Agency's open-data company register as indexed by OpenCheck. APR "
    "publishes companies that are active, in liquidation, in bankruptcy or in "
    "forced liquidation; deleted companies and entrepreneurs are not in it, "
    "so a company that has been struck off is not found here."
)

#: The intermediate certificate ``openapi.apr.gov.rs`` fails to send. Fetched
#: from the leaf's AIA URL
#: (http://crt.sectigo.com/SSL2BUYEMEARSADomainValidationSecureServerCA.crt),
#: issued by Sectigo Public Server Authentication Root R46, valid to
#: 2034-07-30. SHA-256 fingerprint
#: 58:F0:F7:56:75:8E:93:FB:0B:6B:17:A3:6A:38:50:47:5D:68:BC:0D:6C:99:CB:E2:2A:1B:18:35:1C:89:FF:1F
APR_INTERMEDIATE_PEM = """-----BEGIN CERTIFICATE-----
MIIGSzCCBDOgAwIBAgIRAJvUQHUeRiPp62D8LGqVNMcwDQYJKoZIhvcNAQEMBQAw
XzELMAkGA1UEBhMCR0IxGDAWBgNVBAoTD1NlY3RpZ28gTGltaXRlZDE2MDQGA1UE
AxMtU2VjdGlnbyBQdWJsaWMgU2VydmVyIEF1dGhlbnRpY2F0aW9uIFJvb3QgUjQ2
MB4XDTI0MDczMTAwMDAwMFoXDTM0MDczMDIzNTk1OVowZjELMAkGA1UEBhMCQUUx
GTAXBgNVBAoTEFNTTDJCVVkgRU1FQSBMTEMxPDA6BgNVBAMTM1NTTDJCVVkgRU1F
QSBSU0EgRG9tYWluIFZhbGlkYXRpb24gU2VjdXJlIFNlcnZlciBDQTCCAaIwDQYJ
KoZIhvcNAQEBBQADggGPADCCAYoCggGBALsbge3dwtQ6DNyJOcsZ+ZNJw/wpFgoW
xtz5f07UlYcc5ob1YU5hUbwTdfCHl3p2NiIjYedo2jb0qmgD8x0fycJ4yNG0hQRy
ptIUSkzyCEfG5QbNLK8osOPq/b274qVYuh7FeGk08JO050i7yJdUyYT3FlrTr/BA
hy2ckfwu27j63NVWzmT+piryjtCa2md1Txpmn0P2JedZKdzO3nopLYrvQ7ulxh0k
kZFG0QJbTwIC2miuExxTsLfr88dy2OiArQ7mts4aJJTWGxKGP14qTQiv9La5GUwV
y1j4tSxMVJtrQyhdXrAqydo7Z/CP8Crw/5aUlK1DRu7kTQ36S2CJD1M65h7sYaZc
TuXUGcgmeQXBeVsUYPQB7jsrkgaZLEwliyrWF8YUJcYfYHzKqb5TVuMljDsKLweI
1dowbB6B21QpWHwf0n2kCJPR0R2daKClSR9cVi/WyyMBy7zBnMVW6XvfQqH6qrs3
7oGjZhS+XmzgIJIrW9+G46zhw02QiyclLQIDAQABo4IBeTCCAXUwHwYDVR0jBBgw
FoAUVnNYZJX5khqwEioEYnmhQBWIIUkwHQYDVR0OBBYEFIFZSMm65QBv37f/UMWr
793m5K6QMA4GA1UdDwEB/wQEAwIBhjASBgNVHRMBAf8ECDAGAQH/AgEAMB0GA1Ud
JQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjATBgNVHSAEDDAKMAgGBmeBDAECATBU
BgNVHR8ETTBLMEmgR6BFhkNodHRwOi8vY3JsLnNlY3RpZ28uY29tL1NlY3RpZ29Q
dWJsaWNTZXJ2ZXJBdXRoZW50aWNhdGlvblJvb3RSNDYuY3JsMIGEBggrBgEFBQcB
AQR4MHYwTwYIKwYBBQUHMAKGQ2h0dHA6Ly9jcnQuc2VjdGlnby5jb20vU2VjdGln
b1B1YmxpY1NlcnZlckF1dGhlbnRpY2F0aW9uUm9vdFI0Ni5wN2MwIwYIKwYBBQUH
MAGGF2h0dHA6Ly9vY3NwLnNlY3RpZ28uY29tMA0GCSqGSIb3DQEBDAUAA4ICAQAI
Lv4hSPNBcCiZPejKD1/xCJsZjLEHiQDEMCABEQ3wzmdKlCGxeKBhyi46URu0BKLY
SoDHT/qXa1QOxt8lUYn1LkGwiN2jWgUn8UK3VloGxmHgKQ9OuAH893TyQNktvZLr
35Ve9qREIBlBNtfNlwqBsm2Z3Vyzd1r+iZJioqG63dSx29Tm9YUcRPzkFXEsrWvp
3uTpSWWnSEo7FPEKk66RuCy1qDViOuAB5DOAWdM/dB5OL52UxisLf91EvZfZHOu2
G5KVA45DvXi7Jmgq/9zL9hM74ziNAudWjruOE/cPfPr2HbW5MTXcmcMctSXy93Df
RqIXoMVHj5ZrZRpxO6vvxNm6wxyizkqOg6WndgTFa13o0Tmva6fVzqVsdm06lNT1
dQA8tu7c+fdWw98ZIUSNeR9S0b/Rzjl0nr0mITVliDwJJhfsPjijLaCLCXBQqA1h
nYTWaGt1HYWEVK0L0SpZRqPcBa15QMHX3Y6wd3nNcRrU2OkgMHox0pKnuhbsVblM
wf5onD0m/CIPJ2pIEKn60Pv8oAXVk19hwHA8IhzSZi8u9atuNJC5vjDj2Uo9bUW1
UJj7MuKhTihTxYlaib8dh1Oq6k9S6AISeQJFgz/eEYPqyTm+irLaFMM/S6eg9M84
J/LdTC0VaJ+lSR+7k+6h3usVo1OXybt5lV9hC+Ymqg==
-----END CERTIFICATE-----
"""


def tls_context() -> ssl.SSLContext:
    """The normal trust store plus APR's missing intermediate. Verification on."""
    try:
        import certifi

        context = ssl.create_default_context(cafile=certifi.where())
    except ImportError:  # pragma: no cover - certifi ships with httpx
        context = ssl.create_default_context()
    context.load_verify_locations(cadata=APR_INTERMEDIATE_PEM)
    return context


# ---------------------------------------------------------------------------
# Statuses
# ---------------------------------------------------------------------------
#: Every status APR publishes in the company feed → (liveness class, English).
#: The feed's own description lists exactly these four. An unknown status
#: fails the build, rather than defaulting three layers away.
STATUSES: dict[str, tuple[str, str]] = {
    "Активан": ("live", "Active"),
    "У ликвидацији": ("pending", "In liquidation"),
    "У стечају": ("pending", "In bankruptcy"),
    "У принудној ликвидацији": ("pending", "In forced liquidation"),
}


# ---------------------------------------------------------------------------
# Identifier grammar
# ---------------------------------------------------------------------------


def normalise_mb(raw: str) -> str:
    """Return a Serbian company registration number (matični broj) as 8 digits.

    Whitespace is removed. No check digit is verified: measured on the
    31 August 2026 cut, neither of the published check-digit formulas holds
    for most of the register's 133,932 numbers (older numbers predate it), so
    a check would reject real companies. Every RA000517 ``registeredAs`` on
    GLEIF is already 8 digits, so nothing is zero-padded either.
    """
    text = re.sub(r"\s+", "", str(raw or ""))
    if not re.fullmatch(r"\d{8}", text):
        raise ValueError(f"not a Serbian company registration number: {raw!r}")
    return text


# ---------------------------------------------------------------------------
# Names
# ---------------------------------------------------------------------------
#: Serbian Cyrillic → Serbian Latin (Gajica). One-to-one per letter; the
#: digraphs are single Cyrillic letters. Capitals of a digraph are written
#: title-case (Љ → Lj) and fixed up to all-caps inside an all-caps word.
_SR_CYR_TO_LAT: dict[str, str] = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "ђ": "đ", "е": "e",
    "ж": "ž", "з": "z", "и": "i", "ј": "j", "к": "k", "л": "l", "љ": "lj",
    "м": "m", "н": "n", "њ": "nj", "о": "o", "п": "p", "р": "r", "с": "s",
    "т": "t", "ћ": "ć", "у": "u", "ф": "f", "х": "h", "ц": "c", "ч": "č",
    "џ": "dž", "ш": "š",
}


def has_cyrillic(text: str | None) -> bool:
    return any("Ѐ" <= ch <= "ӿ" for ch in str(text or ""))


def to_latin(text: str | None) -> str:
    """A Serbian Cyrillic string in Serbian Latin; other characters unchanged."""
    source = str(text or "")
    out: list[str] = []
    for i, ch in enumerate(source):
        low = ch.lower()
        latin = _SR_CYR_TO_LAT.get(low)
        if latin is None:
            out.append(ch)
            continue
        if ch == low:
            out.append(latin)
            continue
        # A capital. A digraph is all-caps when a neighbour is a capital too.
        neighbours = source[i - 1 : i] + source[i + 1 : i + 2]
        if len(latin) > 1 and any(n.isalpha() and n.isupper() for n in neighbours):
            out.append(latin.upper())
        else:
            out.append(latin[0].upper() + latin[1:])
    return "".join(out)


def fold(text: str | None) -> str:
    """Uppercase ASCII: Latin script, diacritics removed, punctuation spaced."""
    latin = to_latin(text).replace("đ", "dj").replace("Đ", "DJ")
    decomposed = unicodedata.normalize("NFKD", latin)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(re.sub(r"[^0-9A-Za-z]+", " ", stripped).upper().split())


#: Legal-form words removed from the search key. Order-preserving.
_FORM_WORDS = re.compile(
    r"\b(?:PRIVREDNO )?DRUSTVO SA OGRANICENOM ODGOVORNOSCU\b"
    r"|\bAKCIONARSKO DRUSTVO\b|\bJAVNO PREDUZECE\b|\bPREDUZECE\b"
    r"|\bD ?O ?O\b|\bA ?D\b"
)


def name_key(name: str) -> str:
    """The key a name search matches against: folded, form words removed."""
    return " ".join(_FORM_WORDS.sub(" ", fold(name)).split())


# ---------------------------------------------------------------------------
# Reading the register as a stream
# ---------------------------------------------------------------------------


class IndexBuildError(RuntimeError):
    """The register could not be read as published; the old index is kept."""


class _Reader:
    """A rolling text buffer over a UTF-8 byte stream."""

    def __init__(self, fh: IO[bytes], chunk: int = 1 << 16) -> None:
        self._fh = fh
        self._chunk = chunk
        self._decoder = codecs.getincrementaldecoder("utf-8-sig")()
        self.buf = ""
        self.pos = 0
        self.eof = False

    def fill(self) -> bool:
        if self.eof:
            return False
        data = self._fh.read(self._chunk)
        if not data:
            self.buf = self.buf[self.pos :] + self._decoder.decode(b"", final=True)
            self.pos = 0
            self.eof = True
            return False
        self.buf = self.buf[self.pos :] + self._decoder.decode(data)
        self.pos = 0
        return True

    def skip_ws(self) -> str:
        """Advance past whitespace; return the next character ('' at end)."""
        while True:
            while self.pos < len(self.buf) and self.buf[self.pos] in " \t\r\n":
                self.pos += 1
            if self.pos < len(self.buf):
                return self.buf[self.pos]
            if not self.fill():
                return ""

    def expect(self, char: str) -> None:
        if self.skip_ws() != char:
            found = self.buf[self.pos : self.pos + 20]
            raise IndexBuildError(f"expected {char!r} in the register JSON, found {found!r}")
        self.pos += 1

    def value(self, decoder: json.JSONDecoder = json.JSONDecoder()) -> Any:
        """Decode one JSON value, reading more until it is complete."""
        self.skip_ws()
        while True:
            try:
                value, end = decoder.raw_decode(self.buf, self.pos)
            except json.JSONDecodeError as exc:
                if self.eof or len(self.buf) - self.pos > _MAX_RECORD_CHARS:
                    raise IndexBuildError(f"malformed register JSON: {exc}") from exc
                self.fill()
                continue
            # A number at the very end of the buffer may be cut short.
            if end >= len(self.buf) and not self.eof and not isinstance(value, (dict, list, str)):
                self.fill()
                continue
            self.pos = end
            return value


def iter_register(fh: IO[bytes], *, chunk: int = 1 << 16) -> Iterator[tuple[str, Any]]:
    """Yield the register's top level as it is read.

    Yields ``("DatumPreseka", "2026-08-31")`` for a scalar top-level key and
    ``("Podaci", (matični_broj, record))`` for each company, in file order,
    without holding more than one record in memory. The ``DatumPreseka``
    comes first in every file seen, which is what lets :func:`peek_cut_date`
    stop after a few bytes.
    """
    reader = _Reader(fh, chunk)
    reader.expect("{")
    while True:
        nxt = reader.skip_ws()
        if nxt == "}":
            return
        if nxt == ",":
            reader.pos += 1
            continue
        key = reader.value()
        if not isinstance(key, str):
            raise IndexBuildError("register JSON has a non-string key")
        reader.expect(":")
        if key != "Podaci":
            yield key, reader.value()
            continue
        reader.expect("{")
        while True:
            inner = reader.skip_ws()
            if inner == "}":
                reader.pos += 1
                break
            if inner == ",":
                reader.pos += 1
                continue
            mb = reader.value()
            reader.expect(":")
            yield "Podaci", (mb, reader.value())


# ---------------------------------------------------------------------------
# Building the index
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE company (
    mb TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_latin TEXT,
    name_key TEXT,
    legal_form TEXT,
    status TEXT,
    founded_on TEXT,
    municipality TEXT,
    municipality_code TEXT,
    activity_code TEXT
);
"""

_FIELDS = ("PoslovnoIme", "NazivStatus", "NazivPravneForme")


def _clean(value: Any) -> str | None:
    text = " ".join(str(value).split()) if value is not None else ""
    return text or None


def _iso_date(value: Any) -> str | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def build_index(source: Path, out: Path, *, source_url: str = COMPANIES_URL) -> dict[str, str]:
    """Index a downloaded company-register file into SQLite at ``out``.

    Written to a temporary file next to ``out`` and swapped in with
    ``os.replace`` only when the whole file has been read, so a failed or
    changed export never replaces a working index.
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".building")
    if tmp.exists():
        tmp.unlink()
    conn = sqlite3.connect(tmp)
    cut_date: str | None = None
    counts = {"companies": 0, "skipped_bad_number": 0}
    by_status: dict[str, int] = {}
    try:
        conn.executescript(_SCHEMA)
        batch: list[tuple[Any, ...]] = []
        with open(source, "rb") as fh:
            for key, value in iter_register(fh):
                if key == "DatumPreseka":
                    cut_date = _iso_date(value)
                    continue
                if key != "Podaci":
                    continue
                mb_raw, record = value
                try:
                    mb = normalise_mb(mb_raw)
                except ValueError:
                    counts["skipped_bad_number"] += 1
                    continue
                if not isinstance(record, dict) or any(f not in record for f in _FIELDS):
                    raise IndexBuildError(
                        f"a register record lacks {', '.join(_FIELDS)} — the feed changed shape"
                    )
                name = _clean(record.get("PoslovnoIme"))
                status = _clean(record.get("NazivStatus"))
                if status not in STATUSES:
                    raise IndexBuildError(f"unknown company status {status!r} — add it to STATUSES")
                if not name:
                    counts["skipped_bad_number"] += 1
                    continue
                by_status[status] = by_status.get(status, 0) + 1
                batch.append(
                    (
                        mb,
                        name,
                        to_latin(name) if has_cyrillic(name) else None,
                        name_key(name),
                        _clean(record.get("NazivPravneForme")),
                        status,
                        _iso_date(record.get("DatumOsnivanja")),
                        _clean(record.get("NazivOpstine")),
                        _clean(record.get("SifraOpstine")),
                        _clean(record.get("SifraDelatnosti")),
                    )
                )
                counts["companies"] += 1
                if len(batch) >= 5000:
                    conn.executemany("INSERT OR REPLACE INTO company VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
                    batch.clear()
        if batch:
            conn.executemany("INSERT OR REPLACE INTO company VALUES (?,?,?,?,?,?,?,?,?,?)", batch)
        if cut_date is None:
            raise IndexBuildError("the register file carries no DatumPreseka cut date")
        if counts["companies"] == 0:
            raise IndexBuildError("the register file holds no companies")
        conn.execute("CREATE INDEX idx_company_name_key ON company(name_key)")
        meta = {
            "schema_version": INDEX_SCHEMA_VERSION,
            "snapshot_date": cut_date,
            "source_url": source_url,
            "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "companies": str(counts["companies"]),
            "skipped_bad_number": str(counts["skipped_bad_number"]),
            **{f"status:{k}": str(v) for k, v in sorted(by_status.items())},
        }
        conn.executemany("INSERT INTO meta VALUES (?, ?)", meta.items())
        conn.commit()
    except BaseException:
        conn.close()
        tmp.unlink(missing_ok=True)
        raise
    conn.close()
    os.replace(tmp, out)
    logger.info("apr_serbia: indexed %s companies, cut %s", counts["companies"], cut_date)
    return meta


# ---------------------------------------------------------------------------
# Where the index lives, and keeping it current
# ---------------------------------------------------------------------------


def index_path() -> Path:
    configured = getattr(get_settings(), "apr_serbia_db_file", None)
    if configured:
        return Path(configured)
    return Path(tempfile.gettempdir()) / "opencheck" / "apr_serbia.sqlite"


def sync_enabled() -> bool:
    settings = get_settings()
    return bool(settings.allow_live and getattr(settings, "apr_serbia_sync", True))


_SHARED: dict[str, sqlite3.Connection | None] = {}
_STATE: dict[str, Any] = {"last_check": None, "last_outcome": None, "thread": None}
_BUILD_LOCK = threading.Lock()


def read_meta(path: Path | None = None) -> dict[str, str]:
    target = path or index_path()
    if not target.exists():
        return {}
    try:
        conn = sqlite3.connect(f"file:{target}?mode=ro", uri=True)
        try:
            return dict(conn.execute("SELECT key, value FROM meta").fetchall())
        finally:
            conn.close()
    except sqlite3.Error:
        return {}


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        path = index_path()
        if not path.exists():
            return None
        meta = read_meta(path)
        if meta.get("schema_version") != INDEX_SCHEMA_VERSION:
            logger.warning("apr_serbia: index at %s has schema %r; ignoring", path, meta.get("schema_version"))
            return None
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        _SHARED["conn"] = conn
    return _SHARED.get("conn")


def reset_connection() -> None:
    """Forget the cached connection.

    Deliberately not closed: a rebuild runs in a background thread while a
    lookup may be reading, and the old connection keeps reading the file it
    opened after ``os.replace`` swaps in the new one.
    """
    _SHARED.pop("conn", None)


def index_available() -> bool:
    return _shared_conn() is not None


def snapshot_age_days(meta: dict[str, str]) -> int | None:
    try:
        snapshot = date.fromisoformat(meta.get("snapshot_date") or "")
    except ValueError:
        return None
    return (date.today() - snapshot).days


def peek_cut_date(url: str = COMPANIES_URL) -> str:
    """The cut date of the register APR is serving now, from its first bytes.

    Streams the response and closes it as soon as ``DatumPreseka`` has been
    read, so a check costs a few kilobytes rather than 58 MB.
    """
    import httpx

    with httpx.stream("GET", url, timeout=60.0, follow_redirects=True, verify=tls_context()) as response:
        response.raise_for_status()

        class _Stream:
            def __init__(self) -> None:
                self._it = response.iter_bytes()

            def read(self, _n: int = -1) -> bytes:
                return next(self._it, b"")

        for key, value in iter_register(_Stream()):  # type: ignore[arg-type]
            if key == "DatumPreseka":
                cut = _iso_date(value)
                if cut:
                    return cut
            if key == "Podaci":
                break
    raise IndexBuildError("APR's response did not open with a DatumPreseka cut date")


def _download(url: str, target: Path) -> int:
    import httpx

    target.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    with httpx.stream("GET", url, timeout=300.0, follow_redirects=True, verify=tls_context()) as response:
        response.raise_for_status()
        with open(target, "wb") as fh:
            for chunk in response.iter_bytes():
                size += len(chunk)
                fh.write(chunk)
    return size


def sync_index(*, force: bool = False) -> dict[str, Any]:
    """Make sure the index holds APR's newest monthly cut. Blocking.

    Keeps the file when its cut is younger than ``REFRESH_AFTER_DAYS``, or
    when APR is still serving the same cut; otherwise downloads the register
    and rebuilds. Never raises: a month-old index beats none, and the outcome
    is returned and logged.
    """
    path = index_path()
    with _BUILD_LOCK:
        meta = read_meta(path)
        current = meta.get("schema_version") == INDEX_SCHEMA_VERSION
        age = snapshot_age_days(meta) if current else None
        if not force and current and age is not None and age <= REFRESH_AFTER_DAYS:
            return _outcome("kept", path, meta)
        if not sync_enabled():
            return _outcome("kept" if current else "not_configured", path, meta)
        now = time.monotonic()
        last = _STATE["last_check"]
        if not force and current and last is not None and now - last < _CHECK_INTERVAL_S:
            return _outcome("kept", path, meta)
        _STATE["last_check"] = now
        download = path.with_name(path.name + ".json")
        try:
            if not force and current:
                try:
                    served = peek_cut_date()
                except IndexBuildError:
                    served = None  # an unexpected opening: read the whole file
                if served is not None and served <= (meta.get("snapshot_date") or ""):
                    return _outcome("kept", path, meta)
            _download(COMPANIES_URL, download)
            new_meta = build_index(download, path)
            reset_connection()
            return _outcome("built", path, new_meta)
        except Exception as exc:  # noqa: BLE001 - reported, never raised
            logger.warning("apr_serbia: index sync failed: %s", exc)
            return _outcome("failed", path, meta, error=f"{type(exc).__name__}: {exc}"[:300])
        finally:
            if download.exists():
                try:
                    download.unlink()
                except OSError:  # pragma: no cover - best effort
                    pass


def _outcome(kind: str, path: Path, meta: dict[str, str], *, error: str | None = None) -> dict[str, Any]:
    result = {
        "apr_serbia": kind,
        "path": str(path),
        "snapshot_date": meta.get("snapshot_date") or None,
        "companies": meta.get("companies") or None,
    }
    if error:
        result["error"] = error
    _STATE["last_outcome"] = result
    return result


def warm_index() -> dict[str, Any]:
    """Startup hook: build or refresh the index. Synchronous; run in a thread."""
    if not sync_enabled() and not index_path().exists():
        return _outcome("not_configured", index_path(), {})
    return sync_index()


def _start_background_sync() -> threading.Thread | None:
    thread = _STATE.get("thread")
    if thread is not None and thread.is_alive():
        return thread
    if not sync_enabled():
        return None
    thread = threading.Thread(target=sync_index, name="apr_serbia-sync", daemon=True)
    _STATE["thread"] = thread
    thread.start()
    return thread


def state() -> dict[str, Any]:
    """What the last sync did, for diagnostics."""
    return dict(_STATE.get("last_outcome") or {})


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class AprSerbiaAdapter(SourceAdapter):
    """Source adapter for Serbia's register of companies (APR)."""

    id = "apr_serbia"

    lookup_derivers = (LookupDeriver(RS_RA_CODES, "rs_mb", normalise_mb),)
    lookup_pass_legal_name = True
    lookup_timeout_s = _LOOKUP_TIMEOUT_S

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="APR — Business Registers Agency company register (Serbia)",
            homepage=_HOMEPAGE,
            description=(
                "Serbia's register of companies (Регистар привредних друштава), "
                "published monthly by the Agencija za privredne registre "
                "(Business Registers Agency) through its open-data API. "
                "Business name, registration number (matični broj), legal form, "
                "status — active, in liquidation, in bankruptcy or in forced "
                "liquidation — founding date, municipality and activity code. "
                "No addresses and no people: no directors, shareholders or "
                "beneficial owners. Deleted companies and entrepreneurs are "
                "not published."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains data from the Register of Business Entities published "
                "by the Agencija za privredne registre (Serbian Business "
                "Registers Agency) at https://openapi.apr.gov.rs/api/opendata/companies, "
                "under the Serbian Open Data Portal licence (SODL 1.0). "
                "Downloaded on the cut date shown on each record; OpenCheck "
                "adds a Serbian Latin transliteration of Cyrillic names and "
                "maps the register's status to a liveness class."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=bool(settings.allow_live or index_available()),
            is_national_register=True,
            country="RS",
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """Name search over the index. Empty when no index is present."""
        if kind != SearchKind.ENTITY:
            return []
        conn = _shared_conn()
        key = name_key(query)
        if conn is None or len(key) < 3:
            return []
        cur = conn.execute(
            "SELECT mb, name, legal_form FROM company WHERE name_key LIKE ? LIMIT 10",
            (f"%{key}%",),
        )
        return [
            SourceHit(
                source_id=self.id,
                hit_id=row["mb"],
                kind=SearchKind.ENTITY,
                name=row["name"],
                summary=f"{RS_APR_SCHEME} {row['mb']}",
                identifiers={"rs_mb": row["mb"]},
                raw={"mb": row["mb"], "legal_form": row["legal_form"]},
                is_stub=False,
                liveness="snapshot",
            )
            for row in cur.fetchall()
        ]

    def _bundle(self, mb: str, legal_name: str, **extra: Any) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "mb": mb,
            "name": legal_name or "",
            "company": None,
            "snapshot_date": None,
            "legal_name": legal_name,
            "link": COMPANIES_URL,
            "not_found": False,
            "coverage_note": None,
            "is_stub": True,
        }
        bundle.update(extra)
        return bundle

    async def _wait_for_index(self) -> sqlite3.Connection | None:
        thread = _start_background_sync()
        if thread is None:
            return None
        deadline = time.monotonic() + _BUILD_WAIT_S
        while thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(0.5)
        return _shared_conn()

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the register's record for one matični broj."""
        try:
            mb = normalise_mb(hit_id)
        except ValueError:
            return self._bundle(str(hit_id or ""), legal_name)

        conn = _shared_conn()
        if conn is None:
            conn = await self._wait_for_index()
        if conn is None:
            if sync_enabled():
                outcome = state()
                degradation.record(
                    self.id,
                    (
                        "The APR company register index was not available: "
                        + (
                            "the register could not be downloaded or built"
                            if outcome.get("apr_serbia") == "failed"
                            else "the register is still being downloaded and indexed"
                        )
                    ),
                    reason=degradation.REASON_UPSTREAM_ERROR,
                )
            return self._bundle(mb, legal_name)

        meta = read_meta()
        age = snapshot_age_days(meta)
        if age is None or age > REFRESH_AFTER_DAYS:
            _start_background_sync()
        snapshot = meta.get("snapshot_date") or None
        built = None
        if snapshot:
            built = datetime.fromisoformat(snapshot).replace(tzinfo=timezone.utc)
        provenance.record_snapshot(
            built,
            f"APR open-data company register, cut {snapshot}" if snapshot else "APR open-data company register",
        )

        row = conn.execute("SELECT * FROM company WHERE mb = ?", (mb,)).fetchone()
        if row is None:
            return self._bundle(
                mb,
                legal_name,
                snapshot_date=snapshot,
                not_found=True,
                coverage_note=COVERAGE_NOT_IN_REGISTER,
                is_stub=False,
            )
        company = dict(row)
        company.pop("name_key", None)
        bundle = self._bundle(
            mb,
            legal_name,
            name=company.get("name") or legal_name,
            company=company,
            snapshot_date=snapshot,
            is_stub=False,
        )
        validate_raw(self.id, AprSerbiaBundle, bundle)
        return bundle
