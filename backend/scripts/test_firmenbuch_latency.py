#!/usr/bin/env python3
"""Quick latency probe for the Austrian Firmenbuch SOAP API.

Uses only the Python standard library — no virtualenv needed.

Usage:
    FIRMENBUCH_API_KEY=<your-key> python scripts/test_firmenbuch_latency.py

Fires three requests and reports wall-clock time for each:
  1. SUCHEFIRMAREQUEST  — name search for "OMV"
  2. AUSZUG_V2_REQUEST  — entity extract for FN 93293f (OMV AG)
  3. AUSZUG_V2_REQUEST  — entity extract for FN 229831m (smaller GmbH)
"""

from __future__ import annotations

import datetime
import os
import re
import sys
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET

API_KEY = os.getenv("FIRMENBUCH_API_KEY", "")
ENDPOINT = "https://justizonline.gv.at/jop/api/at.gv.justiz.fbw/ws"

_SOAP_NS = "http://www.w3.org/2003/05/soap-envelope"
_NS_SUCHE_FIRMA = "ns://firmenbuch.justiz.gv.at/Abfrage/SucheFirmaRequest"
_NS_AUSZUG = "ns://firmenbuch.justiz.gv.at/Abfrage/v2/AuszugRequest"


# ---------------------------------------------------------------------------
# SOAP envelope builders — identical to firmenbuch.py adapter
# ---------------------------------------------------------------------------

def _soap_envelope(ns_prefix: str, ns_uri: str, body_xml: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<soap:Envelope xmlns:soap="{_SOAP_NS}" '
        f'xmlns:{ns_prefix}="{ns_uri}">'
        "<soap:Header/>"
        "<soap:Body>"
        f"{body_xml}"
        "</soap:Body>"
        "</soap:Envelope>"
    )


def _search_envelope(name: str) -> str:
    esc = name.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return _soap_envelope(
        "suc",
        _NS_SUCHE_FIRMA,
        f"<suc:SUCHEFIRMAREQUEST>"
        f"<suc:FIRMENWORTLAUT>{esc}</suc:FIRMENWORTLAUT>"
        f"<suc:EXAKTESUCHE>false</suc:EXAKTESUCHE>"
        f"<suc:SUCHBEREICH>1</suc:SUCHBEREICH>"
        f"<suc:GERICHT></suc:GERICHT>"
        f"<suc:RECHTSFORM></suc:RECHTSFORM>"
        f"<suc:RECHTSEIGENSCHAFT></suc:RECHTSEIGENSCHAFT>"
        f"<suc:ORTNR></suc:ORTNR>"
        f"</suc:SUCHEFIRMAREQUEST>",
    )


def _auszug_envelope(fn: str) -> str:
    esc = fn.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    today = datetime.date.today().isoformat()
    return _soap_envelope(
        "aus",
        _NS_AUSZUG,
        f"<aus:AUSZUG_V2_REQUEST>"
        f"<aus:FNR>{esc}</aus:FNR>"
        f"<aus:STICHTAG>{today}</aus:STICHTAG>"
        f"<aus:UMFANG>Kurzinformation</aus:UMFANG>"
        f"</aus:AUSZUG_V2_REQUEST>",
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

PROBES = [
    ("SUCHEFIRMAREQUEST — name search 'OMV'",  _search_envelope("OMV")),
    ("AUSZUG_V2_REQUEST — FN 093363z (OMV AG)", _auszug_envelope("093363z")),
    ("AUSZUG_V2_REQUEST — FN 229831m (GmbH)",  _auszug_envelope("229831m")),
]


def run() -> None:
    if not API_KEY:
        print("ERROR: FIRMENBUCH_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)

    print(f"Endpoint: {ENDPOINT}\n")

    for label, body in PROBES:
        data = body.encode("utf-8")
        req = urllib.request.Request(
            ENDPOINT,
            data=data,
            headers={
                "Content-Type": "application/soap+xml; charset=utf-8",
                "X-API-KEY": API_KEY,
            },
            method="POST",
        )
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            elapsed = time.perf_counter() - t0
            status = resp.status
            print(f"  {label}")
            print(f"    → HTTP {status}  {elapsed * 1000:.0f}ms")
            # Strip namespace prefixes so ElementTree can use plain local names
            # (identical to _strip_namespaces in firmenbuch.py)
            clean = re.sub(r'\s+xmlns(?::\w+)?="[^"]*"', "", raw)
            clean = re.sub(r"<(/?)\w+:(\w)", r"<\1\2", clean)
            clean = re.sub(r"\s\w+:(\w+=)", r" \1", clean)
            try:
                root = ET.fromstring(clean)
                if "SUCHEFIRMA" in label:
                    hits = root.findall(".//ERGEBNIS")
                    if hits:
                        for h in hits:
                            fn   = (h.findtext("FNR") or "").strip()
                            name = (h.findtext("NAME") or "").strip()
                            sitz = (h.findtext("SITZ") or "").strip()
                            rf_el = h.find("RECHTSFORM")
                            rf = (rf_el.findtext("TEXT") if rf_el is not None else "") or ""
                            print(f"    FN {fn:<14} {name}  ({rf}, {sitz})")
                    else:
                        print(f"    (no ERGEBNIS elements found in response)")
                else:
                    resp = root.find(".//AUSZUG_V2_RESPONSE") or root
                    fn_attr = resp.get("FNR", "")
                    name_parts = [b.text.strip() for b in resp.iter("BEZEICHNUNG") if b.text]
                    name = " ".join(name_parts)
                    print(f"    FN={fn_attr!r}  name={name!r}")
            except ET.ParseError as e:
                print(f"    (XML parse error: {e})")
        except urllib.error.HTTPError as exc:
            elapsed = time.perf_counter() - t0
            body_snippet = exc.read().decode("utf-8", errors="replace")[:400].replace("\n", " ")
            print(f"  {label}")
            print(f"    → HTTP {exc.code}  {elapsed * 1000:.0f}ms  ERROR")
            print(f"    {body_snippet}")
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            print(f"  {label}")
            print(f"    → ERROR after {elapsed * 1000:.0f}ms: {exc}")
        print()


if __name__ == "__main__":
    run()
