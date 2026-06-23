#!/usr/bin/env python3
"""Throwaway probe: does the Estonian e-Äriregister SOAP service return HISTORY?

Purpose
-------
The Time Machine needs *dated historic* records. The documented
``arireg.detailandmed_v2`` (Detailed company data query) should return the full
registry-card history by default (``ainult_kehtivad=0``), with every name /
address / legal-form / status / person record carrying ``algus_kpv`` (start) and
``lopp_kpv`` (end), plus a ``registrikaardid → kanded`` typed entry log.

BUT this is the X-Road SOAP service at ariregxmlv6.rik.ee that OpenCheck currently
bans, because the Phase-37 contract authenticated (HTTP 200) yet returned ZERO
rows for every query ("the contract type did not grant data-query access").

This script settles that empirically with the credentials obtained 2026-05-29
(``ARIREGISTER_USERNAME`` / ``ARIREGISTER_PASSWORD`` in ``.env``). It fires one
``detailandmed_v2`` request and tells you which of three worlds you're in:

  1. AUTH FAILED            — SOAP Fault / bad credentials.
  2. AUTH OK, NO DATA       — the Phase-37 symptom: 0 companies returned.
  3. HISTORY AVAILABLE 🎉   — companies returned AND closed (lopp_kpv) rows /
                              multiple registry-card entries are present.

Nothing here is wired into the app — it's a standalone, stdlib-only diagnostic.
Run it locally where the .env credentials live; do NOT commit any output.

Usage
-----
  cd backend
  python3 scripts/spike_ariregister_history.py                  # RIK itself (70000310)
  python3 scripts/spike_ariregister_history.py 12417834         # Bolt Technology OÜ
  python3 scripts/spike_ariregister_history.py 12417834 --raw out.xml   # dump full body
  python3 scripts/spike_ariregister_history.py --test           # demo host
  python3 scripts/spike_ariregister_history.py --format xml      # ask for XML body

Credentials are read from the environment, falling back to ../.env (repo root).
The password is never printed.
"""

from __future__ import annotations

import argparse
import re
import sys
import urllib.request
from pathlib import Path

PROD_URL = "https://ariregxmlv6.rik.ee/"
TEST_URL = "https://demo-ariregxmlv6.rik.ee/"
NS = "http://arireg.x-road.eu/producer/"


def _load_env_creds() -> tuple[str | None, str | None]:
    """Return (username, password) from the environment or ../.env."""
    import os

    user = os.environ.get("ARIREGISTER_USERNAME")
    pw = os.environ.get("ARIREGISTER_PASSWORD")
    if user and pw:
        return user, pw

    # Fall back to the repo-root .env (scripts/ -> backend/ -> repo root).
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key == "ARIREGISTER_USERNAME" and not user:
                user = val
            elif key == "ARIREGISTER_PASSWORD" and not pw:
                pw = val
    return user, pw


def _escape(text: str) -> str:
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )


def _build_envelope(user: str, pw: str, reg_code: str, fmt: str) -> str:
    """Build the detailandmed_v2 SOAP request.

    yandmed=1 (general data) + iandmed=1 (personnel data) so we get names,
    addresses, legal forms, statuses AND the persons-on-card history.
    ainult_kehtivad=0 → include historic (ended) rows, the whole point here.
    """
    return f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:prod="{NS}">
 <soapenv:Header/>
 <soapenv:Body>
  <prod:detailandmed_v2>
   <prod:keha>
    <prod:ariregister_kasutajanimi>{_escape(user)}</prod:ariregister_kasutajanimi>
    <prod:ariregister_parool>{_escape(pw)}</prod:ariregister_parool>
    <prod:ariregistri_kood>{_escape(reg_code)}</prod:ariregistri_kood>
    <prod:ariregister_valjundi_formaat>{fmt}</prod:ariregister_valjundi_formaat>
    <prod:yandmed>1</prod:yandmed>
    <prod:iandmed>1</prod:iandmed>
    <prod:kandmed>0</prod:kandmed>
    <prod:dandmed>0</prod:dandmed>
    <prod:maarused>0</prod:maarused>
    <prod:ainult_kehtivad>0</prod:ainult_kehtivad>
    <prod:keel>eng</prod:keel>
   </prod:keha>
  </prod:detailandmed_v2>
 </soapenv:Body>
</soapenv:Envelope>"""


def _post(url: str, body: str, timeout: float) -> tuple[int, str]:
    req = urllib.request.Request(
        url,
        data=body.encode("utf-8"),
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '""',
            "User-Agent": "OpenCheck-spike/1.0 (history probe)",
            "Accept": "text/xml, application/json, */*",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        # SOAP faults come back as HTTP 500 with the fault in the body.
        return exc.code, exc.read().decode("utf-8", errors="replace")


def _first_int(pattern: str, text: str) -> int | None:
    m = re.search(pattern, text)
    return int(m.group(1)) if m else None


def _count_field(name: str, text: str) -> int:
    """Count value-bearing occurrences of a field, for XML *or* JSON bodies.

    Matches an XML opening tag (``<ns1:lopp_kpv>``) or a JSON key
    (``"lopp_kpv"``) — never the XML closing tag — so counts aren't doubled.
    """
    return len(re.findall(rf'<[^/>]*{name}>|"{name}"', text))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("reg_code", nargs="?", default="70000310",
                    help="Estonian registry code (default 70000310 = RIK).")
    ap.add_argument("--test", action="store_true", help="Use the demo host.")
    ap.add_argument("--format", choices=["json", "xml"], default="json",
                    help="Output body format to request (default json).")
    ap.add_argument("--raw", metavar="FILE", help="Write the full response body here.")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    user, pw = _load_env_creds()
    if not user or not pw:
        print("✖ No credentials. Set ARIREGISTER_USERNAME / ARIREGISTER_PASSWORD "
              "in the environment or ../.env.", file=sys.stderr)
        return 2

    url = TEST_URL if args.test else PROD_URL
    print(f"Endpoint:      {url}")
    print(f"Query:         arireg.detailandmed_v2 (ainult_kehtivad=0 → history)")
    print(f"Registry code: {args.reg_code}")
    print(f"Username:      {user[:2]}***  (password hidden)")
    print(f"Output format: {args.format}\n")

    status, raw = _post(url, _build_envelope(user, pw, args.reg_code, args.format), args.timeout)

    if args.raw:
        Path(args.raw).write_text(raw, encoding="utf-8")
        print(f"Full response written to {args.raw}\n")

    print(f"HTTP status:   {status}")
    print(f"Body length:   {len(raw):,} chars")

    # --- Fault / auth detection ------------------------------------------------
    fault = re.search(r"<faultstring>(.*?)</faultstring>", raw, re.DOTALL | re.IGNORECASE)
    if fault or "soap:Fault" in raw or "SOAP-ENV:Fault" in raw:
        msg = (fault.group(1).strip() if fault else "(fault element present)")
        print("\n▶ Verdict: AUTH FAILED / FAULT")
        print(f"  Fault: {msg[:300]}")
        print("  → Credentials rejected or the operation isn't permitted for this contract.")
        return 1

    # --- Did we get any company back? -----------------------------------------
    found = _first_int(r'leitud_ettevotjate_arv["\s:>]*?(\d+)', raw)

    # --- History markers (work for both JSON and XML bodies) ------------------
    n_start = _count_field("algus_kpv", raw)
    n_end = _count_field("lopp_kpv", raw)
    n_entries = _count_field("kande_kpv", raw)             # registry-card entries

    print(f"\nCompanies found (leitud_ettevotjate_arv): "
          f"{found if found is not None else 'unknown'}")
    print(f"Dated rows — algus_kpv (starts): {n_start}, lopp_kpv (ends): {n_end}")
    print(f"Registry-card entries (kande_kpv): {n_entries}")

    # Sample a few end-dated (historic) rows so you can eyeball them.
    sample = re.findall(r".{0,60}lopp_kpv.{0,40}", raw)[:5]
    if sample:
        print("\nSample of closed/historic rows:")
        for s in sample:
            print("  …", re.sub(r"\s+", " ", s).strip())

    # --- Verdict --------------------------------------------------------------
    print()
    if found in (0, None) and n_start == 0:
        print("▶ Verdict: AUTH OK, NO DATA  (the Phase-37 symptom)")
        print("  Authenticated but the contract returned no company data.")
        print("  → This contract tier still lacks data-query access; ask RIK to")
        print("    enable detailandmed_v2 for the open-data API contract.")
        return 1

    if n_end > 0 or (n_entries and n_entries > 1):
        print("▶ Verdict: HISTORY AVAILABLE 🎉")
        print("  Closed (lopp_kpv) rows and/or a multi-entry registry-card log are")
        print("  present — i.e. full dated history is returned. The Time Machine can")
        print("  source Estonian change events from detailandmed_v2.")
        return 0

    print("▶ Verdict: DATA RETURNED, BUT NO HISTORY MARKERS")
    print("  Got company data but saw no lopp_kpv / multi-entry log. Try an older")
    print("  company (e.g. 70000310) — a young entity may simply have no closed rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
