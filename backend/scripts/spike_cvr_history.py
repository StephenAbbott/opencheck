"""Spike: how much dated/bitemporal history does Denmark's CVR return?

Finding from the code review (no key needed): the CVR adapter's GraphQL queries
already select ``virkningFra`` / ``virkningTil`` on every node and apply NO
``virkningstid`` filter — so Datafordeler returns the *full* history, and the
adapter preserves it in the bundle's ``_raw_navn`` / ``_raw_adressering`` /
``_raw_branche`` / ``_raw_form`` / ``_raw_deltager`` fields. It only collapses to
the current state at the *summary* step (``_current()`` → ``virkningTil is null``).

So a Time Machine emitter needs **no new API calls** for name / address /
legal-form / industry / participant history — the dated blocks are already there.

This script measures, for one real entity, how many of those blocks carry a
*closed* period (``virkningTil`` set) — i.e. how many change events are
reconstructable — and separately probes the virksomhed/status period list (which
the adapter currently keeps only the current row of).

Run (from backend/):

    CVR_DENMARK_API_KEY=<key> OPENCHECK_ALLOW_LIVE=1 \
        uv run python -m scripts.spike_cvr_history 24256790   # Novo Nordisk
"""

from __future__ import annotations

import asyncio
import sys

from opencheck.config import get_settings
from opencheck.http import build_client
from opencheck.sources.cvr_denmark import (
    _GRAPHQL_URL,
    _Q_VIRKSOMHED,
    CvrDenmarkAdapter,
    normalise_cvr,
)

# (bundle key, human label → candidate ChangeType, field to print)
_BLOCKS = [
    ("_raw_navn", "name → LEGAL_NAME_CHANGE", "vaerdi"),
    ("_raw_form", "legal form → LEGAL_FORM_CHANGE", "vaerdiTekst"),
    ("_raw_adressering", "address → ADDRESS_CHANGE", "CVRAdresse_postdistrikt"),
    ("_raw_branche", "industry (branche)", "vaerdi"),
    ("_raw_deltager", "fully-liable participants", "deltagendeEnhedsId"),
]


def _d(v: str | None) -> str:
    return (str(v)[:10]) if v else "current"


async def main(cvr: str) -> None:
    settings = get_settings()
    if not settings.cvr_denmark_api_key:
        print("CVR_DENMARK_API_KEY is not set — cannot run the live spike.")
        return

    bundle = await CvrDenmarkAdapter().fetch(cvr)
    print(
        f"\nCVR {bundle['cvr_number']}  {bundle['name']}  "
        f"status={bundle['status']}  start={bundle['start_date']} end={bundle['end_date']}\n"
    )

    reconstructable = 0
    for key, label, field in _BLOCKS:
        nodes = bundle.get(key) or []
        closed = [n for n in nodes if n.get("virkningTil")]
        reconstructable += len(closed)
        print(f"  {label:34} {len(nodes):3} records, {len(closed):3} historical")
        for n in sorted(nodes, key=lambda x: x.get("virkningFra") or ""):
            print(f"        {_d(n.get('virkningFra'))} → {_d(n.get('virkningTil')):10}  {n.get(field)}")
        print()

    # Status / lifecycle history: re-query the virksomhed node list (the adapter
    # keeps only the current row, so STATUS_CHANGED would need this list preserved).
    async with build_client() as client:
        r = await client.post(
            _GRAPHQL_URL,
            params={"apiKey": settings.cvr_denmark_api_key},
            json={"query": _Q_VIRKSOMHED, "variables": {"cvr": int(normalise_cvr(cvr))}},
            timeout=45.0,
        )
        vnodes = (r.json().get("data") or {}).get("CVR_Virksomhed", {}).get("nodes", [])
    vclosed = [n for n in vnodes if n.get("virkningTil")]
    print(
        f"  virksomhed/status periods       {len(vnodes):3} records, {len(vclosed):3} historical"
        "  (STATUS_CHANGED source — NOT currently kept in the bundle)"
    )
    for n in sorted(vnodes, key=lambda x: x.get("virkningFra") or ""):
        print(f"        {_d(n.get('virkningFra'))} → {_d(n.get('virkningTil')):10}  status={n.get('status')}")

    print(
        f"\nReconstructable historical events from the existing bundle "
        f"(excl. status): {reconstructable}"
    )


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "24256790"))
