"""Capture the UK Companies House PSC stream to a JSON-Lines file.

Step 2 of the "Create a UK PSC > BODS livestream demo" capture-and-validate
test. Opens the public PSC streaming endpoint, reads newline-delimited JSON
events for a bounded window, writes each raw event verbatim to a ``.jsonl``
file, and prints a running rate + mix tally so we can size the eventual UX
(readable trickle vs firehose) before building anything.

The companion ``map_psc_stream.py`` then feeds the captured events through
OpenCheck's ``map_companies_house`` mapper and validates the BODS v0.4 output.

Endpoint (see CH streaming docs)::

    GET https://stream.companieshouse.gov.uk/persons-with-significant-control

Auth: HTTP Basic with the **streaming** API key as the username and an empty
password (this is a different credential from the REST key). Each event is one
line of JSON; blank lines are heartbeats. The connection is long-lived and CH
will periodically drop it — we reconnect, resuming from the last ``timepoint``
so no events are missed.

Usage::

    export COMPANIES_HOUSE_STREAM_KEY=your_streaming_key
    python scripts/capture_psc_stream.py --minutes 60 \\
        --out data/cache/psc_stream/psc_stream_sample.jsonl

    # or cap by event count instead of (or as well as) time:
    python scripts/capture_psc_stream.py --max-events 500

Stop early with Ctrl-C — a summary is printed on exit either way.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STREAM_URL = "https://stream.companieshouse.gov.uk/persons-with-significant-control"


def _classify(event: dict[str, Any], mix: Counter) -> None:
    """Tally an event by lifecycle type and PSC kind for the running summary."""
    ev_type = (event.get("event") or {}).get("type") or "?"
    mix[f"event:{ev_type}"] += 1
    data = event.get("data") or {}
    kind = data.get("kind") or ("<no-data>" if not data else "<unknown-kind>")
    mix[f"kind:{kind}"] += 1
    if data.get("ceased_on") or data.get("ceased"):
        mix["lifecycle:ceased"] += 1


def _print_summary(count: int, started: float, mix: Counter, out_path: Path) -> None:
    elapsed = max(time.monotonic() - started, 1e-9)
    rate_min = count / (elapsed / 60.0)
    print("\n--- capture summary ---")
    print(f"  events captured : {count:,}")
    print(f"  elapsed         : {elapsed/60.0:.1f} min")
    print(f"  rate            : {rate_min:.1f} events/min  ({count/elapsed:.2f}/s)")
    print(f"  output          : {out_path}")
    print("  mix:")
    for label, n in sorted(mix.items()):
        print(f"    {label:<48} {n:>7,}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--minutes", type=float, default=60.0, help="capture window (default 60)")
    ap.add_argument("--max-events", type=int, default=None, help="stop after N events")
    ap.add_argument(
        "--out",
        default="data/cache/psc_stream/psc_stream_sample.jsonl",
        help="output JSON-Lines path",
    )
    ap.add_argument(
        "--timepoint",
        type=int,
        default=None,
        help="resume the stream from a past timepoint (default: live from now)",
    )
    ap.add_argument(
        "--key",
        default=os.environ.get("COMPANIES_HOUSE_STREAM_KEY"),
        help="streaming API key (default: $COMPANIES_HOUSE_STREAM_KEY)",
    )
    ap.add_argument(
        "--progress-every", type=int, default=25, help="print a tally every N events"
    )
    args = ap.parse_args(argv)

    if not args.key:
        print(
            "ERROR: no streaming key. Set COMPANIES_HOUSE_STREAM_KEY or pass --key.\n"
            "Note: this is the *streaming* key, not your REST key.",
            file=sys.stderr,
        )
        return 2

    try:
        import httpx
    except ImportError:
        print("ERROR: pip install httpx", file=sys.stderr)
        return 1

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    deadline = time.monotonic() + args.minutes * 60.0
    started = time.monotonic()
    count = 0
    mix: Counter = Counter()
    last_timepoint: int | None = args.timepoint

    # Flush + summarise on Ctrl-C.
    stopping = {"flag": False}

    def _on_sigint(signum, frame):  # noqa: ANN001, ARG001
        stopping["flag"] = True

    signal.signal(signal.SIGINT, _on_sigint)

    # read=None: the stream is long-lived, so never time out mid-connection.
    timeout = httpx.Timeout(connect=15.0, read=None, write=15.0, pool=15.0)
    backoff = 2.0

    print(f"Connecting to PSC stream -> {out_path}")
    print(f"Window: {args.minutes:.0f} min" + (f", max {args.max_events} events" if args.max_events else ""))

    with out_path.open("a", encoding="utf-8") as sink:
        with httpx.Client(timeout=timeout) as client:
            while not stopping["flag"] and time.monotonic() < deadline:
                if args.max_events and count >= args.max_events:
                    break
                params = {"timepoint": last_timepoint} if last_timepoint is not None else {}
                try:
                    with client.stream(
                        "GET", STREAM_URL, params=params, auth=(args.key, "")
                    ) as resp:
                        if resp.status_code == 401:
                            print("ERROR: 401 Unauthorized — check the streaming key.", file=sys.stderr)
                            return 1
                        if resp.status_code == 416:
                            print("  timepoint too old (416) — restarting live.", file=sys.stderr)
                            last_timepoint = None
                            continue
                        if resp.status_code == 429:
                            retry = float(resp.headers.get("retry-after", backoff))
                            print(f"  rate-limited (429) — sleeping {retry:.0f}s", file=sys.stderr)
                            time.sleep(retry)
                            backoff = min(backoff * 2, 60.0)
                            continue
                        resp.raise_for_status()
                        backoff = 2.0  # healthy connection — reset backoff

                        for line in resp.iter_lines():
                            if stopping["flag"] or time.monotonic() >= deadline:
                                break
                            if args.max_events and count >= args.max_events:
                                break
                            if not line or not line.strip():
                                continue  # heartbeat
                            try:
                                event = json.loads(line)
                            except json.JSONDecodeError:
                                print("  ! skipped a non-JSON line", file=sys.stderr)
                                continue

                            sink.write(json.dumps(event, ensure_ascii=False) + "\n")
                            sink.flush()
                            count += 1
                            _classify(event, mix)
                            tp = (event.get("event") or {}).get("timepoint")
                            if isinstance(tp, int):
                                last_timepoint = tp

                            if count % args.progress_every == 0:
                                elapsed = time.monotonic() - started
                                rate = count / (elapsed / 60.0) if elapsed else 0.0
                                ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                                print(f"  [{ts}] {count:,} events  ({rate:.1f}/min)")
                except httpx.HTTPError as exc:
                    if stopping["flag"]:
                        break
                    print(f"  connection dropped ({type(exc).__name__}) — reconnecting in {backoff:.0f}s", file=sys.stderr)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)

    _print_summary(count, started, mix, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
