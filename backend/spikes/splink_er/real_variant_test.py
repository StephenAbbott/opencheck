"""real_variant_test.py — iteration 2: REAL name-variant positives.

Replaces the synthetic perturbation test (hard_positive_test.py) with
publisher-asserted aliases. For a sample of LEIs it re-fetches the bundle and
finds entity statements carrying ``alternateNames`` — i.e. the *same* entity
named two ways by a real source (e.g. "NNE A/S" / "NNE PHARMAPLAN A/S"). Each
(primary name, alternate name) is a true match by definition, with the other
features (jurisdiction / inc date / address) genuinely identical because it's
one entity. We score Splink (``compare_two_records``) vs ``difflib >= 0.88``.

Resumable: harvested pairs accumulate in ``corpus/variant_pairs.jsonl``; re-run
with ``--fetch N`` to add more before scoring.

Run from ``backend/``::

    uv run python spikes/splink_er/real_variant_test.py --fetch 3
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path

import httpx

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from build_corpus import (  # noqa: E402
    API_BASE,
    _address_norm,
    _identifiers,
    _jurisdiction,
    normalise_name,
)
from train_model import _strip_country, load_corpus  # noqa: E402

from splink import DuckDBAPI, Linker  # noqa: E402

CORPUS = HERE / "corpus"
PAIRS = CORPUS / "variant_pairs.jsonl"
VDONE = CORPUS / "_variant_done.txt"
DONE_LEIS = CORPUS / "_done_leis.txt"
MODEL = HERE / "model.json"
BASELINE = 0.88


def harvest(fetch_n: int) -> int:
    done = {ln.strip() for ln in VDONE.read_text().splitlines()} if VDONE.exists() else set()
    all_leis = [ln.strip() for ln in DONE_LEIS.read_text().splitlines() if ln.strip()]
    todo = [l for l in all_leis if l not in done][:fetch_n]
    added = 0
    with httpx.Client(timeout=60, follow_redirects=True) as client, PAIRS.open("a") as out:
        for lei in todo:
            try:
                bods = client.get(
                    f"{API_BASE}/export", params={"lei": lei, "format": "json", "deepen_top": 3}
                ).json()
            except Exception as e:  # noqa: BLE001
                print(f"  ! {lei}: {type(e).__name__}")
                done.add(lei)
                continue
            for s in bods if isinstance(bods, list) else []:
                if s.get("recordType") != "entity":
                    continue
                src = ((s.get("source") or {}).get("description") or "")
                if "opensanctions" in src.lower():
                    continue
                rd = s.get("recordDetails") or {}
                primary = (rd.get("name") or "").strip()
                alts = rd.get("alternateNames") or []
                if not primary or not isinstance(alts, list) or not alts:
                    continue
                jur = _jurisdiction(rd)
                base_lei, nat = _identifiers(rd, jur)
                base = {
                    "name_norm": normalise_name(primary),
                    "jurisdiction": jur or None,
                    "inc_date": (rd.get("foundingDate") or "").strip() or None,
                    "address_norm": _address_norm(rd) or None,
                    "lei": base_lei or None, "nat_reg": nat or None,
                }
                for alt in alts:
                    an = normalise_name(str(alt))
                    if an and an != base["name_norm"]:
                        out.write(json.dumps({"base": base, "alt_name": an}) + "\n")
                        added += 1
            done.add(lei)
    VDONE.write_text("\n".join(sorted(done)) + "\n")
    print(f"harvested {added} real variant pairs from {len(todo)} lookups")
    return added


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fetch", type=int, default=3, help="LEIs to scan for alternateNames this run")
    args = ap.parse_args()
    CORPUS.mkdir(exist_ok=True)
    if args.fetch:
        harvest(args.fetch)
    if not PAIRS.exists():
        print("no variant pairs yet — run with --fetch")
        return 0

    pairs = [json.loads(ln) for ln in PAIRS.read_text().splitlines() if ln.strip()]
    # dedupe identical (base name, alt) pairs
    seen, uniq = set(), []
    for p in pairs:
        k = (p["base"]["name_norm"], p["alt_name"])
        if k not in seen:
            seen.add(k)
            uniq.append(p)
    print(f"scoring {len(uniq)} unique real variant pairs\n")

    df = load_corpus()
    linker = Linker(df, json.loads(MODEL.read_text()), db_api=DuckDBAPI())

    buckets = {"<0.7": [0, 0, 0], "0.7-0.9": [0, 0, 0], ">=0.9": [0, 0, 0]}  # [n, splink, difflib]
    examples = []
    for p in uniq:
        b = p["base"]
        rec1 = {**b, "record_id": "v1", "address_local": _strip_country(b["address_norm"], b["jurisdiction"])}
        rec2 = {**rec1, "record_id": "v2", "name_norm": p["alt_name"]}
        ratio = difflib.SequenceMatcher(a=b["name_norm"], b=p["alt_name"]).ratio()
        try:
            sp = float(linker.inference.compare_two_records(rec1, rec2)
                       .as_pandas_dataframe()["match_probability"].iloc[0])
        except Exception as e:  # noqa: BLE001
            print(f"compare failed: {type(e).__name__}: {e}")
            return 1
        bucket = "<0.7" if ratio < 0.7 else ("0.7-0.9" if ratio < 0.9 else ">=0.9")
        buckets[bucket][0] += 1
        buckets[bucket][1] += int(sp >= 0.5)
        buckets[bucket][2] += int(ratio >= BASELINE)
        if ratio < 0.9 and len(examples) < 6:
            examples.append((b["name_norm"], p["alt_name"], round(ratio, 2), round(sp, 2)))

    print(f"{'name-sim bucket':<16}{'n':>4}{'splink>=0.5':>13}{'difflib>=0.88':>15}")
    for k, (n, s, d) in buckets.items():
        if n:
            print(f"{k:<16}{n:>4}{s:>13}{d:>15}")
    print("\n(every pair is a TRUE match — real publisher alias; higher = better recovery)")
    if examples:
        print("\nexamples (name | alias | name-sim | splink p):")
        for a, b, r, sp in examples:
            print(f"  '{a}'  ~  '{b}'   sim={r}  splink={sp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
