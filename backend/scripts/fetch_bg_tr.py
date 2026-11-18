#!/usr/bin/env python3
"""Download the Bulgarian Commercial Register daily files from data.egov.bg.

Run on a Mac with a Bulgarian VPN on (the portal refuses non-Bulgarian
addresses). Standard library only.

    python3 scripts/fetch_bg_tr.py list              # what the portal holds; downloads nothing
    python3 scripts/fetch_bg_tr.py get --since 2021-01-01 --out ~/opencheck-bg-data
    python3 scripts/fetch_bg_tr.py get --all

Files land in ./bg_tr/<dataset name>/<resource name>.xml.gz. A file that is
already there is skipped, so the script can be stopped and re-run. Requests
are spaced at 1.1 s (the portal allows 60 a minute per IP) and a 429 waits a
minute. Nothing is sent anywhere but data.egov.bg.
"""
import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request

API = "https://data.egov.bg/api/"
DOWNLOAD = "https://data.egov.bg/resource/download/{uri}/{fmt}"
UA = "Mozilla/5.0 (Macintosh) opencheck-bg-fetch/1"
SPACING_S = 1.1
TITLE = re.compile(r"Търговски регистър", re.I)
_last = [0.0]


def _wait():
    gap = time.time() - _last[0]
    if gap < SPACING_S:
        time.sleep(SPACING_S - gap)
    _last[0] = time.time()


def _open(req, tries=5):
    for n in range(tries):
        _wait()
        try:
            return urllib.request.urlopen(req, timeout=300)
        except urllib.error.HTTPError as e:
            if e.code == 403:
                sys.exit("403 from data.egov.bg: is the Bulgarian VPN on?")
            if e.code == 429 or e.code >= 500:
                print(f"  HTTP {e.code}, waiting 60 s", file=sys.stderr)
                time.sleep(60)
                continue
            raise
        except urllib.error.URLError as e:
            print(f"  {e.reason}, retrying in 20 s", file=sys.stderr)
            time.sleep(20)
    raise RuntimeError("gave up after repeated failures")


def call(method, body):
    req = urllib.request.Request(
        API + method, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "User-Agent": UA})
    with _open(req) as r:
        return json.load(r)


def paged(method, criteria, key):
    page, out = 1, []
    while True:
        res = call(method, {"criteria": criteria, "records_per_page": 100,
                            "page_number": page})
        if not res.get("success", True):
            sys.exit(f"{method} refused: {json.dumps(res, ensure_ascii=False)[:500]}")
        rows = res.get(key) or res.get("data") or []
        if page == 1 and not rows:
            print(f"{method}: no rows; answer was {json.dumps(res, ensure_ascii=False)[:500]}")
        out += rows
        if len(rows) < 100:
            return out
        page += 1


def datasets():
    rows = paged("listDatasets", {"keywords": "Търговски регистър", "locale": "bg"},
                 "datasets")
    keep = [d for d in rows if TITLE.search(d.get("name") or "")]
    return sorted(keep, key=lambda d: d.get("created_at") or "")


def resources(ds_uri):
    return paged("listResources", {"dataset_uri": ds_uri, "locale": "bg"}, "resources")


def day_of(name):
    name = name or ""
    m = re.search(r"((?:19|20)\d{2})-(\d{1,2})-(\d{1,2})", name)  # 2020-10-12
    if m:
        return f"{m[1]}-{int(m[2]):02d}-{int(m[3]):02d}"
    # 13.11.2026, and the register's slips: "05..10.2024", "12.12 2023"
    m = re.search(r"(?<!\d)(\d{1,2})[./ -]+(\d{1,2})[./ -]+((?:19|20)\d{2})", name)
    if m:
        return f"{m[3]}-{int(m[2]):02d}-{int(m[1]):02d}"
    m = re.search(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", name)  # 20210104
    if m:
        return f"{m[1]}-{m[2]}-{m[3]}"
    m = re.search(r"(?<!\d)(\d{2})(\d{2})(20\d{2})(?!\d)", name)  # 07012023
    return f"{m[3]}-{m[2]}-{m[1]}" if m else None


def safe(s):
    return re.sub(r"[^\w.-]+", "_", s, flags=re.U).strip("_")[:80]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=("list", "get"))
    ap.add_argument("--since", help="only resources dated on or after YYYY-MM-DD")
    ap.add_argument("--all", action="store_true", help="every resource")
    ap.add_argument("--out", default="bg_tr")
    a = ap.parse_args()
    if a.mode == "get" and not (a.since or a.all):
        sys.exit("get needs --since YYYY-MM-DD or --all")

    sets = datasets()
    print(f"{len(sets)} Commercial Register datasets")
    plan = []
    for d in sets:
        rs = resources(d["uri"])
        days = sorted(x for x in (day_of(r.get("name")) for r in rs) if x)
        span = f"{days[0]} → {days[-1]}" if days else "no dated files"
        print(f"- {d['name']} | {d['uri']} | {len(rs)} files | {span} | updated {d.get('updated_at')}")
        if a.mode == "list":
            years = {}
            for x in days:
                years[x[:4]] = years.get(x[:4], 0) + 1
            print("  files per year:", ", ".join(f"{y}: {n}" for y, n in sorted(years.items())))
            undated = [r.get("name") for r in rs if not day_of(r.get("name"))]
            print(f"  {len(undated)} files with no date in the name, e.g. {undated[:10]}")
            fmts = {}
            for r in rs:
                fmts[r.get("file_format")] = fmts.get(r.get("file_format"), 0) + 1
            print("  formats:", fmts)
            continue
        for r in rs:
            day = day_of(r.get("name"))
            if a.all or (day and day >= a.since):
                plan.append((d, r))
    if a.mode == "list":
        return

    print(f"\n{len(plan)} files to fetch into {os.path.abspath(a.out)}")
    names = {}
    for d, r in plan:
        k = (d["name"], safe(r.get("name") or r["uri"]))
        names[k] = names.get(k, 0) + 1
    failed = []
    for i, (d, r) in enumerate(plan, 1):
        folder = os.path.join(a.out, safe(d["name"]))
        os.makedirs(folder, exist_ok=True)
        fmt = (r.get("file_format") or "xml").lower()
        zipped = fmt == "zip"  # already compressed: saved as it comes
        stem = safe(r.get("name") or r["uri"])
        if names[(d["name"], stem)] > 1:  # two resources share a name
            shared = os.path.join(folder, stem + (".zip" if zipped else ".xml.gz"))
            if os.path.exists(shared):  # an earlier run saved one of them here
                os.makedirs(os.path.join(folder, "_bad"), exist_ok=True)
                os.replace(shared, os.path.join(folder, "_bad", os.path.basename(shared)))
            stem += "_" + r["uri"][:8]
        path = os.path.join(folder, stem + (".zip" if zipped else ".xml.gz"))
        if os.path.exists(path):
            if looks_right(path, zipped):
                continue
            os.makedirs(os.path.join(folder, "_bad"), exist_ok=True)
            os.replace(path, os.path.join(folder, "_bad", os.path.basename(path)))
        if fetch_one(r, fmt, zipped, path):
            print(f"[{i}/{len(plan)}] {path} ({os.path.getsize(path) // 1024} KB)")
        else:
            failed.append(r)
            print(f"[{i}/{len(plan)}] FAILED {r.get('name')} ({r['uri']}, {fmt})")
            print("   resource as listed:", json.dumps(r, ensure_ascii=False)[:600])
    print(f"\nDone. {len(failed)} files could not be fetched.")


def looks_right(path, zipped):
    """A file is kept only if it is a zip (PK) or gzipped XML, never a web page."""
    try:
        if zipped:
            with open(path, "rb") as fh:
                return fh.read(2) == b"PK"
        with gzip.open(path, "rb") as fh:
            head = fh.read(512).lstrip(b"\xef\xbb\xbf \r\n\t").lower()
        return head.startswith(b"<") and b"<!doctype html" not in head and b"<html" not in head
    except (OSError, EOFError):
        return False


def fetch_one(r, fmt, zipped, path):
    urls = [DOWNLOAD.format(uri=r["uri"], fmt=fmt)]
    if zipped:  # the portal's link for a ZIP resource is not certain; try the likely ones
        urls += [DOWNLOAD.format(uri=r["uri"], fmt="ZIP"),
                 f"https://data.egov.bg/resource/download/{r['uri']}"]
    if r.get("resource_url"):
        urls.append(r["resource_url"])
    tmp = path + ".part"
    for url in urls:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with _open(req) as resp, (open(tmp, "wb") if zipped else gzip.open(tmp, "wb")) as fh:
                while chunk := resp.read(1 << 20):
                    fh.write(chunk)
        except urllib.error.HTTPError as e:
            print(f"  {url} -> HTTP {e.code}", file=sys.stderr)
            continue
        if looks_right(tmp, zipped):
            os.replace(tmp, path)
            return True
        print(f"  {url} -> a web page, not the file", file=sys.stderr)
    if os.path.exists(tmp):
        os.remove(tmp)
    return False

if __name__ == "__main__":
    main()
