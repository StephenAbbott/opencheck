"""One-off probe of AJPES's open PRS CSV against GLEIF's Slovenian LEIs.

Prints aggregate measurements only (no rows naming a sole trader).
Run: python3 -I probe_slovenia_prs.py <opsiprs.csv>
"""
import collections, csv, io, json, re, sys, time, unicodedata, urllib.request, zipfile

csv_path = sys.argv[1]

raw = open(csv_path, "rb").read()
print("CSV bytes", len(raw), "BOM", raw[:2])
text = raw.decode("utf-16")
rows = list(csv.reader(io.StringIO(text)))
header, body = rows[0], rows[1:]
print("header", header)
print("rows", len(body), "bad widths", sum(1 for r in body if len(r) != len(header)))
body = [r for r in body if len(r) == len(header)]
nums = collections.Counter(r[0] for r in body)
print("distinct numbers", len(nums), "dups", sum(1 for v in nums.values() if v > 1))
print("number shapes", collections.Counter(re.sub(r"\d", "9", r[0]) for r in body).most_common(5))
print("number suffix (last 3)", collections.Counter(r[0][-3:] for r in body).most_common(8))
print("forms", collections.Counter(r[3] for r in body).most_common(60))
print("organs", collections.Counter(re.sub(r"izpostava .*", "izpostava *", r[4]) for r in body).most_common(20))
print("countries", collections.Counter(r[11] for r in body).most_common(10))
print("HSEID blank", sum(1 for r in body if not r[2].strip()))
print("street blank", sum(1 for r in body if not r[5].strip()))
# Name for a 7-digit stem: main units end 000, others are sub-units (branches)?
stem = collections.Counter(r[0][:7] for r in body)
print("stems with >1 unit", sum(1 for v in stem.values() if v > 1))

by = {r[0]: r for r in body}

def fold(s):
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()

recs, page = [], 1
while True:
    url = ("https://api.gleif.org/api/v1/lei-records?filter%5Bentity.legalAddress.country%5D=SI"
           f"&page%5Bsize%5D=200&page%5Bnumber%5D={page}")
    for a in range(5):
        try:
            d = json.load(urllib.request.urlopen(url, timeout=60)); break
        except Exception as e:
            time.sleep(5 * (a + 1))
    for r in d["data"]:
        a = r["attributes"]; e = a["entity"]
        recs.append(dict(lei=a["lei"], name=e["legalName"]["name"], ra=(e.get("registeredAt") or {}).get("id"),
                         ras=e.get("registeredAs") or "", st=e.get("status"), reg=a["registration"]["status"],
                         cat=e.get("category")))
    if page >= d["meta"]["pagination"]["lastPage"]:
        break
    page += 1
print("GLEIF SI records", len(recs))
for label, sel in [("ISSUED", lambda r: r["reg"] == "ISSUED"), ("ALL", lambda r: True)]:
    sub = [r for r in recs if sel(r) and r["ra"] == "RA000529"]
    hit = [r for r in sub if r["ras"] in by]
    agree = [r for r in hit if fold(r["name"])[:12] == fold(by[r["ras"]][1])[:12]]
    print(label, "RA000529", len(sub), "in CSV", len(hit), "name prefix agrees", len(agree))
    print(label, "misses by entity status", collections.Counter(r["st"] for r in sub if r["ras"] not in by))
    print(label, "misses by reg status", collections.Counter(r["reg"] for r in sub if r["ras"] not in by))
    print(label, "hits by form", collections.Counter(by[r["ras"]][3] for r in hit).most_common(15))
    print(label, "hits by category", collections.Counter(r["cat"] for r in hit))
iss = [r for r in recs if r["reg"] == "ISSUED" and r["ra"] == "RA000529"]
print("sample ISSUED misses (legal persons only):")
for r in [r for r in iss if r["ras"] not in by and r["cat"] != "SOLE_PROPRIETOR"][:15]:
    print("  ", r["ras"], r["name"], r["st"], r["cat"])
print("sample ISSUED name disagreements:")
for r in [r for r in iss if r["ras"] in by and fold(r["name"])[:12] != fold(by[r["ras"]][1])[:12]][:15]:
    print("  ", r["ras"], "|", r["name"], "|", by[r["ras"]][1])
inactive = [r for r in recs if r["st"] == "INACTIVE" and r["ra"] == "RA000529"]
print("INACTIVE GLEIF entities", len(inactive), "of which in CSV", sum(1 for r in inactive if r["ras"] in by))
