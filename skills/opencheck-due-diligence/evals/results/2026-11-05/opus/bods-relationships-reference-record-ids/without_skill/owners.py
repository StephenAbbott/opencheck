#!/usr/bin/env python3
"""Print every owner of the company in an OpenCheck opencheck_export_bods response.

Usage: python3 owners.py export.json

BODS 0.4 notes this relies on:
  * A relationship's recordDetails.subject / interestedParty hold the *recordId*
    of the entity/person record (not its statementId), so lookups are keyed on recordId.
  * OpenCheck emits one subject record per source (GLEIF, OpenSanctions,
    Companies House, ...), each the declarationSubject of its own statements, so
    the company is every entity record whose recordId equals its declarationSubject.
  * Directors (seniorManagingOfficial) are officers, not owners, and are left out.
  * Shares from different sources are separate readings and are never added.
"""
import json
import sys

NOT_OWNERSHIP = {"seniorManagingOfficial"}


def name_of(rec):
    d = rec["recordDetails"]
    if rec["recordType"] == "entity":
        return d.get("name") or "(unnamed entity)"
    names = d.get("names") or []
    return names[0].get("fullName", "(unnamed person)") if names else "(unnamed person)"


def fmt_share(share):
    if not share:
        return "not stated"
    if "exact" in share:
        return f"{share['exact']:g}%"
    lo = share.get("minimum", share.get("exclusiveMinimum"))
    hi = share.get("maximum", share.get("exclusiveMaximum"))
    lo_op = ">" if "exclusiveMinimum" in share else "≥"
    hi_op = "<" if "exclusiveMaximum" in share else "≤"
    parts = []
    if lo is not None:
        parts.append(f"{lo_op}{lo:g}%")
    if hi is not None:
        parts.append(f"{hi_op}{hi:g}%")
    return " and ".join(parts) or "not stated"


def main(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    resp = data.get("response", data)
    statements = resp["statements"]

    records = {s["recordId"]: s for s in statements if s["recordType"] in ("entity", "person")}
    subjects = {
        s["recordId"] for s in statements
        if s["recordType"] == "entity" and s["recordId"] == s.get("declarationSubject")
    }

    rows = []
    skipped_officers = 0
    for s in statements:
        if s["recordType"] != "relationship":
            continue
        d = s["recordDetails"]
        if d.get("subject") not in subjects:
            continue
        interests = [i for i in d.get("interests", []) if i.get("type") not in NOT_OWNERSHIP]
        if not interests:
            skipped_officers += 1
            continue
        party = records.get(d.get("interestedParty"))
        owner = name_of(party) if party else f"(unresolved record {d.get('interestedParty')})"
        kind = party["recordType"] if party else "?"
        src = s.get("source", {}).get("description", "unknown source")
        for i in interests:
            rows.append((owner, kind, i.get("type", "?"), fmt_share(i.get("share")),
                         i.get("directOrIndirect", "-"), src))

    company = resp.get("lei", "")
    print(f"Owners of {name_of(records[next(iter(subjects))])} (LEI {company})")
    print(f"Read from {len(subjects)} subject records, one per source; shares are per source and never added.\n")
    headers = ("Owner", "Kind", "Interest", "Share", "Direct?", "Source")
    widths = [max(len(str(r[c])) for r in rows + [headers]) for c in range(len(headers))]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))
    for r in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
    print(f"\n{len(rows)} ownership/control interests; {skipped_officers} director relationships left out (officers, not owners).")
    for n in resp.get("license_notices", []):
        print(f"Licence: {n['notice']}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "export-persia.json")
