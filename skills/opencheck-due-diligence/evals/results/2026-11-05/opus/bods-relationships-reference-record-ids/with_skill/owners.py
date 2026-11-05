#!/usr/bin/env python3
"""List the owners of a company from an OpenCheck opencheck_export_bods response.

Usage: python3 owners.py export.json

Reads BODS v0.4 statements (response.statements) and prints every party holding
a shareholding or voting-rights interest in the company, with its share and the
source that published it.

Rules followed:
- Relationships reference parties by recordId (not statementId), so nodes are
  indexed on recordId.
- Only shareholding / votingRights interests are ownership. Directors
  (seniorManagingOfficial), GLEIF reporting exceptions (accounting
  consolidation), and unknownInterest links are listed separately, not as owners.
- Shares are printed per source, exactly as published (PSC bands are ranges),
  and never added across sources.
- Ended relationships (recordStatus closed, or every interest with an endDate
  on or before today) are marked as former.
"""
import json
import sys
from datetime import date

OWNERSHIP_TYPES = {"shareholding", "votingRights"}


def load_statements(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    resp = data.get("response", data)
    return resp["statements"] if isinstance(resp, dict) else resp


def latest_by_record(statements):
    """Index statements on recordId, keeping the latest statementDate."""
    idx = {}
    for s in statements:
        rid = s.get("recordId")
        if rid is None:
            continue
        if rid not in idx or s.get("statementDate", "") >= idx[rid].get("statementDate", ""):
            idx[rid] = s
    return idx


def party_name(ref, idx):
    if isinstance(ref, dict) and "unspecified" in ref:
        u = ref["unspecified"]
        return f"[unspecified: {u.get('reason')}{' – ' + u['description'] if u.get('description') else ''}]"
    st = idx.get(ref)
    if st is None:
        return f"[unresolved recordId {ref}]"
    rd = st["recordDetails"]
    if st["recordType"] == "person":
        names = rd.get("names") or [{}]
        name = names[0].get("fullName", "?")
        kind = rd.get("personType", "person")
    else:
        name = rd.get("name", "?")
        kind = (rd.get("entityType") or {}).get("type", "entity")
    juris = (rd.get("jurisdiction") or {}).get("code") or \
        ",".join(n.get("code", "") for n in rd.get("nationalities", []) if n.get("code"))
    return f"{name} ({kind}{', ' + juris if juris else ''})"


def fmt_share(share):
    if not share:
        return "no share stated"
    if "exact" in share:
        return f"{share['exact']}%"
    lo = share.get("exclusiveMinimum", share.get("minimum"))
    hi = share.get("exclusiveMaximum", share.get("maximum"))
    lo_op = ">" if "exclusiveMinimum" in share else "≥"
    hi_op = "<" if "exclusiveMaximum" in share else "≤"
    parts = []
    if lo is not None:
        parts.append(f"{lo_op}{lo}%")
    if hi is not None:
        parts.append(f"{hi_op}{hi}%")
    return " and ".join(parts) or "no share stated"


def is_ended(rel, interests, today):
    if rel.get("recordStatus") == "closed":
        return True
    return bool(interests) and all(
        i.get("endDate") and i["endDate"] <= today for i in interests)


def main(path):
    statements = load_statements(path)
    idx = latest_by_record(statements)
    today = date.today().isoformat()

    owners, other = [], []
    for rel in idx.values():
        if rel.get("recordType") != "relationship":
            continue
        rd = rel["recordDetails"]
        subject = party_name(rd["subject"], idx)
        holder = party_name(rd["interestedParty"], idx)
        src = rel.get("source", {})
        source = f"{src.get('description', '?')} [{src.get('opencheckSourceId', '?')}]"
        interests = rd.get("interests", [])
        status = "former" if is_ended(rel, interests, today) else "current"
        for i in interests:
            row = {
                "holder": holder, "subject": subject, "type": i.get("type"),
                "share": fmt_share(i.get("share")),
                "direct": i.get("directOrIndirect", "not stated"),
                "bo": i.get("beneficialOwnershipOrControl"),
                "details": i.get("details", ""), "source": source,
                "status": status, "date": rel.get("statementDate"),
            }
            (owners if i.get("type") in OWNERSHIP_TYPES else other).append(row)

    owners.sort(key=lambda r: (r["source"], r["holder"]))
    print(f"OWNERS (shareholding / voting rights) — {len(owners)} interest(s); "
          "shares are per source and must not be added across sources\n")
    for r in owners:
        print(f"- {r['holder']}")
        print(f"    share:  {r['share']}  ({r['type']}, {r['direct']}, {r['status']})")
        if r["details"]:
            print(f"    filed:  {r['details']}")
        bo = {True: "yes", False: "no (not declared as beneficial owner)", None: "not stated"}[r["bo"]]
        print(f"    BO flag: {bo}")
        print(f"    source: {r['source']}, statement dated {r['date']}")
        print(f"    of:     {r['subject']}")

    print(f"\nOTHER RELATIONSHIPS (not ownership) — {len(other)}\n")
    for r in sorted(other, key=lambda r: (r["source"], r["type"], r["holder"])):
        extra = f" — {r['details']}" if r["details"] else ""
        print(f"- {r['holder']}: {r['type']} ({r['status']}) [{r['source']}]{extra}")

    notices = {}
    with open(path, encoding="utf-8") as f:
        resp = json.load(f).get("response", {})
    for n in resp.get("license_notices", []) if isinstance(resp, dict) else []:
        notices[n["source_id"]] = n["notice"]
    if notices:
        print("\nLICENCE NOTICES")
        for sid, n in notices.items():
            print(f"- {sid}: {n}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "export.json")
