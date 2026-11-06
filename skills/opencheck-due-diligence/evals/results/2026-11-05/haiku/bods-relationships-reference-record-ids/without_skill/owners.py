#!/usr/bin/env python3
"""List the owners of a bank from an opencheck_export_bods response.

Usage: python3 -I owners.py <export.json>

Owners are relationship statements whose subject is a record for the bank
and whose interest is a shareholding (or is flagged beneficial ownership).
Each source publishes its own record of the bank, so owners are listed per
source and shares are never added across sources.
"""
import json
import sys

BANK_NAME = "PERSIA INTERNATIONAL BANK PLC"


def fmt_share(share):
    if not share:
        return "no share stated"
    lo, hi = share.get("minimum"), share.get("maximum")
    lo_x, hi_x = share.get("exclusiveMinimum"), share.get("exclusiveMaximum")
    parts = []
    if lo_x is not None:
        parts.append(f"more than {lo_x}%")
    elif lo is not None:
        parts.append(f"at least {lo}%")
    if hi_x is not None:
        parts.append(f"less than {hi_x}%")
    elif hi is not None:
        parts.append(f"up to {hi}%")
    return " and ".join(parts) if parts else "no share stated"


def party_name(record, rec_id):
    if not record:
        return f"(no record for {rec_id})"
    details = record.get("recordDetails", {})
    if details.get("name"):
        return details["name"]
    names = details.get("names") or []
    if names and names[0].get("fullName"):
        return names[0]["fullName"]
    return f"(unnamed record {rec_id})"


def main(path):
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    resp = data.get("response", data)
    statements = resp.get("statements", [])

    by_record = {}
    for st in statements:
        by_record.setdefault(st["recordId"], st)

    # Every source publishes its own record of the bank; collect them all.
    bank_records = {
        st["recordId"]: st
        for st in statements
        if st.get("recordType") == "entity"
        and st.get("recordDetails", {}).get("name", "").upper() == BANK_NAME
    }
    bank_ids = set(bank_records)

    owners, others = [], []
    for st in statements:
        if st.get("recordType") != "relationship":
            continue
        rd = st.get("recordDetails", {})
        if rd.get("subject") not in bank_ids:
            continue
        src = st.get("source", {})
        for interest in rd.get("interests", []):
            row = {
                "party": party_name(by_record.get(rd.get("interestedParty")), rd.get("interestedParty")),
                "interest": interest.get("type", "unspecified"),
                "share": fmt_share(interest.get("share")),
                "source": f"{src.get('description')} ({src.get('opencheckSourceId')})",
                "retrieved": src.get("retrievedAt", "unknown"),
                "url": src.get("url", ""),
                "statement": st["statementId"],
            }
            is_owner = (interest.get("type") == "shareholding"
                        or interest.get("beneficialOwnershipOrControl") is True)
            (owners if is_owner else others).append(row)

    print(f"LEI: {resp.get('lei')}")
    print(f"Bank: {BANK_NAME}")
    print("Bank records by source:")
    for rid, st in bank_records.items():
        print(f"  {st['source']['opencheckSourceId']:<16} {rid}")
    print()

    print(f"Owners ({len(owners)}):")
    for row in sorted(owners, key=lambda r: (r["source"], r["party"])):
        print(f"- {row['party']}")
        print(f"    interest:  {row['interest']}; share: {row['share']}")
        print(f"    source:    {row['source']}, retrieved {row['retrieved']}")
        print(f"    url:       {row['url']}")
        print(f"    statement: {row['statement']}")
    print()

    print(f"Other interests, not counted as owners ({len(others)}):")
    for row in sorted(others, key=lambda r: (r["source"], r["party"])):
        print(f"- {row['party']} [{row['interest']}] via {row['source']}")
    print()
    print("Shares are per source and are not added across sources. "
          "Party names are as each source publishes them; no matching is applied.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: owners.py <export.json>")
    main(sys.argv[1])
