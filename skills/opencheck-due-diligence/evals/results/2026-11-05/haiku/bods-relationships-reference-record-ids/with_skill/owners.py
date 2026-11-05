#!/usr/bin/env python3
"""Print every owner of the company in an OpenCheck BODS v0.4 export.

Usage:  python3 -I owners.py <export.json>

Reads the output of opencheck_export_bods (its `response` object, with
statements under `response.statements`).

Rules used:
  * Owners are relationship statements whose interest is a shareholding or an
    unknown interest, held by a named party. Each is printed with the share
    the filing states (or "no share stated") and the source of that statement.
  * Each subject record (one per source describing the company) is printed
    separately. Shares are never added across sources.
  * Control reported only as a GLEIF reporting exception (an unnamed natural
    person) and officers (directors, seniorManagingOfficial) are NOT owners.
    They are listed in their own sections so nothing is silently dropped.
  * T-Rank integrated-ownership figures are printed as published, per source.
"""
import json
import sys


def party_name(rec):
    d = rec.get("recordDetails", {})
    if rec.get("recordType") == "entity":
        return d.get("name", "(unnamed entity)")
    names = d.get("names") or [{}]
    return names[0].get("fullName", "(unnamed person)")


def is_unnamed(rec):
    return (rec.get("recordType") == "person"
            and rec.get("recordDetails", {}).get("personType") == "unknownPerson")


def identifiers(rec):
    ids = rec.get("recordDetails", {}).get("identifiers") or []
    return "; ".join(f'{i.get("scheme")} {i.get("id")}' for i in ids) or "none"


def source_text(stmt):
    s = stmt.get("source", {})
    return (f'{s.get("description", "?")} [{s.get("opencheckSourceId", "?")}], '
            f'retrieved {s.get("retrievedAt", "?")}, {s.get("url", "")}')


def share_text(interest):
    share = interest.get("share")
    if share:
        if "exact" in share:
            return f'{share["exact"]:g}%'
        lo = share.get("exclusiveMinimum")
        hi = share.get("maximum")
        if lo is not None and hi is not None:
            return f"more than {lo:g}% and up to {hi:g}%"
        if share.get("minimum") is not None and hi is not None:
            return f'{share["minimum"]:g}% to {hi:g}%'
        if hi is not None:
            return f"up to {hi:g}%"
    if interest.get("type") == "unknownInterest":
        return "interest type unknown, no share stated"
    return "no share stated"


def main(path):
    with open(path, encoding="utf-8") as fh:
        export = json.load(fh)
    resp = export["response"]
    statements = resp["statements"]

    # One record per recordId; keep the most recently dated statement if repeated.
    records = {}
    for s in statements:
        if s.get("recordType") in ("entity", "person"):
            rid = s["recordId"]
            if rid not in records or s["statementDate"] > records[rid]["statementDate"]:
                records[rid] = s

    # A subject record describes the company itself (declarationSubject == its own id).
    subjects = [rid for rid, rec in records.items()
                if rec["recordType"] == "entity" and rec.get("declarationSubject") == rid]

    rels = [s for s in statements if s.get("recordType") == "relationship"]

    lei = resp.get("lei", "?")
    company_names = sorted({party_name(records[r]) for r in subjects})
    print(f"Owners of {', '.join(company_names)} (LEI {lei})")
    print(f"Statements: {resp.get('statement_count')}  |  "
          f"subject records: {len(subjects)}  |  export as of "
          f"{resp.get('integrated_ownership', {}).get('as_of', '?')}")
    print("Shares are per source. Figures from different sources are not added.")
    print()

    total_owners = 0
    for sid in subjects:
        subj = records[sid]
        print(f"== Subject record {sid}")
        print(f"   {party_name(subj)} | identifiers: {identifiers(subj)}")
        print(f"   described by: {source_text(subj)}")
        print()

        owners, unnamed_control, officers, other = [], [], [], []
        for rel in rels:
            d = rel["recordDetails"]
            if d.get("subject") != sid:
                continue
            party = records.get(d.get("interestedParty"))
            for interest in d.get("interests", []):
                itype = interest.get("type")
                if itype == "seniorManagingOfficial":
                    officers.append((rel, party, interest))
                elif party is None or is_unnamed(party):
                    unnamed_control.append((rel, party, interest))
                elif itype in ("shareholding", "unknownInterest"):
                    owners.append((rel, party, interest))
                else:
                    other.append((rel, party, interest))

        print("   Owners (named parties with a shareholding or unknown interest):")
        if not owners:
            print("     none in this record")
        for rel, party, interest in owners:
            total_owners += 1
            print(f"   - {party_name(party)} [{party.get('recordType')}]")
            print(f"       share: {share_text(interest)}"
                  f" | direct/indirect: {interest.get('directOrIndirect', 'not stated')}"
                  f" | statement date: {rel.get('statementDate')}")
            if interest.get("details"):
                print(f"       filed as: {interest['details']}")
            print(f"       holder identifiers: {identifiers(party)}")
            print(f"       source: {source_text(rel)}")
        print()

        if unnamed_control:
            print("   Control without a named party (not counted as owners):")
            for rel, party, interest in unnamed_control:
                who = party_name(party) if party else "(party not in export)"
                print(f"   - {who} | interest: {interest.get('type')}"
                      f" | {interest.get('details', '')}")
                print(f"       source: {source_text(rel)}")
            print()

        if other:
            print("   Other influence or control (named, not counted as owners):")
            for rel, party, interest in other:
                print(f"   - {party_name(party)} | {interest.get('type')}"
                      f" | {interest.get('details', '')}")
                print(f"       source: {source_text(rel)}")
            print()

        print(f"   Officers (directors, not owners, no shares): {len(officers)}")
        for rel, party, interest in officers:
            print(f"   - {party_name(party)} | {interest.get('details', '')}"
                  f" | source: {source_text(rel)}")
        print()

    io = resp.get("integrated_ownership", {})
    print("== T-Rank integrated ownership (as published, per source, not summed)")
    for h in io.get("holders", []):
        for f in h.get("figures", []):
            flag = " (incomplete)" if f.get("incomplete") else ""
            print(f'   - {h.get("name")} [{h.get("kind")}] {f.get("text")}'
                  f' | source: {f.get("source")} | route: {f.get("route")}{flag}')
    print()

    print("== Licence notices")
    for n in resp.get("license_notices", []):
        print(f"   - {n.get('notice')}")
    print()
    print(f"Owner entries printed: {total_owners}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: owners.py <export.json>")
    main(sys.argv[1])
