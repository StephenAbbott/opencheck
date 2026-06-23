# New Zealand — director / shareholder associations

A lazy, panel-only enrichment on the New Zealand source that flags when a
company's directors or shareholders also hold roles in **other** companies — a
customer-due-diligence indicator for **nominees and mass directorships**.

It uses the NZ **Companies Entity Role Search API** (v3), separate from the NZBN
API that powers the main NZ lookup.

- Endpoint: `GET /nz-associations?company_number=<n>` (never on the main lookup).
- Auth: a **separate** subscription key, `NZBN_ROLE_SEARCH_API_KEY`
  (`Ocp-Apim-Subscription-Key` header). Also requires `OPENCHECK_ALLOW_LIVE` and
  the NZBN `NZBN_API_KEY` (to read the subject's role holders).
- Service: `backend/opencheck/nz_associations.py`; router:
  `backend/opencheck/routers/nz_associations.py`; UI:
  `frontend/src/components/cdd/NzAssociations.tsx`.

## Why this is a matching problem, not a count

The Role Search API is keyed on a **name string** — there is no stable person
id. "How many companies is Jane Smith linked to?" really means "how many role
records exist under the name 'Jane Smith'?", and names are not unique. In a CDD
tool a false *"linked to 200 companies / nominee"* is more harmful than a missed
one, so the design leads with **confidence, not count**, and never asserts that
a person *is* a nominee — it reports what appears under a name, **for review**.

## Confidence tiering

Both the subject's role holders (from the NZBN `FullEntity`) and the Role Search
results carry a `physicalAddress` with a **`pafId`** (NZ Post delivery-point id).
Each candidate match is tiered:

| Tier | Basis | Counted? |
|---|---|---|
| **high** | same `pafId` (exact registered address) | yes |
| **medium** | same / strongly-overlapping address lines | yes |
| **low** | name only (no address corroboration) | **no** — surfaced separately as "weaker matches" |

The headline count is **high + medium only**. The bias is deliberately toward
**precision over recall**: people move, so the same person can carry different
addresses across companies and land in "medium" or be missed — for a red flag
that's the right way to be wrong (under-flag rather than wrongly brand someone).

Note a shared `pafId` means "same registered control point", which is exactly
what nominee detection wants — but it can also be a shared service address (a
formation agent's office). So the drill-down always shows the **evidence** (the
companies, roles and match basis); the number is never presented on its own.

## What it returns

Per director/shareholder of the subject company:

- distinct **other active companies** at high+medium confidence (subject company
  excluded; deduped by company number; ceased directorships skipped);
- the **high-confidence** subset count;
- a split into **as director** vs **as shareholder** (control vs ownership read
  differently for AML);
- the company list — name, role(s), share % (where shareholder), confidence +
  match basis, and a link out;
- a count of **weaker name-only matches** (shown, not counted).

## Disclosure (three layers)

1. **Invitation** — a "Check director & shareholder associations" button on the
   NZ card. Nothing fires until clicked (one button runs all role holders),
   because each role holder is a separate rate-limited API call.
2. **Per-person summary**, ranked most-connected first, with a panel lead
   ("N of M role holders linked") and an always-on honesty caveat.
3. **Drill-down** — the companies and the match basis behind each number.

## Limits and tuning (v1)

- **Panel-only.** This does **not** (yet) emit an OpenCheck risk signal — it
  won't appear in the risk chips, AI summary, PDF or BODS export. That's
  deliberate until the matching is validated against live data.
- **Neutral styling.** No amber "too many" thresholds yet — counts are
  informational so real NZ data can be eyeballed before deciding what
  concentration warrants emphasis.
- **Bounded.** Up to 15 role holders per company, up to 3 result pages per name,
  `registered-only=true`. Results are cached per company number.

## Roadmap

- **Risk signal** — promote to a deterministic `PROLIFIC_ROLE_HOLDER` /
  nominee-adjacent indicator (source-agnostic, so other registers can feed it),
  evidence-linked to the BODS node, once thresholds are tuned.
- **Co-control network** — beyond per-person counts, detect where *several* of a
  company's role holders co-occur on the *same* other companies, surfacing the
  shared control cluster rather than flagging individuals.
