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
records exist under the name 'Jane Smith'?", and names are not unique. So the
panel shows every name match but leads with **confidence, not just a count**:
matches are graded by address corroboration and the credible (address-matched)
subset is separated from the name-only one, the per-name register total flags
common names, and it never asserts that a person *is* a nominee — it reports what
appears under a name, **for review**. (An earlier version went further and hid
every name-only match; that suppressed real associations for career directors,
so name-only matches are now shown — clearly labelled — rather than dropped.)

## Confidence grading (address upgrades, it doesn't gate)

Both the subject's role holders (from the NZBN `FullEntity`) and the Role Search
results carry a `physicalAddress` with a **`pafId`** (NZ Post delivery-point id).
**Every name match counts**; the address is used to *grade* each match, not to
exclude it:

| Tier | Basis | Shown / counted? |
|---|---|---|
| **high** | same `pafId` (exact registered address) | yes — "address-matched" |
| **medium** | same / strongly-overlapping address lines | yes — "address-matched" |
| **low** | name matches, address doesn't corroborate | yes — **"name-only"**, clearly labelled |

**Why name-only is shown (the recall fix).** An earlier version counted only
high + medium and hid the rest as "weaker matches (not counted)". In practice
that made the panel empty for exactly the people worth surfacing: a **career
director** files a different address on each board (home, a service address, the
company's registered office), so almost every genuine match landed in "low" and
vanished. Recall collapsed to roughly zero. Now every name match is shown, split
into an **address-matched** subset (high + medium, the credible core) and a
**name-only** subset (low, "may be a different person who shares the name").

The honesty rails carry the weight instead of a hard gate: each person leads with
the `N address-matched, M name-only` split, the per-name register **total**
(`totalResults`) flags common names, the drill-down always shows the **evidence**
(companies, roles, match basis), and nothing is ever asserted as a determination.
A shared `pafId` ("same registered control point") is the strongest signal for
nominee detection — but can also be a shared formation-agent office — so it is
surfaced as confidence, not proof.

## What it returns

Per director/shareholder of the subject company:

- distinct **other active companies** under that name (subject company excluded;
  deduped by company number; ceased directorships skipped);
- the **address-matched** count (high + medium) and the **name-only** count (low),
  plus the **high-confidence** (exact-`pafId`) subset;
- a split into **as director** vs **as shareholder** (control vs ownership read
  differently for AML);
- the company list — name, role(s), share % (where shareholder), confidence +
  match basis, and a link out — ordered address-matched first, then name-only.

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
- **Covers all role holders.** Every director and shareholder is checked
  (directors first, since control matters more for nominee detection), run with
  bounded concurrency (5 parallel calls) and a safety ceiling of 60 — beyond
  which a "+ N more not checked" note is shown rather than silently dropping
  holders. Each name is paged up to 150 records; when the register holds more,
  the API's `totalResults` magnitude is surfaced ("N records under this name —
  only a sample checked") so a prolific name isn't quietly undercounted.
  `registered-only=true`; results cached per company number.

## Roadmap

- **Risk signal** — promote to a deterministic `PROLIFIC_ROLE_HOLDER` /
  nominee-adjacent indicator (source-agnostic, so other registers can feed it),
  evidence-linked to the BODS node, once thresholds are tuned.
- **Co-control network** — beyond per-person counts, detect where *several* of a
  company's role holders co-occur on the *same* other companies, surfacing the
  shared control cluster rather than flagging individuals.
