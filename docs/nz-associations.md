# New Zealand — director / shareholder associations

A lazy, on-demand enrichment that shows when a New Zealand company's directors
or shareholders also hold roles in **other** companies — a
customer-due-diligence indicator for **nominees and mass directorships**.

**Where it shows (since 19 Oct 2026):** on each person's card in the
**BackgroundCheck** tab, as a "Companies on the New Zealand register" control
beside the Estonian register's "Companies on the Estonian register" — one person,
fetched when asked for, listed and never drawn as graph nodes, absent from a
saved report. Until then it was a company-wide "Director & shareholder
associations" panel on QuickCheck's New Zealand source card; that card now says
where it went. Two things did not move: corporate shareholders (BackgroundCheck
lists people only — their other companies stay in the company-wide endpoint),
and the company-wide endpoint itself, which stays in the documented API.

It uses the NZ **Companies Entity Role Search API** (v3), separate from the NZBN
API that powers the main NZ lookup.

- Endpoints (never on the main lookup):
  - `GET /nz-associations/person?company_number=<n>&name=<name>` — one director
    or shareholder, what the BackgroundCheck card asks. The name must be a role
    holder of that company as the filing gives it (the filing supplies the
    address each match is graded against, and keeps this from becoming a free
    name search); anything else answers `available: false`. One Role Search
    call, cached per company and name; a cached company-wide result answers it
    with no call.
  - `GET /nz-associations?company_number=<n>` — every role holder of the
    company (up to 60 searches), the original panel's view.
- Auth: a **separate** subscription key, `NZBN_ROLE_SEARCH_API_KEY`
  (`Ocp-Apim-Subscription-Key` header). Also requires `OPENCHECK_ALLOW_LIVE` and
  the NZBN `NZBN_API_KEY` (to read the subject's role holders).
- Service: `backend/opencheck/nz_associations.py`; router:
  `backend/opencheck/routers/nz_associations.py`; UI:
  `frontend/src/components/cdd/NzPersonCompanies.tsx` (values in
  `frontend/src/lib/nzPerson.ts`; the person's anchor, `nzRoleHolder`, is read
  off the NZ relationship statements by `lib/backgroundCheck.ts`).

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
surfaced as confidence, not proof. From 18 November 2026 that caveat becomes the
common case for any director who elects an **alternative address**: see
[Alternative addresses](#alternative-addresses-amendment-act-2025-in-force-18-november-2026)
below.

## Alternative addresses (Amendment Act 2025, in force 18 November 2026)

The **Companies (Address Information) Amendment Act 2025** lets a director — and
a shareholder who is that director or lives with them — show an **alternative
physical address** on the public Companies Register in place of their
residential one. It commences **18 November 2026** (the NZBN and Companies APIs
are offline 7pm–midnight NZ time on 17 November, 06:00–11:00 UTC).

The API versions do not change; the address fields do. On the NZBN v5 side,
`roles[x].roleAddress[x].addressType` and
`company-details.shareholding.shareAllocation[x].shareholder[x].shareholderAddress.addressType`
become `PHYSICAL`, `ALTERNATIVE` or null. A **public viewer** (which is what
OpenCheck is) sees `ALTERNATIVE` where one is filed and otherwise null — and
**null means residential**, the pre-Act behaviour, not "unknown". An authority
holder sees `PHYSICAL` and/or `ALTERNATIVE`, and for *directors* gets **both
blocks** when both exist. OpenCheck does not read the Companies v2 API, whose
`physicalOrPostalAddresses[x].addressType` gains `Alternative`, so that half of
the change does not reach us.

Two consequences for this panel:

- **`_role_address()` chooses, it no longer takes `[0]`.** `roleAddress` can
  hold more than one block and MBIE does not specify the order, so position is
  a coin-flip between a home address and a service address. The adapter ranks a
  current block before an ended one, then residential (`PHYSICAL` or null)
  before `ALTERNATIVE`, and carries the chosen block's type through the
  normalised role and shareholder rows as `address_type`. Pre-Act every block
  is untyped and current, so this returns what `[0]` did.
- **A shared alternative address is not a shared residence.** An alternative
  address is typically the office of the accountant or agent who provides it,
  shared by every one of their clients, so a matching `pafId` stops being
  evidence about a *person*. Where **either** side's address is `ALTERNATIVE`
  the match is graded **at most medium** (decided 27 October 2026), and the
  basis says "Same alternative address — may be a shared service address" or
  "Overlapping alternative address". It still counts as address-matched. Until
  27 October 2026 only the wording changed and the tier did not, because
  Role Search did not yet say what it would serve.

### What Role Search does from 18 November 2026

MBIE's notice of 30 September covered the NZBN v5 and Companies v2 APIs and
did not mention the **Companies Entity Role Search API (v3)**, which is the API
this panel searches. OpenCheck tested it in the sandbox on 1 October 2026 and
found it served the director's **residential** address, so a residential
address withheld from the NZBN API was still reachable by name. We reported
that to MBIE (Case 00094468, 14 October); on 15 October MBIE confirmed it was
unintended, and on 27 October it shipped a fix to the sandbox and announced it
for production on 18 November:

- For a role holder with an alternative address, Role Search returns an
  **empty** block: `"physicalAddress": {"addressLines": []}` in JSON (an empty
  `<physicalAddress />` in XML). Checked in the sandbox on 27 October: no
  `pafId`, `postCode` or `countryCode` either, for the director's record and
  the director-and-shareholder record alike.
- Role holders with a residential address are unchanged — full address and
  `pafId`, matching the NZBN record.
- Role Search has **no `addressType`** field and MBIE plans none (perhaps with
  a future Companies Register replacement, not on its work plan).
- To get the address, MBIE says to take `associatedCompanyNzbn` from the Role
  Search record and read the company from the NZBN API (`GET
  /entities/{nzbn}`, or the `/roles` and `/company-details` sub-resources).

What OpenCheck does with that:

| Role Search block | How the company is graded | `address_source` |
|---|---|---|
| has an address line or a `pafId` | against it, as before | `role_search` |
| no address line and no `pafId` (withheld; a postcode alone is ignored) | against the person's address on **the company's own NZBN record** — the full entity, through the `nz_companies` adapter's fetch and cache, holder found by full name, else first + last name | `company_record` |
| withheld, past the cap | name-only: "Same name — address not published by Role Search, not checked" | `not_checked` |
| withheld, the NZBN read failed | name-only: "Same name — the company's own record could not be read" | `check_failed` |

- **At most 25 companies per person** are read (`_FOLLOWUP_CAP`), in Role
  Search's order; the rest are counted (`withheld_not_checked`) and the card
  says how many. A director-and-shareholder record carries the company's NZBN
  only on its `shareholdings[]`, and that is where it is read from.
- **The company-wide view** (`GET /nz-associations`, up to 60 names) makes no
  follow-up reads; its withheld companies are `not_checked`, and a cached
  company-wide answer stands in for one person only when nothing was left
  unchecked.
- **A failed read is never cached as the answer** (the Phase 262 rule): a
  person view with a `check_failed` company is not cached, and the
  `nz_companies` adapter now raises on any NZBN failure other than a 404
  instead of caching an empty company. That matters on **17 November 2026,
  7pm–midnight NZ time (06:00–11:00 UTC)**, when MBIE takes the production
  NZBN and Companies APIs offline for the release: a lookup in that window
  shows the NZ source as failed, not as a company with no directors.

The sandbox is **synthetic** data (generated companies, placeholder
directors, nearly every address on one Wellington `pafId`), so the opt-in live
smoke tests in `backend/tests/test_live_smoke.py` pin what it shows: that the
withheld block carries nothing (the test fails if a `pafId` or postcode
reappears — the residential address leaking again), that a residential holder
keeps a full address matching the NZBN record, that the NZBN record supplies
the withheld address as `ALTERNATIVE`, and that Role Search still has no
`addressType`. They need the sandbox keys (`NZBN_SANDBOX_API_KEY`,
`NZBN_ROLE_SEARCH_SANDBOX_API_KEY` — the production keys 401 against
`/sandbox/`) **exported into the environment**, since the test tier runs with
`OPENCHECK_DISABLE_DOTENV=1`:

```
set -a && . ./.env && set +a && cd backend \
  && pytest --run-live -m live tests/test_live_smoke.py -k sandbox
```

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
- where Role Search withheld the address, how many companies were graded on
  the company's own record (`withheld_checked`), how many were not checked
  (`withheld_not_checked`) and how many could not be read
  (`withheld_check_failed`), with each company's `address_source`.

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
- **API request shape.** The Role Search API **requires** `role-type` (`SHR` /
  `DIR` / `ALL`) — we send `role-type=ALL` to get every company a name is linked
  to by any role. (An early version omitted this required parameter, which
  returned nothing and made the panel show no associations for anyone.) Names are
  queried in the Companies Office recommended order, **`LastName FirstName
  MiddleName`** (`_person_search_name()` in the NZBN adapter), which exact-matches
  last + first and does a 'starts with' on the middle name — tolerant of the
  middle name being entered for one company but not another. Organisation
  shareholders fall back to the display name (matched fuzzily).

## Roadmap

- **Risk signal** — promote to a deterministic `PROLIFIC_ROLE_HOLDER` /
  nominee-adjacent indicator (source-agnostic, so other registers can feed it),
  evidence-linked to the BODS node, once thresholds are tuned.
- **Co-control network** — beyond per-person counts, detect where *several* of a
  company's role holders co-occur on the *same* other companies, surfacing the
  shared control cluster rather than flagging individuals.
