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
  evidence about a *person*. The **tier is deliberately unchanged** — the
  `address_match_count` / `name_only_count` split and the panel ordering keep
  their meaning — but the **basis wording** changes, so the drill-down says
  "Same alternative address — may be a shared service address" rather than
  "Same registered address". Relabelling is honest at any volume; re-tiering
  would silently move counts on evidence we do not yet have.

### Is Role Search affected? Tested in the sandbox — no.

MBIE's notice covers the NZBN v5 and Companies v2 APIs. It does **not** mention
the **Companies Entity Role Search API (v3)**, which is the API this panel
actually searches. Tested against MBIE's sandbox on 1 October 2026, using the
two alternative-address test entities it named:

| Checked | Result |
|---|---|
| NZBN v5 `addressType` on a director with an alternative address | `ALTERNATIVE` — populated as documented |
| Other directors on the same entity | `addressType` null — null *is* the residential address |
| `PHYSICAL` ever visible to a public viewer | no — only `ALTERNATIVE` or null |
| Does an `ALTERNATIVE` block carry a `pafId`? | **inconsistent** — absent on one test entity, present (`3161519`) on the other |
| Does an `ALTERNATIVE` `pafId` ever equal a residential one? | no |
| Role Search `physicalAddress` fields | `addressLines`, `postCode`, `countryCode`, `pafId` — still **no `addressType`** |
| Does Role Search serve the alternative address? | **no** — its `pafId` for the same director at the same company is a different one |

So of the two branches, the **false-negative** one is what the sandbox shows:
for any director who elects an alternative address, the subject's NZBN address
and the Role Search address disagree, and genuine matches fall from high/medium
to name-only — exactly for the directors who elected privacy. The panel already
shows name-only matches rather than hiding them (the recall fix above), so they
are not lost, only demoted.

Two caveats on that result. The sandbox is **synthetic** data, not a clone of
production — the test entities are generated companies (`MANDATORY
CONTEXTUALLY-BASED SUCCESS LIMITED`) with placeholder directors, and almost
every synthetic address resolves to one Wellington `pafId`. And a sandbox index
that has not been refreshed would look identical to an API that is deliberately
out of scope. So this is evidence, not proof.

It also raises something larger than our matching. If the address Role Search
keeps serving is the **residential** one, then the Act's protection does not
hold across MBIE's own APIs: a residential address withheld from the NZBN API
would still be reachable through entity-roles v3. That is the first question in
the feedback sent to MBIE before the **14 October 2026** deadline
(`helpdesk@mail.api.business.govt.nz`), alongside the inconsistent `pafId` on
alternative addresses.

Because the answer is a sandbox observation rather than a commitment from MBIE,
the **tier algorithm is left alone** and only the basis wording changes. Four
opt-in live smoke tests in `backend/tests/test_live_smoke.py` pin each finding,
including one that fails the day Role Search starts serving the alternative
address — at which point the false-positive branch is live (every client of one
agent sharing a `pafId`) and `_tier()` does need revisiting. They need the
sandbox keys (`NZBN_SANDBOX_API_KEY`, `NZBN_ROLE_SEARCH_SANDBOX_API_KEY` — the
production keys 401 against `/sandbox/`) **exported into the environment**, since
the test tier runs with `OPENCHECK_DISABLE_DOTENV=1`:

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
