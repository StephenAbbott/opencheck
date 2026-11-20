---
title: "OpenCheck’s watchlist as perpetual KYC: from “what changed?” to “what did you decide?”"
subtitle: "Spotting that a company’s record changed is the easy part. Saying whether it matters, and keeping a record of what you did about it, is…"
published: "2026-10-19T13:33:04.183Z"
medium_url: "https://medium.com/@stephenabbottpugh/openchecks-watchlist-as-perpetual-kyc-from-what-changed-to-what-did-you-decide-d4052ce42668"
medium_id: "d4052ce42668"
topics:
  - "Know Your Customer"
  - "Open Standards"
  - "Open Data"
  - "Beneficial Ownership"
image: "/blog/media/openchecks-watchlist-as-perpetual-kyc-from-what-changed-to-what-did-you-decide/01.webp"
image_alt: ""
---

*Spotting that a company’s record changed is the easy part. Saying whether it matters, and keeping a record of what you did about it, is what turns monitoring into perpetual KYC.*

In September I wrote about [watchlist](https://opencheck.world/features#watchlist), OpenCheck’s way to [spot when a company’s records change without watching the registers](/blog/watchlist-spot-when-a-companys-record-change-without-watching-the-registers). The design rested on one idea: don’t poll. Rather than re-check every watched company on a schedule, [OpenCheck](https://opencheck.world/) waits for a source to publish a delta naming a company, re-runs the check and compares the result with the last one.

However reading Moody’s post on [the KYC continuum and perpetual KYC](https://www.moodys.com/web/en/us/kyc/resources/insights/the-kyc-continuum-part-2-ongoing-monitoring-or-perpetual-kyc-pkyc.html) made me realise that answered watchlist could be made more useful if it could answer more than one of three questions posed in the piece.

### Three questions, not one

Moody’s frames ongoing monitoring around three questions: what has changed? Does the change matter? What action is proportionate?

OpenCheck answered the first well. [Watchlist](https://opencheck.world/features#watchlist) is risk-driven rather than calendar-driven, the post’s central principle and its cost tracks how much of the world changed, not how long your list is.

The rest was thinner. Every entry in the feed carried equal weight, so a register that was briefly unreachable sat beside a new sanctions listing. There was nowhere to record why a company was watched or what you expected to see. Nothing prompted a periodic review; a list left unopened for 90 days was simply deleted. Entries logged what changed, but nothing recorded what anyone decided.

The post also puts change in ownership and control first among review triggers. Yet watchlist saw ownership only through GLEIF’s accounting consolidation data. A filing to the UK’s people with significant control (PSC) register reached OpenCheck only as a side effect of something else changing.

So I split the gaps into five tickets, which have shipped over the past few days.

### Deltas do the work

Nothing polls. Every trigger is a change a source has published:

- **GLEIF**’s Golden Copy delta, filtered so renewal churn never queues a re-run
- **OpenSanctions**’ entity-level delta, with each release
- **Estonia’s e-Business Register** open data, diffed day on day
- and now the **Companies House PSC stream**, which already feeds OpenCheck’s UK ownership graph

The PSC stream carries around 15,000 to 17,000 filings a day across 5.5 million UK companies. Only about 4% of them hold an LEI, so a watched UK company can expect roughly one PSC-triggered re-run a year. That is what delta-driven monitoring looks like: the work follows the change.

![What’s new in Watchlist](/blog/media/openchecks-watchlist-as-perpetual-kyc-from-what-changed-to-what-did-you-decide/02.webp "What’s new in Watchlist")

**Does it matter?** Every entry now carries one of three tiers: *Note only*, *Recheck the source* or *Escalate*. They come from a closed, published table rather than a score. A dissolution, a move of jurisdiction, a new sanctions listing or a new ultimate parent escalates. A source that could not be reached is a note, because absence of evidence is not a finding. The tier is set on the day and kept.

**What did you expect?** A watch can hold its onboarding context: the jurisdiction and ultimate parent you expect, a one-line rationale and the saved report that was your onboarding record. An entry can then say “GLEIF ultimate parent: X → Y — your watch expected X”.

**When did OpenCheck last look?** Each watch has a review line saying when every source was last asked and which answered from an older copy; it never claims a company is unchanged, and flags a review after 90 days. Stale lists are now warned in their feed 30 days before deletion.

**What did you decide?** Any entry can be marked *Reviewed, no action*, *Re-checked* or *Escalated*, with a note and, if you choose, a saved report pinned as evidence with its SHA-256 hash. Decisions are append-only, and the whole list downloads as a review record.

**Did ownership change?** Beyond the PSC tier, every re-run now compares the ownership and control statements before and after. A shareholding or voting-rights band of 25% or more being added or ended escalates.

![](/blog/media/openchecks-watchlist-as-perpetual-kyc-from-what-changed-to-what-did-you-decide/03.webp)

### Why BODS made this quick

That last change is where use of the [Beneficial Ownership Data Standard](https://standard.openownership.org/) (BODS) pays off. Every [OpenCheck source](https://opencheck.world/sources), from Companies House to Estonia to OpenSanctions, is mapped into BODS v0.4. Each relationship carries a stable `recordId`, a `recordStatus` and dated interests, with shares as ranges rather than guesses.

So comparing ownership was written once, against the standard, rather than once per register. Every source that publishes shareholdings gained ownership-change detection on the same day.

### What OpenCheck won’t do

The third question, what action is proportionate, is left to the user. That is a position, not a gap. OpenCheck records what changed, how much it matters by a published rule, what you expected and what you decided.

Adverse media and transaction monitoring are out of scope; [OpenAleph](https://openaleph.org/) archive matches are the nearest open equivalent to adverse media.

[Try the watchlist on OpenCheck](https://opencheck.world/features#watchlist).

*For four years, I was the product owner of the* [*Beneficial Ownership Data Standard*](https://standard.openownership.org/)*, the world’s leading open standard for beneficial ownership information and continue to advocate to improve beneficial ownership data quality and standardisation. To learn more or if you need technical guidance or consultancy support, contact me via* [*beneficialownership.co.uk*](https://www.beneficialownership.co.uk/)*.*
