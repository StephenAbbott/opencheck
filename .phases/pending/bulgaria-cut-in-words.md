---
title: Bulgaria's data-as-of line writes the cut in words.
readme: The Bulgaria card's data-as-of line now gives the cut as a date in words, like the date beside it.
tests: backend +1
---
The `bg_tr` snapshot detail read "Commercial Register open data, entries 1 January 2021 to 2026-11-17", mixing the house date form with the ISO cut (found by the post-deploy tester on UBB, 19 Nov 2026). `declare_snapshot` now formats the cut with `findings.human_date`, so the line reads "… 1 January 2021 to 17 November 2026" on the card and in `/lookup`'s `source_liveness.bg_tr.detail`. An index with no cut still reads "Commercial Register open data". The source's clock (`source_as_of`) is unchanged.
