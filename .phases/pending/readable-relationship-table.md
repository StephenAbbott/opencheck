---
title: The report's relationship table says its dates and interest types in words.
readme: The relationship tables in the PDF and Markdown reports now show dates as "29 Dec 2022" and interest types in words.
tests: backend +2
---
A follow-up to the readable-report fix, from its post-deploy check on EIDE SEAFOOD AS (23 Nov 2026): the text table under each source's diagram (`reporting/diagram.py`, `_row_interest`) still printed "from 2022-12-29" and fell back to the raw BODS interest type when an interest had no `details`. Its start and end dates now go through `ui_date`, which also learns to say a year-month as "Dec 2022" (a bare year stays as it is), and an interest with no `details` is worded from its type ("Voting rights"). The diagram's edge labels were already in words. To check after deploy: EIDE SEAFOOD AS's Markdown export reads "GLEIF Level 2 ultimate-parent (accounting consolidation), from 29 Dec 2022".
