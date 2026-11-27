---
title: Run a check from the daily changes page opens the ordinary lookup address.
readme: Running a check from the daily changes page or the watchlist now opens the ordinary `/?lei=` address instead of keeping the page's own parameters.
tests: vitest +3
---
"Run a check" on `/changes` left the address at `/changes?day=2026-11-26&lei=…`: `lookupLei` kept the current path and query and only added `lei`, which was right on the main view and wrong on every other one. `lookupBaseUrl` in `lib/views.ts` now resets the path to `/` and drops the query whenever a lookup starts from a view that is not the main one (the daily changes page, the watchlist, and the rest), as it already did for a saved report; on the main view `?mode=` and the other parameters are kept. After deploy: open /changes, pick a day, Run a check, and the address reads `/?lei=<LEI>`.
