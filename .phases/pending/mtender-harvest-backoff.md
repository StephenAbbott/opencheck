---
title: The MTender harvest now waits out a spell of 502s from MTender's API instead of ending the run, so a backfill is not cut short by a minute-long outage.
readme: The MTender harvest now waits out short outages of MTender's API instead of stopping.
tests: backend +1
---
On 13 Nov 2026 the first backfill run after the harvest merged read 50 pages (5,000 procedures, 3,296 with awards) and then stopped at the 10 December 2025 cursor when the listing answered 502 three times in six seconds; the next two runs failed on their first page for the same reason, which fails the job before the index is rebuilt. Nothing was lost (the cursor only moves past a page read whole, and a failed run neither replaces the store nor the index), but a run gave up within seconds of an outage that cleared within minutes. `_get` now backs off 5, 15, 30, 60 and 120 seconds (`BACKOFF_S`, six attempts, about four minutes) on a 429, 5xx or network error, for the listing and for each package. Keep dispatching `refresh-mtender-index` by hand until its summary says the harvest is caught up.
