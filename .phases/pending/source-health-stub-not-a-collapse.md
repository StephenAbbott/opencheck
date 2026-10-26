---
title: The source-health sweep no longer counts a source that did not answer as a statement collapse.
readme: A rate-limited source in the weekly health sweep is reported as degraded rather than failing the run as a collapse of its BODS statements.
tests: backend +2
---
The 26 Oct 2026 sweep failed on KvK alone: a 429 that outlasted the adapter's retries returned a stub bundle, `statement_counts` mapped it to zeros, and the week-over-week diff read "entity 1 → 0" as a collapse — redding the run over a rate limit the same report already showed as a degradation, which by design does not fail the job. `statement_counts` now returns None for a stub bundle (`is_stub_bundle`, the pipeline's own rule), and the diff already skips a source with no counts on either side. A register that answers and stops carrying statements is still a collapse; a test pins both halves.
