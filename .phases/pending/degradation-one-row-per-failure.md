---
title: A source that fails the same way twice in one lookup is listed once, whatever the cause each time.
readme: A source read that fails twice in one lookup, once by error and once by timeout, is now listed as one incomplete source rather than two.
tests: backend +1
---
Found by the Canada post-deploy check (10 Nov 2026, during ISED's gateway outage). A lookup for TRANSCONTINENTAL INC. listed "The ISED API did not return the corporation's directors" twice in `degraded_sources`: once with reason `upstream_error` and once with `timeout`. A failed directors read is never cached, so the read happened twice in the run, once at dispatch and again in the deepen pass, and the two attempts failed in different ways. `degradation.record` already collapsed identical rows, but its match included the reason. It now matches on source, check and detail, and keeps the first observation. The detail text is count-free by rule, so a second row with the same sentence added nothing for the reader.
