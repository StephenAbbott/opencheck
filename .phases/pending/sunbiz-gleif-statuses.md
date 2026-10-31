---
title: The Florida index builder asks GLEIF only for registration statuses GLEIF accepts.
readme: The Florida index builder now asks GLEIF only for registration statuses it accepts, so the quarterly rebuild can publish its first index.
tests: backend +2
---
The first run of `refresh-sunbiz-florida-index.yml` (31 Oct 2026) reached the Division's SFTP server from a GitHub runner, which had been untested (cordata.zip, 1.79 GB, in 422 s; corevent.zip in 46 s). It then failed: `gleif_registered_as` filtered on `TRANSFERRED`, which GLEIF answers with HTTP 400 ("Invalid enum value"), as it does `CANCELLED`. The builder now asks for the eight values GLEIF's enum holds (`GLEIF_REGISTRATION_STATUSES`). It also refuses to build when the per-status totals do not add up to the unfiltered total, so a status GLEIF adds later cannot silently drop records. Read live on 31 Oct 2026: 12,891 records under RA000603, the eight statuses sum to it, and they give 12,871 document numbers and 5 FEIs. Check after merge: re-run the workflow by hand, and confirm the `sunbiz-florida-index` asset is published and a Florida LEI shows the card once Render has downloaded it.
