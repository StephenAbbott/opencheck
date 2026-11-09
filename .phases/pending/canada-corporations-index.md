---
title: Corporations Canada answers from a daily index of the Federal Corporations open data first, and the ISED API second.
readme: Canada's federal register is now read from a daily index of the Federal Corporations open data, with the ISED API adding dated activities and directors, so a Canadian lookup works without an API key, covers dissolved corporations and supports name search.
tests: backend +33
---
The Canada index-tier ticket. `scripts/build_corporations_canada_index.py` builds a SQLite index (1,578,206 corporations on the 8 Nov 2026 cut, ~366 MB, ~35 s) from open.canada.ca's four English Federal Corporations CSVs, active and inactive or dissolved, CBCA and other federal acts. The CSVs are keyless and published daily. `refresh-corporations-canada-index.yml` rebuilds it every day at 12:23 UTC into the `corporations-canada-index` release asset, and the backend downloads it at boot (`warm_index`). The builder refuses a changed header, a short file, more than 200 malformed rows, or files more than a day apart.

The adapter reads the index first. It still calls the API for dated activities: the record is cached for a day, and directors, which need a gateway key, for seven days. Without a key the record comes from the keyless legacy endpoint, which has no directors. If the API fails beside an index record, that is a `source_read` degradation. If neither the index nor the API answers, that is a `source_fetch` degradation. A failure is never a stub card or a cached absence. A number on neither is a note card.

Stephen's decisions (9 Nov 2026):
- Index the whole register.
- Keep the API for every hit.
- Use the legacy endpoint for the record only.
- Refresh daily.
- Use the 1-day and 7-day cache ages.
- A Business Number filed under RA000072 resolves to the one active corporation with it, or is refused.
- No name gate.

Three corrections came with this work:
- GLEIF's `035818-5` is corporation 358185. The number includes its check digit and has no leading zero, and the old normaliser kept the zero, so it missed.
- `register_links` demanded eight digits, but 4–8 are valid.
- The status is the register's own detail ("Dissolved for non-compliance", "Dissolution Pending (non-compliance)").

The search adapter serves FTS5 name search over the index, including French names. Amalgamation partners are not mapped yet. The index sits on the data root, not the persistent disk.

After merging, run the workflow once by hand to confirm a GitHub runner reaches CloudFront, then look up a Canadian LEI.
