---
title: A Business Number filed for a Canadian federal LEI now resolves through the index to its corporation.
readme: A Canadian LEI filed with a Business Number now reaches the federal corporation the Business Number names, and the GLEIF card no longer shows that Business Number as a corporation number.
tests: backend +3
---
Found by the post-deploy check of the Canada index tier (9 Nov 2026). The lookup's deriver turns GLEIF's `103795266RC0001` into the key `BN103795266`, and the adapter's `fetch` normalises its `hit_id` a second time. That second pass dropped the `BN` prefix, so production read the Business Number as corporation number 103795266. The index resolution never ran. The card showed "CA-CC 103795266" because the legacy API happens to accept a Business Number and returned Mitutoyo Canada Inc., which is corporation 528757. `normalise_corp_id` now reads the `BN<9>` key as itself. The adapter tests now include one that passes the deriver's key rather than GLEIF's filing.

The GLEIF hit also copied the derived `BN<9>` key into its identifiers as `ca_corp_id`. GLEIF publishes no number in that spelling, so `_build_gleif_hit` now leaves it out. The Corporations Canada hit asserts the corporation the Business Number resolves to.
