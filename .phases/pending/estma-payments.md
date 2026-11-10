---
title: Canadian extractives' payments to governments, from ESTMA, on the Climate & ESG tab.
readme: The Climate & ESG tab shows payments to governments that Canadian extractive companies report under ESTMA, from Statistics Canada's weekly file, linked to LEIs through a hand-reviewed crosswalk.
tests: backend +28, vitest +11, vitest_files +2
---
New `estma` source (the "Check Canadian extractives open datasets" ticket). Reports filed under Canada's Extractive Sector Transparency Measures Act are read from Statistics Canada's keyless SDMX-CSV (114,623 rows, 1,141 entities, 828 reporting, cut 5 Nov 2026). `backend/scripts/build_estma_index.py` packs it into a 5.3 MB SQLite (1.2 MB gzipped) shipped as the `estma-index` release asset, and `refresh-estma-index.yml` rebuilds it on Tuesdays. The builder refuses a changed header, a non-CAD unit or a malformed ESTMA ID. SEDAR+ was assessed and ruled out by its terms of use.

The ESTMA file carries no LEI or business number. The LEI comes from the committed crosswalk `backend/opencheck/data/estma_lei_crosswalk.csv`: 704 accepted rows, matched by name and reviewed by hand, with five confirmed by Stephen (McEwen, K+S Windsor Salt, Golden Arrow/GRG Resources, Archer Exploration, Minto). The card grades the link as a medium match. The hit asserts only the ESTMA entity ID (`ca_estma_id`, BODS scheme `CA-ESTMA`), and the LEI is an identifying annotation.

Each payment is filed once by payee and again by project. A report's total is therefore OpenCheck's sum of its payee lines, and the project lines are shown as the same payments and never added. Two reports for one year are never added together. An entity whose payments are filed in another entity's report is said so: this is a reporting arrangement under the Act, not ownership, and no relationship is drawn. When a company also has EITI data, one note says the two disclosures are not added or compared.

The licence is the Statistics Canada Open Licence. `SourceInfo.country` is left unset, as for every ESG source, so knowability does not count ESTMA as a register. After merge, run the workflow once by hand: until the asset exists, the source is never announced. Check Suncor (894500XO86FKXQGPYV71) in production. Its 2025 report should read CAD 4.6bn across 18 payees and 10 projects, and the report also covers 8 other entities.
