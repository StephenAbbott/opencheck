---
title: The source card's beneficial ownership note gives the register's acronym after the heading names it in full, and Ireland's row links the CRBOT and the CFV.
readme: The beneficial ownership note on a source card no longer repeats the register's full name, and Ireland's entry links its trusts and financial-vehicle registers.
tests: vitest +1
---
After the Ireland deploy the Diageo CRO card named the Central Register of Beneficial Ownership of Companies and Industrial and Provident Societies (RBO) twice, once in the heading and again in the sentence. The sentence now uses the acronym when the register's name ends with one in brackets (`registerShortName` in `lib/boAccess.ts`), and says "most companies in <country>" when the register covers companies but not trusts (`register_covers` on `BoAccessNotice`, from the jurisdictions table), so Ireland reads "Beneficial ownership information for most companies in Ireland is kept in the RBO, not in this source." (Stephen's wording, 15 Nov 2026). The Ireland row is re-synced from Notion with the CRBOT and CFV home and access pages in its notes and sources. Check after deploy: DIAGEO IRELAND UNLIMITED COMPANY (213800Z3C9XEVWY1CT65).
