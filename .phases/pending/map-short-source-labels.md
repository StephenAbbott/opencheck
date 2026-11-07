---
title: The MCP ownership map names each source by its short name on the drawing, and keeps the full name in its JSON.
readme: The MCP ownership map's drawing names sources by their short names, so figure lines and edge labels fit without being cut.
tests: backend +8
---
The post-deploy re-test of the six curated examples (7 Nov 2026, MCP testing ticket) found Eli Lilly's figure lines cut at "SEC EDGAR — U.S. Securities and Exchange Commissi…" by the 56-character sublabel clip, and Taqa Bratani's GLEIF + MEIP edge label wrapped to four lines by MEIP's 88-character description. `owners.short_source` gives the name the drawing uses: the part before " — " (SEC EDGAR, CVR, KRS, ЄДР…), or for a long name with no dash its bracketed acronym after any upper-case lead word (OECD-UNSD MEIP); names up to 45 characters, such as Estonian e-Business Register (e-Äriregister) and Global Energy Monitor / Climate TRACE, are unchanged. It is used for the figure lines under a node and the source names on a drawn edge label. The map's JSON (`edges[].source`, `integrated_figures`) keeps every source's full name.
