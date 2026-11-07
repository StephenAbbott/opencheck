---
title: The MCP ownership map's integrated_figures keeps every node's figures when two owners share a name.
readme: The MCP ownership map's figure list no longer drops one owner's figures when two records of different sources share a name.
tests: backend +1
---
Found by the MCP testing of 7 Nov 2026 (Notion "MCP testing ahead of Connected Data London 2026", issue 3): the map's JSON `integrated_figures` was a dict keyed by display name, so two nodes with one name — Rosneft's "Rosneftegaz JSC" from OpenSanctions ("at least 44.69%") and from GEM ("40.4%"), two records with no shared identifier — kept only the last, and the JSON disagreed with the image, whose sublabels are per node. Eesti Energia and Ørsted lost a source's figure the same way. `owners._figures_by_name` gathers every figure for a name (each already names its source; nothing is added, duplicates are dropped). Nodes are not merged by this — that remains an identifier decision (and, for states, the issue 4 fold). After deploy: `opencheck_ownership_map` for 253400JT3MQWNDKMJE44 lists both Rosneftegaz JSC figures.
