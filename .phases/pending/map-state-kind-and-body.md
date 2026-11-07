---
title: A folded state is typed "state" on the MCP ownership map's edge rows, and each owners figure names the body its own source filed.
readme: The MCP owners answer now names, for each source's figure, the state body that source filed, and the map types a folded state as a state.
tests: backend +2
---
The post-deploy re-test of the six curated examples (7 Nov 2026, MCP testing ticket) found two disagreements between `opencheck_owners` and `opencheck_ownership_map` on a folded state. The map's `edges[].owner_kind` said "entity" for "Norway (state)" where the owners row says `kind: "state"`; it now says "state". On Eesti Energia, the owners figures gave e-Äriregister's 100% "as Government of Estonia" while the map's edge, read from the filing, said Rahandusministeerium: e-Äriregister's and OpenSanctions' records share an identifier and are one party, and the party's single display name was used for every figure. `owners.filed_bodies` now gives, per source, the body that source filed the holding under (a current filing before an ended one), and `fold_states` uses it for each figure's `as` and the row's `bodies`. Figures, folding and the summary's arithmetic are unchanged.
