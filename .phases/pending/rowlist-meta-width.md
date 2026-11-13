---
title: A source card's row list no longer crushes its titles when the row's details are long.
readme: Contract and notice lists on source cards keep their titles readable on a wide screen.
---
Post-deploy check of ΚΗΜΔΗΣ (13 Nov 2026): on a desktop-width page the ΚΗΜΔΗΣ contracts list drew each title one word per line with the detail line over it and past the card's edge, and Qantas's AusTender card did the same. `ui/RowList` kept the meta column at its natural width (`sm:shrink-0`), which is fine for TED's short meta and wider than the card for a contract row (authority, value, dates, procedure, id). The meta is now capped at 55% of the row and wraps inside it, and the title takes the rest; the phone layout (stacked) is unchanged. Every caller of `RowList` gets the fix: AusTender, ΚΗΜΔΗΣ, MTender, Compras.gov.br, TED, Find a Tender and the rest.
