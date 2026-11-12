---
title: Slovenia's Business Register (AJPES PRS) joins as an index of its open CSV, keyed on the ten-character registration number GLEIF files under RA000529.
readme: Slovenian companies now have a register card from AJPES's Business Register open file, joined to GLEIF on the registration number.
tests: backend +59
---
The new `si_prs` source reads a SQLite index built from AJPES's `opsiprs.csv`, published on podatki.gov.si under CC BY 4.0. The file is UTF-16LE with 12 quoted columns and 295,249 units (1 Nov 2026 cut). It carries a name, a legal form, the registering authority (an AJPES branch office or a district court) and the registered address. It publishes no status, no dates, no officers and no owners. So the card says the unit is on the register, never that it trades: `finding_si_prs` says "no status published", the mapper adds a note saying so, and the knowability sentence says OpenCheck reads Slovenia for the register record only.

The join was measured against GLEIF on 12 Nov 2026. 3,075 of 3,077 ISSUED records under RA000529 are in the file. The two misses are SINTECO 1404164000 and a Groupama branch, 0110041071. None of the 302 INACTIVE GLEIF entities are in it, which suggests the file lists current units only. The number is ten characters: 93 units carry a letter in the eighth place (`\d{7}[0-9A-Z]\d{2}`). A number not in the index is a note card, not a degradation. GLEIF's statement and the register's corroborate on `SI-PRS`. There is no name gate.

podatki.gov.si refuses every non-browser client, from datacentres and from a home connection alike, with HTTP 407 from its proxy. Stephen decided (12 Nov 2026) on a monthly manual refresh. He saves the CSV from a browser and runs `backend/scripts/refresh_si_prs_index.sh <opsiprs.csv> <cut>`. That script runs the standard-library builder and replaces the `si-prs-index` release asset, which `warm_index` downloads at boot. Until the asset exists the source is not announced.

Legal persons and natural persons are told apart by form. Every form in the real file was measured and sorted into a natural-person list (sole traders, their branches, farm supplementary activities, room renters, registered doctors, lawyers, notaries…), a legal-person list (companies, associations, institutes, state bodies down to the President of the Republic) and three unit forms. A unit (Poslovna enota, Organizacijska enota, Članica univerze (del)) takes the reading of its main unit, `number[:7] + "000"`, and the mapper notes which main unit it belongs to. In the real file 29,461 business units belong to legal persons and 79 to natural persons; every unit's main unit is present. A form the lists do not know is read as a natural person and logged as NEW FORM; there were none.

A natural person's street, house number and HSEID are never stored, and the bundle schema refuses them. Name search covers legal persons only. The real build is 145,144 legal and 150,105 natural units: 77 MB, built in 5 seconds on a Mac.

After merge, Stephen runs the refresh script once with the 1 Nov 2026 file to create the release asset. Then check Krka (LEI 549300H9RLRTRTLRUZ73, number 5043611000) shows the card.
