# Shell plc subsidiaries in Norway and the Netherlands

The OpenCheck subsidiaries result for Shell plc (LEI 21380068P1DRHMJ8KU70) has **no ownership percentages**. It lists GLEIF Level 2 accounting-consolidation relationships, not shareholdings, so the "% owned" column cannot be filled from this data. The result's own `measures` note says so: GLEIF Level 2 records entities that report this entity as their direct or ultimate consolidating parent, and that is not ownership.

What the result does give, for the 32 entities (30 Netherlands, 2 Norway):

- **Relation:** `both` = reported as a direct child and an ultimate child; `ultimate` = reported only as an ultimate (indirect) child.
- **LEI status:** the entity's LEI registration status from GLEIF (ISSUED, or LAPSED where the LEI has not been renewed).
- **Since:** the relationship start date as reported to GLEIF.

## Norway (2)

| Entity | LEI | Relation | LEI status | Since |
|---|---|---|---|---|
| A/S NORSKE SHELL | 213800F4ETX85XLF5K47 | both | ISSUED | 2021-05-11 |
| MOLGAS NORWAY AS | 213800TBZMU2HKQA6W02 | both | ISSUED | 2021-05-11 |

## Netherlands (30)

| Entity | LEI | Relation | LEI status | Since |
|---|---|---|---|---|
| B.V. DORDTSCHE PETROLEUM MAATSCHAPPIJ | 2138008FIL755M9UH213 | both | ISSUED | 2013-11-01 |
| ENERGIEPARK POTTENDIJK B.V. | 213800HML5VL7IIM9G57 | both | ISSUED | 2023-06-14 |
| NEDERLANDSE AARDOLIE MAATSCHAPPIJ B.V. | 2138004JQD1KA2GSC430 | both | ISSUED | 2018-04-30 |
| SHELL ASSET MANAGEMENT COMPANY B.V. | 21380062EZANCJTT2O28 | both | LAPSED | 2014-01-15 |
| SHELL BRAZIL HOLDING B.V. | 2138008DL1UU9QGACF51 | both | LAPSED | 2013-11-01 |
| SHELL CHEMICALS EUROPE B.V. | 2138001O1DFAN7FVH171 | both | ISSUED | 2013-12-17 |
| SHELL ENERGY EUROPE B.V. | 2138001BE7467RUBQS88 | both | ISSUED | 2018-09-21 |
| SHELL EXPLORATION AND PRODUCTION (84) B.V. | 213800X65BJZAEPM4W02 | both | ISSUED | 2023-11-01 |
| SHELL EXPLORATION AND PRODUCTION (LXI) B.V. | 213800WQ36VBEPHQOX52 | both | ISSUED | 2023-11-01 |
| SHELL FINANCE (NETHERLANDS) B.V. | 213800BKRTTJIF5R3Q22 | both | LAPSED | 2013-11-01 |
| SHELL GAS (LPG) HOLDINGS B.V. | 213800C2Y6KDQCD2WZ09 | both | LAPSED | 2013-11-01 |
| SHELL GLOBAL SOLUTIONS INTERNATIONAL B.V. | 2138001LEZWU9YYE5N65 | both | LAPSED | 2013-11-01 |
| SHELL INTERNATIONAL EXPLORATION AND PRODUCTION B.V. | 213800EUIXDMYNUJ4462 | both | ISSUED | 2013-11-01 |
| SHELL INTERNATIONAL FINANCE B.V. | 213800ITMMKU4Z7I4F78 | both | ISSUED | 2014-01-15 |
| SHELL NEDERLAND CHEMIE B.V. | 213800NZMULWDBYUAN72 | both | ISSUED | 2018-01-11 |
| SHELL NEDERLAND RAFFINADERIJ B.V. | 2138009FAB375EZDNU25 | both | ISSUED | 2013-12-17 |
| SHELL NEDERLAND VERKOOPMAATSCHAPPIJ B.V. | 213800TUUTV73BQKPL24 | both | ISSUED | 2013-12-17 |
| SHELL NEW ENERGIES NL B.V. | 213800E99C3JVZGDPV44 | both | ISSUED | 2023-01-05 |
| SHELL OLIE- OG GASUDVINDING DANMARK B.V. | 21380018Z33A3VAKQQ62 | both | LAPSED | 2013-11-01 |
| SHELL OVERSEAS INVESTMENTS B.V. | 213800J8M689LDFS4K06 | both | ISSUED | 2013-11-01 |
| SHELL PETROLEUM B.V. | 213800U43H56YKOIF596 | both | ISSUED | 2013-10-16 |
| SHELL PHILIPPINES EXPLORATION B.V. | 5493000I1ONBQLNMP873 | both | LAPSED | 2018-05-04 |
| SHELL SHARED SERVICES (ASIA) B.V. | 549300IRNZELANQIAW38 | both | ISSUED | 2018-05-04 |
| SHELL TRADING RUSSIA B.V. | 2138008WK9V1AC9LWU43 | both | LAPSED | 2017-10-18 |
| SHELL UPSTREAM INDONESIA SERVICES B.V. | 549300WW482N6G7Q3O71 | both | LAPSED | 2018-05-02 |
| SHELL VENTURES B.V. | 2138003779624KDFO823 | both | ISSUED | 2018-02-05 |
| EUROSHELL CARDS B.V. | 254900CEFSJXVQZS5W34 | ultimate | ISSUED | 1996-04-12 |
| NOORDZEEWIND C.V. | 213800Y17U9X5943J241 | ultimate | ISSUED | 2022-12-16 |
| SHELL Information Technology International B.V. | 25490086WQ90FWHQ8L98 | ultimate | ISSUED | 1993-12-30 |
| Shell EV Charging Solutions B.V. | 6488N221P8URN766AU62 | ultimate | ISSUED | 2017-10-11 |

## Caveats

- **Ownership %:** not in this result. Consolidation (GLEIF Level 2) is an accounting relationship and does not give a shareholding. Figures would need an owners or ownership-map lookup, which reads the ownership registers per source. Those are separate calls and were not run here.
- **Coverage:** only entities that hold an LEI and report Shell plc as parent appear. Norwegian or Dutch subsidiaries without an LEI, or that report no parent, are absent.
- **Lapsed LEIs:** 9 of the 32 entities have a LAPSED LEI registration, so their GLEIF record may be out of date.
- **Relation `ultimate`:** these four Dutch entities are reported as indirect children only. EUROSHELL CARDS B.V. and Shell EV Charging Solutions B.V. name a direct parent that is itself in the network (Shell Petroleum B.V. and Shell Overseas Investments B.V. respectively). NOORDZEEWIND C.V. and SHELL Information Technology International B.V. report no direct parent in the network.
- **Source and licence:** the snapshot is dated 2026-11-05 (source: mirror, snapshot fallback). LEI data is from GLEIF under CC0-1.0.
