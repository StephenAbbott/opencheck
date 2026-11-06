# Shell plc subsidiaries in Norway and the Netherlands

**The OpenCheck result has no ownership percentages, so the "% owned" column cannot be filled from it.** The subsidiaries result is GLEIF Level 2 accounting consolidation: entities that hold an LEI and report Shell plc as their direct or ultimate consolidating parent. The result's own `measures` field says so, and it is not an ownership register. No row in the file carries a share, stake or percentage, and `owners` and `ownership_map` were not part of this result.

Source: `opencheck_subsidiaries` for Shell plc (LEI `21380068P1DRHMJ8KU70`), recorded 2026-11-05T13:32:52Z. It is a mirror snapshot dated 2026-11-05 (`snapshot_fallback: true`), not a live read. Licence: GLEIF data, CC0-1.0. Attribution: "Contains LEI data from GLEIF, available under CC0 1.0."

## Norway (NO): 2 entities

| Name | LEI | GLEIF relation | Direct parent in GLEIF | LEI status | Shell % owned |
|---|---|---|---|---|---|
| A/S NORSKE SHELL | 213800F4ETX85XLF5K47 | both | not given | Issued | Not in source |
| MOLGAS NORWAY AS | 213800TBZMU2HKQA6W02 | both | not given | Issued | Not in source |

## Netherlands (NL): 30 entities

| Name | LEI | GLEIF relation | Direct parent in GLEIF | LEI status | Shell % owned |
|---|---|---|---|---|---|
| B.V. DORDTSCHE PETROLEUM MAATSCHAPPIJ | 2138008FIL755M9UH213 | both | not given | Issued | Not in source |
| ENERGIEPARK POTTENDIJK B.V. | 213800HML5VL7IIM9G57 | both | not given | Issued | Not in source |
| NEDERLANDSE AARDOLIE MAATSCHAPPIJ B.V. | 2138004JQD1KA2GSC430 | both | not given | Issued | Not in source |
| SHELL ASSET MANAGEMENT COMPANY B.V. | 21380062EZANCJTT2O28 | both | not given | Lapsed | Not in source |
| SHELL BRAZIL HOLDING B.V. | 2138008DL1UU9QGACF51 | both | not given | Lapsed | Not in source |
| SHELL CHEMICALS EUROPE B.V. | 2138001O1DFAN7FVH171 | both | not given | Issued | Not in source |
| SHELL ENERGY EUROPE B.V. | 2138001BE7467RUBQS88 | both | not given | Issued | Not in source |
| SHELL EXPLORATION AND PRODUCTION (84) B.V. | 213800X65BJZAEPM4W02 | both | not given | Issued | Not in source |
| SHELL EXPLORATION AND PRODUCTION (LXI) B.V. | 213800WQ36VBEPHQOX52 | both | not given | Issued | Not in source |
| SHELL FINANCE (NETHERLANDS) B.V. | 213800BKRTTJIF5R3Q22 | both | not given | Lapsed | Not in source |
| SHELL GAS (LPG) HOLDINGS B.V. | 213800C2Y6KDQCD2WZ09 | both | not given | Lapsed | Not in source |
| SHELL GLOBAL SOLUTIONS INTERNATIONAL B.V. | 2138001LEZWU9YYE5N65 | both | not given | Lapsed | Not in source |
| SHELL INTERNATIONAL EXPLORATION AND PRODUCTION B.V. | 213800EUIXDMYNUJ4462 | both | not given | Issued | Not in source |
| SHELL INTERNATIONAL FINANCE B.V. | 213800ITMMKU4Z7I4F78 | both | not given | Issued | Not in source |
| SHELL NEDERLAND CHEMIE B.V. | 213800NZMULWDBYUAN72 | both | not given | Issued | Not in source |
| SHELL NEDERLAND RAFFINADERIJ B.V. | 2138009FAB375EZDNU25 | both | not given | Issued | Not in source |
| SHELL NEDERLAND VERKOOPMAATSCHAPPIJ B.V. | 213800TUUTV73BQKPL24 | both | not given | Issued | Not in source |
| SHELL NEW ENERGIES NL B.V. | 213800E99C3JVZGDPV44 | both | not given | Issued | Not in source |
| SHELL OLIE- OG GASUDVINDING DANMARK B.V. | 21380018Z33A3VAKQQ62 | both | not given | Lapsed | Not in source |
| SHELL OVERSEAS INVESTMENTS B.V. | 213800J8M689LDFS4K06 | both | not given | Issued | Not in source |
| SHELL PETROLEUM B.V. | 213800U43H56YKOIF596 | both | not given | Issued | Not in source |
| SHELL PHILIPPINES EXPLORATION B.V. | 5493000I1ONBQLNMP873 | both | not given | Lapsed | Not in source |
| SHELL SHARED SERVICES (ASIA) B.V. | 549300IRNZELANQIAW38 | both | not given | Issued | Not in source |
| SHELL TRADING RUSSIA B.V. | 2138008WK9V1AC9LWU43 | both | not given | Lapsed | Not in source |
| SHELL UPSTREAM INDONESIA SERVICES B.V. | 549300WW482N6G7Q3O71 | both | not given | Lapsed | Not in source |
| SHELL VENTURES B.V. | 2138003779624KDFO823 | both | not given | Issued | Not in source |
| EUROSHELL CARDS B.V. | 254900CEFSJXVQZS5W34 | ultimate | 213800U43H56YKOIF596 (Shell Petroleum B.V.) | Issued | Not in source |
| NOORDZEEWIND C.V. | 213800Y17U9X5943J241 | ultimate | not given | Issued | Not in source |
| SHELL Information Technology International B.V. | 25490086WQ90FWHQ8L98 | ultimate | not given | Issued | Not in source |
| Shell EV Charging Solutions B.V. | 6488N221P8URN766AU62 | ultimate | 213800J8M689LDFS4K06 (Shell Overseas Investments B.V.) | Issued | Not in source |

## Notes

- **Relation labels.** The file does not define `both`, `direct` or `ultimate`. Per the `measures` field, each row is an entity reporting Shell plc as a direct or ultimate consolidating parent. "Direct parent in GLEIF" is the parent LEI the record names, if any.
- **LEI status.** "Lapsed" is the LEI registration status, not the company's status; every entity is listed as ACTIVE. Nine of the 30 Dutch entities have lapsed LEIs, so their GLEIF data may be out of date.
- **Not in this result.** Ownership percentages, voting or control shares, and ownership paths. Consolidation does not give any of these, so a percentage for any row above would need a different source.
- **Completeness.** The file lists 154 entities in total, and `truncated` is false. All 32 Norwegian and Dutch rows are shown above (2 NO, 30 NL), matching the `jurisdictions` counts.

## Next steps to get percentages

- An ownership lookup on Shell plc or on the individual entities (`opencheck_owners` or `opencheck_ownership_map`), which uses the shared lookup budget.
- The national registers: the Dutch Chamber of Commerce (KvK) for the B.V.s and the Brønnøysund Register Centre for the two Norwegian entities. Their published shareholder information varies by register, so check what each one publishes before relying on it.
