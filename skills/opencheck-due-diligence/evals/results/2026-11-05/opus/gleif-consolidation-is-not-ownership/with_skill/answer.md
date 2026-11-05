# Entities Shell plc consolidates in Norway and the Netherlands

**I can't give you ownership percentages from this result, because the data doesn't contain any.** `opencheck_subsidiaries` reads GLEIF Level 2 data. That data records **accounting consolidation**: an entity says that Shell plc (LEI `21380068P1DRHMJ8KU70`) is its direct or ultimate *consolidating* parent. GLEIF publishes no shareholding percentages. Also, a consolidating parent doesn't have to own 100%, or hold any shares directly at all. So the table below says which entities Shell plc **consolidates**. It does not say how much Shell owns of each.

An example of why this matters: Nederlandse Aardolie Maatschappij (NAM) is publicly known to be a 50/50 joint venture between Shell and ExxonMobil, and it still shows up here as consolidated by Shell plc. Putting "100%" (or any number) against these rows would be making the number up.

**Snapshot:** GLEIF Level 2, read from OpenCheck's mirror snapshot dated 2026-11-05 (`snapshot_fallback: true`). The result was not truncated (154 entities fetched, 105 direct and 152 ultimate in total) and reported no degraded sources.

**What "Relation" means:** *Direct + ultimate* means the entity names Shell plc as both its direct and its ultimate consolidating parent. *Ultimate only* means Shell plc is the ultimate consolidating parent and the direct one is some other entity, named where GLEIF gives it.

## Norway (2)

| Entity | LEI | Relation to Shell plc | Since | LEI status | Shell ownership % |
|---|---|---|---|---|---|
| A/S Norske Shell | 213800F4ETX85XLF5K47 | Direct + ultimate | 2021-05-11 | Issued | not published by GLEIF |
| Molgas Norway AS | 213800TBZMU2HKQA6W02 | Direct + ultimate | 2021-05-11 | Issued | not published by GLEIF |

## Netherlands (30)

| Entity | LEI | Relation to Shell plc | Since | LEI status | Shell ownership % |
|---|---|---|---|---|---|
| B.V. Dordtsche Petroleum Maatschappij | 2138008FIL755M9UH213 | Direct + ultimate | 2013-11-01 | Issued | not published |
| Energiepark Pottendijk B.V. | 213800HML5VL7IIM9G57 | Direct + ultimate | 2023-06-14 | Issued | not published |
| Nederlandse Aardolie Maatschappij B.V. | 2138004JQD1KA2GSC430 | Direct + ultimate | 2018-04-30 | Issued | not published (publicly reported as a 50/50 JV with ExxonMobil) |
| Shell Asset Management Company B.V. | 21380062EZANCJTT2O28 | Direct + ultimate | 2014-01-15 | **Lapsed** (2026-01-15) | not published |
| Shell Brazil Holding B.V. | 2138008DL1UU9QGACF51 | Direct + ultimate | 2013-11-01 | **Lapsed** (2024-10-18) | not published |
| Shell Chemicals Europe B.V. | 2138001O1DFAN7FVH171 | Direct + ultimate | 2013-12-17 | Issued | not published |
| Shell Energy Europe B.V. | 2138001BE7467RUBQS88 | Direct + ultimate | 2018-09-21 | Issued | not published |
| Shell Exploration and Production (84) B.V. | 213800X65BJZAEPM4W02 | Direct + ultimate | 2023-11-01 | Issued | not published |
| Shell Exploration and Production (LXI) B.V. | 213800WQ36VBEPHQOX52 | Direct + ultimate | 2023-11-01 | Issued | not published |
| Shell Finance (Netherlands) B.V. | 213800BKRTTJIF5R3Q22 | Direct + ultimate | 2013-11-01 | **Lapsed** (2024-10-18) | not published |
| Shell Gas (LPG) Holdings B.V. | 213800C2Y6KDQCD2WZ09 | Direct + ultimate | 2013-11-01 | **Lapsed** (2019-10-18) | not published |
| Shell Global Solutions International B.V. | 2138001LEZWU9YYE5N65 | Direct + ultimate | 2013-11-01 | **Lapsed** (2023-10-18) | not published |
| Shell International Exploration and Production B.V. | 213800EUIXDMYNUJ4462 | Direct + ultimate | 2013-11-01 | Issued | not published |
| Shell International Finance B.V. | 213800ITMMKU4Z7I4F78 | Direct + ultimate | 2014-01-15 | Issued | not published |
| Shell Nederland Chemie B.V. | 213800NZMULWDBYUAN72 | Direct + ultimate | 2018-01-11 | Issued | not published |
| Shell Nederland Raffinaderij B.V. | 2138009FAB375EZDNU25 | Direct + ultimate | 2013-12-17 | Issued | not published |
| Shell Nederland Verkoopmaatschappij B.V. | 213800TUUTV73BQKPL24 | Direct + ultimate | 2013-12-17 | Issued | not published |
| Shell New Energies NL B.V. | 213800E99C3JVZGDPV44 | Direct + ultimate | 2023-01-05 | Issued | not published |
| Shell Olie- og Gasudvinding Danmark B.V. | 21380018Z33A3VAKQQ62 | Direct + ultimate | 2013-11-01 | **Lapsed** (2019-11-03) | not published |
| Shell Overseas Investments B.V. | 213800J8M689LDFS4K06 | Direct + ultimate | 2013-11-01 | Issued | not published |
| Shell Petroleum B.V. | 213800U43H56YKOIF596 | Direct + ultimate | 2013-10-16 | Issued | not published |
| Shell Philippines Exploration B.V. | 5493000I1ONBQLNMP873 | Direct + ultimate | 2018-05-04 | **Lapsed** (2023-05-26) | not published |
| Shell Shared Services (Asia) B.V. | 549300IRNZELANQIAW38 | Direct + ultimate | 2018-05-04 | Issued | not published |
| Shell Trading Russia B.V. | 2138008WK9V1AC9LWU43 | Direct + ultimate | 2017-10-18 | **Lapsed** (2024-10-18) | not published |
| Shell Upstream Indonesia Services B.V. | 549300WW482N6G7Q3O71 | Direct + ultimate | 2018-05-02 | **Lapsed** (2022-04-15) | not published |
| Shell Ventures B.V. | 2138003779624KDFO823 | Direct + ultimate | 2018-02-05 | Issued | not published |
| Euroshell Cards B.V. | 254900CEFSJXVQZS5W34 | Ultimate only (direct: Shell Petroleum B.V.) | 1996-04-12 | Issued | not published |
| Shell EV Charging Solutions B.V. | 6488N221P8URN766AU62 | Ultimate only (direct: Shell Overseas Investments B.V.) | 2017-10-11 | Issued | not published |
| Noordzeewind C.V. | 213800Y17U9X5943J241 | Ultimate only (direct parent not given) | 2022-12-16 | Issued | not published |
| SHELL Information Technology International B.V. | 25490086WQ90FWHQ8L98 | Ultimate only (direct parent not given) | 1993-12-30 | Issued | not published |

## Reading this carefully

- **The list isn't complete.** GLEIF only covers entities that hold an LEI *and* report Shell plc as a parent. Subsidiaries with no LEI, or with no parent reported, don't appear. Shell's full list of related undertakings in its annual report will be longer.
- **A lapsed LEI doesn't tell you the company's status.** The rows marked *Lapsed* mean the LEI record wasn't renewed. GLEIF still shows these entities as "ACTIVE", but that is only the last status anyone declared, so I can't confirm they still exist or are still Shell's. The Dutch KvK or the Norwegian Brønnøysund register would settle that.
- **Where to get real percentages:**
  - Shell plc's annual report lists related undertakings with the % held.
  - For Norway, the Brønnøysund shareholder register (aksjonærregisteret) and the beneficial-ownership register.
  - For the Netherlands, KvK filings; the Dutch UBO register is not publicly accessible.
  - `opencheck_owners` or `opencheck_lookup` on an individual entity's LEI returns any filed shareholdings OpenCheck can read. I didn't run those here.
- **Licence:** contains LEI data from GLEIF, available under CC0 1.0.
