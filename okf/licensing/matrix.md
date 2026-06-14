---
type: "Reference"
title: "Licensing compatibility matrix"
description: "Per-source licence terms (commercial use, attribution, share-alike) for combining OpenCheck data in exports. Most-restrictive licence wins."
resource: "/license-matrix"
tags: ["licensing", "export", "compliance"]
timestamp: "2026-06-14"
---

# Source licence matrix

Generated from the live registry. The OpenCheck `/license-matrix` API endpoint and the `LICENSES.md` in every export bundle carry the same data.

| Source | Licence | Commercial | Attribution | Share-alike |
|---|---|---|---|---|
| Australian Business Register (ABN Lookup) (`abr_australia`) | `CC-BY-3.0-AU` | yes | yes | no |
| ARES (Czechia) (`ares`) | `CC-BY-4.0` | yes | yes | no |
| Estonian e-Business Register (e-Äriregister) (`ariregister`) | `CC-BY-4.0` | yes | yes | no |
| Belgian Crossroads Bank for Enterprises (BCE/KBO) (`bce_belgium`) | `Custom-KBO-Reuse` | conditional | yes | no |
| Bolagsverket — Swedish Companies Registration Office (`bolagsverket`) | `SE-PSI` | yes | yes | no |
| Brønnøysundregistrene — Norwegian Register Centre (`brreg`) | `NLOD-2.0` | yes | yes | no |
| Global Energy Monitor / Climate TRACE (`climatetrace`) | `CC-BY-4.0` | yes | yes | no |
| UK Companies House (`companies_house`) | `OGL-3.0` | yes | yes | no |
| Corporations Canada — ISED federal register (`corporations_canada`) | `OGL-Canada-2.0` | yes | yes | no |
| CRO — Companies Registration Office Ireland (`cro`) | `CC-BY-4.0` | yes | yes | no |
| CVR — Det Centrale Virksomhedsregister (`cvr_denmark`) | `Danish Open Government Data (CVR brugervilkår)` | yes | yes | no |
| EveryPolitician (`everypolitician`) | `CC-BY-NC-4.0` | no | yes | no |
| Firmenbuch — Austrian Commercial Register (`firmenbuch`) | `CC-BY-4.0` | yes | yes | no |
| GLEIF (`gleif`) | `CC0-1.0` | yes | no | no |
| INPI — Registre National des Entreprises (`inpi`) | `Licence Ouverte / Open Licence 2.0` | yes | yes | no |
| JAR — Lithuanian Register of Legal Entities (`jar_lithuania`) | `CC-BY-4.0` | yes | yes | no |
| KRS — Polish National Court Register (`krs_poland`) | `PL-OGD` | yes | yes | no |
| KvK — Netherlands Chamber of Commerce (`kvk`) | `CC-BY-4.0` | yes | yes | no |
| OpenAleph (`openaleph`) | `per-collection` | conditional | yes | no |
| OpenCorporates (`opencorporates`) | `OC-Terms` | conditional | yes | yes |
| OpenSanctions (`opensanctions`) | `CC-BY-NC-4.0` | no | yes | no |
| PRH — Finnish Patent and Registration Office (`prh`) | `CC-BY-4.0` | yes | yes | no |
| RPO Slovakia (`rpo_slovakia`) | `CC-BY-4.0` | yes | yes | no |
| RPVS Slovakia (`rpvs_slovakia`) | `CC-BY-4.0` | yes | yes | no |
| SEC EDGAR (Schedule 13D/13G) (`sec_edgar`) | `Public Domain` | yes | no | no |
| Sudski registar — Croatian Court Register (`sudreg_croatia`) | `HR-OpenData` | yes | yes | no |
| UR — Latvian Register of Enterprises (`ur_latvia`) | `Open Government Data (PSI Directive)` | yes | yes | no |
| Wikidata (`wikidata`) | `CC0-1.0` | yes | no | no |
| Zefix — Swiss Commercial Registry (`zefix`) | `CC-BY-4.0` | yes | yes | no |

# How combined licensing is assessed

When a result combines several sources, the **most restrictive** licence applies to the bundle: a single non-commercial source (e.g. OpenSanctions `CC-BY-NC-4.0`) makes the whole export non-commercial. OpenCheck computes this verdict at export time (`opencheck.licensing.assess`).

> This licensing summary is informational only and is not legal advice. Verify each source's licence terms before commercial use or redistribution.

# Citations

- https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md
