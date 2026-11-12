---
type: "Data Source"
title: "Sirene \u2014 INSEE register of legal units (France), with the RNA"
description: "INSEE's Sirene register, read through API Sirene: every French legal unit with a SIREN \u2014 companies, but also associations, foundations and public bodies, which the Registre National des Entreprises does not hold. Name, legal category, status, creation date and head-office address; for an association, its record in the R\u00e9pertoire national des associations (title, purpose, declaration and dissolution dates). Sirene publishes no officers, shareholders or beneficial owners."
resource: "https://www.sirene.fr/"
tags: ["cdd", "national-register", "Licence Ouverte / Open Licence 2.0", "commercial-yes"]
timestamp: "2026-11-11"
source_id: "sirene"
license: "Licence Ouverte / Open Licence 2.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

INSEE's Sirene register, read through API Sirene: every French legal unit with a SIREN — companies, but also associations, foundations and public bodies, which the Registre National des Entreprises does not hold. Name, legal category, status, creation date and head-office address; for an association, its record in the Répertoire national des associations (title, purpose, declaration and dissolution dates). Sirene publishes no officers, shareholders or beneficial owners. Official national company / beneficial-ownership register.

- **Source id:** `sirene`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `siren`

# Licensing

- **Licence:** `Licence Ouverte / Open Licence 2.0` — Licence Ouverte / Open Licence 2.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Source: INSEE, Sirene (api.insee.fr), and the Ministry of the Interior, Répertoire national des associations (data.gouv.fr). Licence Ouverte / Open Licence 2.0.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_sirene`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.sirene.fr/
