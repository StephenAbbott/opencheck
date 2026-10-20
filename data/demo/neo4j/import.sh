#!/bin/bash
# Run the import.cypher script via cypher-shell against a running Neo4j instance.
# Place the CSV files in Neo4j's import directory (or serve them via HTTP) first.
#
# Requirements:
#   * APOC Core, for apoc.convert.fromJsonList() (list properties). Neo4j 5
#     bundles it as labs/apoc-*-core.jar: copy it into plugins/ (Docker:
#     NEO4J_PLUGINS='["apoc"]').
#   * Neo4j's DEFAULT CSV quote handling, db.import.csv.legacy_quote_escaping
#     =true. The CSVs double backslashes and quote every cell containing one,
#     which that setting reads back exactly. Do NOT set it to false: every
#     backslash in a name would then load doubled, and JSON list cells that
#     contain one would fail to parse.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cypher-shell "${@}" < "${DIR}/import.cypher"
