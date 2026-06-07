#!/bin/bash
# Run the import.cypher script via cypher-shell against a running Neo4j instance.
# Place the CSV files in Neo4j's import directory (or serve them via HTTP) first.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cypher-shell "${@}" < "${DIR}/import.cypher"
