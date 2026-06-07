# OpenCheck — top-level convenience targets
#
# Run from the repo root.  All Python commands run inside backend/ so that
# the opencheck package is importable without extra sys.path fiddling.
#
# Standard data layout (adjust with env vars if your paths differ):
#   GLEIF_DB   backend/data/bods/gleif/gleif_version_0_4.db
#   UK_DB      backend/data/bods/uk_psc/uk_version_0_4.db

GLEIF_DB  ?= backend/data/bods/gleif/gleif_version_0_4.db
UK_DB     ?= backend/data/bods/uk_psc/uk_version_0_4.db
MAX_HOPS  ?= 3
DEMO_DIR   ?= data/demo
NEO4J_OUT  ?= data/demo/neo4j
SLIDES_OUT ?= data/demo/slides

.PHONY: help build-demo validate-demo export-neo4j slides test lint

help:
	@echo ""
	@echo "  make build-demo       Re-extract + merge + validate the Phase-0 demo graph"
	@echo "  make validate-demo    Validate existing JSON-Lines without re-extracting"
	@echo "  make export-neo4j     Export demo graph to Neo4j-importable CSVs"
	@echo "  make slides           Generate self-contained HTML slide viewer"
	@echo "  make test             Run the full backend test suite (pytest)"
	@echo "  make lint             Run ruff + mypy on the backend"
	@echo ""
	@echo "Override DB paths:"
	@echo "  make build-demo GLEIF_DB=/path/to/gleif.db UK_DB=/path/to/uk.db"
	@echo "Override Neo4j export path:"
	@echo "  make export-neo4j NEO4J_OUT=/path/to/output"
	@echo ""

## -------------------------------------------------------------------------
## Phase 5 — Demo graph build
## -------------------------------------------------------------------------

# Full build: extract from SQLite → merge GLEIF+UK per entity → validate.
# This is the Phase 5 "one reproducible build step."
build-demo:
	cd backend && python scripts/build_demo.py \
		--gleif ../$(GLEIF_DB) \
		--uk ../$(UK_DB) \
		--max-hops $(MAX_HOPS)

# Validate only: skip the (slow) extraction step and just run lib-cove-bods
# over whatever JSON-Lines are already on disk.
validate-demo:
	cd backend && python scripts/build_demo.py \
		--gleif ../$(GLEIF_DB) \
		--uk ../$(UK_DB) \
		--skip-extract

## -------------------------------------------------------------------------
## Phase 7 — Talk / slide exports
## -------------------------------------------------------------------------

# Combine all 9 demo JSONL files, deduplicate by statementId, then run
# bods-neo4j to-csv to produce CSVs + import.cypher in data/demo/neo4j/.
# Requires: pip install git+https://github.com/StephenAbbott/bods-neo4j.git
export-neo4j:
	cd backend && python scripts/export_neo4j.py \
		--demo-dir ../$(DEMO_DIR) \
		--out ../$(NEO4J_OUT)

# Generate data/demo/slides/opencheck_demo.html — self-contained Cytoscape.js
# ownership graph viewer for all 9 demo entities. Open in a browser to
# navigate, screenshot for slides, or print (one entity per page).
slides:
	cd backend && python scripts/gen_slide_html.py \
		--demo-dir ../$(DEMO_DIR) \
		--out ../$(SLIDES_OUT)

## -------------------------------------------------------------------------
## Backend
## -------------------------------------------------------------------------

test:
	cd backend && python -m pytest

lint:
	cd backend && ruff check opencheck tests scripts && mypy opencheck
