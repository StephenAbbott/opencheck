# OpenCheck — top-level convenience targets
#
# Run from the repo root.  All Python commands run inside backend/ so that
# the opencheck package is importable without extra sys.path fiddling.
#
# Standard data layout (adjust with env vars if your paths differ):
#   GLEIF_DB   backend/data/bods/gleif/gleif_version_0_4.db
#   UK_DB      backend/data/bods/uk_psc/uk_version_0_4.db

GLEIF_DB ?= backend/data/bods/gleif/gleif_version_0_4.db
UK_DB    ?= backend/data/bods/uk_psc/uk_version_0_4.db
MAX_HOPS ?= 3

.PHONY: help build-demo validate-demo test lint

help:
	@echo ""
	@echo "  make build-demo       Re-extract + merge + validate the Phase-0 demo graph"
	@echo "  make validate-demo    Validate existing JSON-Lines without re-extracting"
	@echo "  make test             Run the full backend test suite (pytest)"
	@echo "  make lint             Run ruff + mypy on the backend"
	@echo ""
	@echo "Override DB paths:"
	@echo "  make build-demo GLEIF_DB=/path/to/gleif.db UK_DB=/path/to/uk.db"
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
## Backend
## -------------------------------------------------------------------------

test:
	cd backend && python -m pytest

lint:
	cd backend && ruff check opencheck tests scripts && mypy opencheck
