# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

# System dependencies:
#   git                         — uv clones git+ deps such as bods-xml, bods-ftm
#   libpango-1.0-0 / -ft2       — WeasyPrint renders the PDF report (Pango; no
#                                 Cairo needed in WeasyPrint 53+). These pull in
#                                 glib, harfbuzz, fontconfig and freetype.
#   fonts-dejavu-core           — the DejaVu Sans/Mono faces the report uses
#   g++ / libicu-dev / pkg-config — build pyicu from source (followthemoney →
#                                 bods-ftm, installed via the `ftm` extra for
#                                 the canonical BODS↔FtM conversion used by
#                                 the OpenAleph /api/2/match step)
RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        g++ \
        libicu-dev \
        pkg-config \
        libpango-1.0-0 \
        libpangoft2-1.0-0 \
        fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Install uv via pip — avoids GHCR dependency that can fail with 502 on Render
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy README for build metadata
COPY README.md ./

# Cache-friendly: copy only lock+manifest first, then sync.
COPY backend/pyproject.toml ./
# uv.lock will be generated on first run if absent; keep optional for Phase 0.
COPY backend/uv.lock* ./

# Create the environment inside the container (incl. the ftm extra:
# bods-ftm → followthemoney → pyicu, built against the ICU headers above).
RUN uv sync --no-install-project --extra ftm

# Now copy the app source.
COPY backend/ ./

# Install the project itself.
RUN uv sync --extra ftm

# Copy pre-extracted BODS bundles and demo fixtures so the app works
# without a mounted volume (needed on Render and other container hosts).
# The live/ cache directory is gitignored and will be created at runtime
# by the cache layer when live API calls are made.
COPY data/ /app/data/

# Pin the data root explicitly so the app never relies on path heuristics to
# find /app/data (where the pre-extracted BODS bundles + demo fixtures live).
# This is the deterministic counterpart to _find_project_root() in cache.py.
ENV OPENCHECK_DATA_ROOT=/app/data

EXPOSE 8000

# Use $PORT so the process binds to whatever port the host assigns
# (Render injects PORT; falls back to 8000 for local docker-compose).
CMD ["sh", "-c", "uv run uvicorn opencheck.app:app --host 0.0.0.0 --port ${PORT:-8000}"]
