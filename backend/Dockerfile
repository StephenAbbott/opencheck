# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

# Install uv (pinned minor version; newer patches picked up on rebuild)
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv

WORKDIR /app

# Copy README for build metadata
COPY README.md ./

# Cache-friendly: copy only lock+manifest first, then sync.
COPY backend/pyproject.toml ./
# uv.lock will be generated on first run if absent; keep optional for Phase 0.
COPY backend/uv.lock* ./

# Create the environment inside the container.
RUN uv sync --no-install-project

# Now copy the app source.
COPY backend/ ./

# Install the project itself.
RUN uv sync

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "opencheck.app:app", "--host", "0.0.0.0", "--port", "8000"]
