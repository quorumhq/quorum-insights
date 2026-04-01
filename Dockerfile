# Quorum Insights — Standalone container
# Multi-stage build for minimal image size

FROM python:3.12-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY schema/ schema/
COPY connectors/ connectors/
COPY query/ query/
COPY stats/ stats/
COPY intelligence/ intelligence/
COPY digest/ digest/
COPY cli.py .

# Create data directory for cache and sync state
RUN mkdir -p /data/cache /data/state

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default: run the CLI
ENTRYPOINT ["python", "cli.py"]
CMD ["run", "--dry-run"]
