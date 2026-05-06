# vigil — single-instance anomaly-detection service.
#
# Multi-stage: build wheels in a heavy image, copy into a slim runtime.
# The runtime owns /var/lib/vigil for the baseline snapshot (mounted as
# a fly.io volume in production).

FROM python:3.12-slim AS builder

WORKDIR /build

# System deps for psycopg / scipy build (most are wheels but be explicit).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
COPY migrations ./migrations

RUN pip install --upgrade pip wheel \
    && pip wheel --wheel-dir /wheels .

# ---------------------------------------------------------------- runtime

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIGIL_BASELINE_SNAPSHOT_PATH=/var/lib/vigil/baselines.json \
    VIGIL_API_HOST=0.0.0.0 \
    VIGIL_API_PORT=8080

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 1000 -m -d /home/vigil vigil \
    && mkdir -p /var/lib/vigil \
    && chown vigil:vigil /var/lib/vigil

COPY --from=builder /wheels /wheels
COPY migrations ./migrations

RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels

USER vigil

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--"]

# Run the ingest+detect loop and the HTTP API together.
CMD ["vigil", "run"]
