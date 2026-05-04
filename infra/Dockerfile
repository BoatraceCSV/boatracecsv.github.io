# syntax=docker/dockerfile:1.6
#
# Cloud Run Jobs image for preview-realtime.
#
# At build time we install Python deps only.
# The repository itself is cloned at *runtime* by run.sh using a PAT, so the
# image stays small and we always operate against the latest main.
#
# Build (locally / via Cloud Build):
#   docker build -f infra/Dockerfile -t preview-realtime .
#
# Build context MUST be the repo root (so scripts/requirements.txt is reachable).

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    LANG=C.UTF-8 \
    TZ=Asia/Tokyo

# git: clone + commit + push at runtime
# ca-certificates: HTTPS to GitHub / boatrace endpoints
# tzdata: ensure JST is resolvable for datetime.now(JST) calls
# tini: proper PID 1 / signal handling so Cloud Run timeouts terminate cleanly
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git \
        ca-certificates \
        tzdata \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies separately to maximize layer caching.
COPY scripts/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Entry script. The repo (and therefore scripts/preview-realtime.py) is fetched
# at runtime, so we do not COPY scripts/ into the image.
# `--chmod=0755` ensures the file is executable in the image regardless of the
# source file's mode on the build machine (macOS often drops the +x bit when
# files are created via editor/sync tools).
COPY --chmod=0755 infra/run.sh /app/run.sh

# Cloud Run Jobs run as root by default; we drop to an unprivileged user. The
# user needs a writable home for git's HTTP credential / config files.
RUN useradd --create-home --uid 1000 runner \
    && mkdir -p /workspace \
    && chown -R runner:runner /workspace
USER runner
ENV HOME=/home/runner

# All transient state lives under /workspace (cloned repo, logs).
WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--", "/app/run.sh"]
