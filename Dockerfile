# Base image is overridable so a mirrored or air-gapped registry can supply its
# own equivalent without patching this file:
#   docker compose build --build-arg PYTHON_IMAGE=my-mirror/python:3.12-slim
ARG PYTHON_IMAGE=python:3.12-slim

FROM ${PYTHON_IMAGE} AS runtime

# git is a runtime dependency, not a build one: the workspace adapter shells
# out to it for worktrees, checkpoints and diffs.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# uv comes from PyPI rather than a second registry: everything else in this
# build already needs PyPI, and one fewer registry is one fewer thing to mirror.
RUN pip install --no-cache-dir uv==0.10.12

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first: editing source must not re-resolve the lockfile.
# README.md is copied because pyproject declares it as the project readme.
COPY pyproject.toml uv.lock README.md ./
# uv byte-compiles with a parallel worker pool, which needs more than the
# 1024 soft file-descriptor limit that many hosts still default to. Raise to
# whatever the hard limit allows, for this step only.
RUN ulimit -n "$(ulimit -Hn)" && uv sync --frozen --no-install-project

COPY src/ ./src/
COPY migrations/ ./migrations/
COPY examples/ ./examples/
COPY benchmarks/ ./benchmarks/
RUN ulimit -n "$(ulimit -Hn)" && uv sync --frozen

# Run unprivileged. The managed worktree root is owned by this user so the
# workspace adapter can create and clean session worktrees.
RUN useradd --create-home --uid 10001 witness \
    && mkdir -p /workspaces \
    && chown -R witness:witness /app /opt/venv /workspaces
USER witness

RUN git config --global user.email "witness@localhost" \
    && git config --global user.name "Witness Runtime"

EXPOSE 8000
CMD ["react-agent-web"]
