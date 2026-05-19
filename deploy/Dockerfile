# Multi-stage build for the hippius-hub-health Cachet poller.
#
# The hippius_hub package imports its Rust extension at import-time
# (hippius_hub/__init__.py -> .file_download -> hippius_hub.hippius_core),
# so we can't just `pip install .` from a slim base. The builder stage has
# Rust + maturin and produces a wheel; the runtime stage installs only that
# wheel into a slim image.

FROM python:3.11-slim AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential curl ca-certificates pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --default-toolchain stable --profile minimal

RUN pip install --no-cache-dir maturin

COPY . .
RUN maturin build --release --out /wheels


FROM python:3.11-slim AS runtime
WORKDIR /app

COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

USER 65532:65532
ENTRYPOINT ["hippius-hub-health"]
