# Build Stage
FROM rust:1.88-slim AS builder

WORKDIR /app

# Copy full monorepo source
COPY . .

WORKDIR /app/server/rust

ARG EXAMPLE=server_with_redis
ARG FEATURES=redis-adapter

RUN cargo build --release --example ${EXAMPLE} --features ${FEATURES} && \
    cp /app/server/rust/target/release/examples/${EXAMPLE} /app/server_bin

# Production Runner Stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy compiled binary and static assets
COPY --from=builder /app/server_bin /app/server_bin
COPY client /app/client
COPY examples /app/examples
COPY tests /app/tests

# Set working directory to /app/server/rust so relative paths (../../client, ../../examples/index.html) resolve correctly
WORKDIR /app/server/rust

EXPOSE 8080 8081

ENTRYPOINT ["/app/server_bin"]
