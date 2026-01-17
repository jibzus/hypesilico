# Hypesilico Dockerfile
# Multi-stage build for minimal final image

# ====================
# Stage 1: Build
# ====================
FROM rust:1.75-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first (for dependency caching)
COPY Cargo.toml Cargo.lock ./

# Create dummy source for dependency compilation
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn lib() {}" > src/lib.rs

# Build dependencies only (will be cached)
RUN cargo build --release && \
    rm -rf src target/release/deps/hypesilico*

# Copy actual source code
COPY src src

# Build the real binary
RUN touch src/main.rs src/lib.rs && \
    cargo build --release

# ====================
# Stage 2: Runtime
# ====================
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/hypesilico /app/hypesilico

# Copy validation scripts and data
COPY scripts /app/scripts
COPY validation /app/validation

# Create data directory for database
RUN mkdir -p /data

# Set default environment variables
ENV PORT=8080
ENV DATABASE_PATH=/data/ledger.db

# Expose the server port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run the server
CMD ["/app/hypesilico"]
