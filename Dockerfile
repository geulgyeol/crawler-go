# Build stage
FROM --platform=$BUILDPLATFORM golang:1.27.1-trixie AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    libc6-dev \
    gcc-aarch64-linux-gnu \
    libc6-dev-arm64-cross \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

ARG TARGETOS=linux
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
    export CC=aarch64-linux-gnu-gcc; \
    else \
    export CC=gcc; \
    fi && \
    export CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} && \
    mkdir -p /out && \
    go build -a -o /out/crawler . && \
    for app in cmd/*; do \
    go build -a -o "/out/${app##*/}" "./$app" || exit 1; \
    done

# Final stage
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy all executables; mount config.yaml at /app/config.yaml when running.
COPY --from=builder /out/ /app/

# Run the root program by default; override with /app/<cmd> for a worker.
CMD ["/app/crawler"]
