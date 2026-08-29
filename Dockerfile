# ------------------------------------------------------------------------------
# Build Stage
# ------------------------------------------------------------------------------
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Install ca-certificates and git if needed
RUN apk add --no-cache ca-certificates git

# Cache Go dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code and build statically linked binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/roomer-server examples/main.go

# ------------------------------------------------------------------------------
# Runtime Stage (Ultra-lightweight ~15MB)
# ------------------------------------------------------------------------------
FROM alpine:3.20

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/roomer-server /usr/local/bin/roomer-server

# Copy client scripts, templates, and browser test files
COPY --from=builder /app/src ./src
COPY --from=builder /app/examples ./examples
COPY --from=builder /app/tests ./tests

EXPOSE 8080
ENV PORT=8080
ENV REDIS_ADDR=redis:6379

CMD ["roomer-server"]
