# Build Stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY . .

WORKDIR /app/server/go
RUN go build -ldflags="-s -w" -o /app/server_bin ./examples/main.go

# Production Runner Stage
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy compiled binary and static assets
COPY --from=builder /app/server_bin /app/server_bin
COPY client /app/client
COPY examples /app/examples
COPY tests /app/tests

# Set working directory to /app/server/go so relative paths (../../examples, ../../client, etc.) resolve correctly
WORKDIR /app/server/go

EXPOSE 8080 8081

ENTRYPOINT ["/app/server_bin"]
