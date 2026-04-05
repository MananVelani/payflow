# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.24.4-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -ldflags="-w -s" \
  -o /worker \
  ./cmd/worker

# ── Stage 2: Final Image ──────────────────────────────────────────────────────
# Using alpine for final image to support health check via wget
FROM alpine:3.18

RUN apk add --no-cache ca-certificates

WORKDIR /
COPY --from=builder /worker /worker

# EXPOSE metrics and gRPC
EXPOSE 9092 50053

# Health check using metrics endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD ["wget", "--quiet", "--tries=1", "--spider", "http://localhost:9092/metrics"]

ENTRYPOINT ["/worker"]
