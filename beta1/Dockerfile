# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download   # cached layer — only re-runs when go.mod changes
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -extldflags '-static'" \
    -o /worker \
    ./cmd/worker

# ── Stage 2: Final Image ──────────────────────────────────────────────────────
# distroless/static: no shell, no libc, no package manager — minimal attack surface
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /worker /worker

USER nonroot:nonroot
EXPOSE 9092

ENTRYPOINT ["/worker"]
