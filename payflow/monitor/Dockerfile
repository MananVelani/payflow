# C5 Monitor Dockerfile — PayFlow Monitoring Service
# Multi-stage build for minimal final image size

FROM golang:1.25-alpine AS builder
WORKDIR /build
RUN apk add --no-cache git
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /monitor .

FROM alpine:3.19 AS final
RUN apk add --no-cache ca-certificates wget iproute2
RUN adduser -D -u 1001 monitor
COPY --from=builder /monitor /monitor
EXPOSE 3000 9091
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:3000/health || exit 1
ENTRYPOINT ["/monitor"]
