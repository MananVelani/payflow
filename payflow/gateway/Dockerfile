# C1 API Gateway Dockerfile

FROM golang:1.25-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY proto ./proto
COPY gateway ./gateway

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /gateway ./gateway

FROM alpine:3.19 AS final
RUN apk add --no-cache ca-certificates wget busybox-extras iproute2
RUN adduser -D -u 1001 app
COPY --from=builder /gateway /gateway
USER app
EXPOSE 8080 9090 2112
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1
ENTRYPOINT ["/gateway"]
