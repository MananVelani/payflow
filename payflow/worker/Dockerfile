# C3 Worker Dockerfile

FROM golang:1.25-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY worker ./worker

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /worker ./worker

FROM alpine:3.19 AS final
RUN apk add --no-cache ca-certificates wget busybox-extras iproute2
RUN adduser -D -u 1001 app
COPY --from=builder /worker /worker
USER app
EXPOSE 8080 2112
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1
ENTRYPOINT ["/worker"]
