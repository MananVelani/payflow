# C4 Payment Log Dockerfile

FROM golang:1.25-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY proto ./proto
COPY payment-log ./payment-log

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /payment-log ./payment-log
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /audit ./payment-log/audit

FROM alpine:3.19 AS final
RUN apk add --no-cache ca-certificates wget busybox-extras iproute2
COPY --from=builder /payment-log /payment-log
COPY --from=builder /audit /usr/local/bin/payflow-audit
EXPOSE 50054 2112 2113
ENTRYPOINT ["/payment-log"]
