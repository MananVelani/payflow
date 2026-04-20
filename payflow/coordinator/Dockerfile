# C2 Coordinator Dockerfile

FROM golang:1.25-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY proto ./proto
COPY coordinator ./coordinator

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /coordinator ./coordinator

FROM alpine:3.19 AS final
RUN apk add --no-cache ca-certificates wget busybox-extras iproute2
RUN adduser -D -u 1001 app
COPY --from=builder /coordinator /coordinator
USER app
EXPOSE 50051 2112
ENTRYPOINT ["/coordinator"]
