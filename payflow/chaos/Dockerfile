# Chaos CLI Dockerfile — PayFlow Fault Injection Tool
# Multi-stage build for minimal final image size
#
# Run with Docker socket access for container operations:
#   docker run -v /var/run/docker.sock:/var/run/docker.sock chaos

FROM golang:1.25-alpine AS builder
WORKDIR /build
RUN apk add --no-cache git
COPY go.mod go.sum* ./
RUN go mod download || true
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /chaos .

FROM alpine:3.19 AS final
RUN apk add --no-cache ca-certificates
RUN adduser -D -u 1001 chaos
COPY --from=builder /chaos /chaos
USER chaos
ENTRYPOINT ["/chaos"]
