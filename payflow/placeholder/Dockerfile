# Placeholder Dockerfile for PayFlow services (Members 1-4)
# Each member copies this into their service directory and replaces with real code in Weeks 2-3.

FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY main.go .
RUN go mod init placeholder && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /placeholder .

FROM alpine:3.19 AS final
RUN apk add --no-cache ca-certificates wget
RUN adduser -D -u 1001 app
COPY --from=builder /placeholder /placeholder
USER app
EXPOSE 8080 2112
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1
ENTRYPOINT ["/placeholder"]
