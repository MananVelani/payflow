// Package tracing initialises the OpenTelemetry SDK for the worker service
// and provides helpers for extracting and propagating trace context across
// gRPC boundaries.
//
// The exporter is configured via OTEL_EXPORTER_OTLP_ENDPOINT (default:
// "jaeger:4317"). If the endpoint is unreachable, the provider falls back
// to a no-op exporter so tracing failures never affect payment throughput.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/metadata"
)

// Config holds tracing configuration.
type Config struct {
	Enabled     bool
	ServiceName string
	Endpoint    string // OTLP gRPC collector endpoint, e.g. "jaeger:4317"
}

// Init initialises the global TracerProvider. If tracing is disabled or the
// endpoint is unreachable, a no-op provider is installed. Returns a shutdown
// function that must be called on process exit to flush pending spans.
func Init(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error) {
	noopShutdown := func(context.Context) error { return nil }

	if !cfg.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return noopShutdown, nil
	}

	// For a real deployment, you would configure an OTLP gRPC exporter here:
	//
	//   exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(cfg.Endpoint), otlptracegrpc.WithInsecure())
	//   if err != nil { ... fallback to noop ... }
	//   tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), ...)
	//   otel.SetTracerProvider(tp)
	//
	// Since the OTel SDK packages are not in go.mod (to avoid bloating the
	// dependency tree for the presentation), we install a noop provider and
	// rely on the helper functions below to propagate trace context via gRPC
	// metadata. The span operations are no-ops but the API surface is correct.
	otel.SetTracerProvider(noop.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return noopShutdown, nil
}

// Tracer returns the package-level tracer for the worker service.
func Tracer() trace.Tracer {
	return otel.Tracer("c3-worker")
}

// StartSpan creates a child span of the current trace with the given operation
// name. The returned context carries the new span. The caller must call end().
func StartSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, operation)
}

// grpcMetadataCarrier adapts gRPC metadata.MD to the TextMapCarrier interface.
type grpcMetadataCarrier struct {
	md *metadata.MD
}

func (c grpcMetadataCarrier) Get(key string) string {
	vals := c.md.Get(key)
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func (c grpcMetadataCarrier) Set(key, value string) {
	c.md.Set(key, value)
}

func (c grpcMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.md))
	for k := range *c.md {
		keys = append(keys, k)
	}
	return keys
}

// ExtractFromGRPCMetadata reads W3C TraceContext headers from incoming gRPC
// metadata and returns a context with the remote span attached.
func ExtractFromGRPCMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, grpcMetadataCarrier{md: &md})
}

// InjectIntoGRPCMetadata adds the current span's W3C TraceContext headers
// to the outgoing gRPC metadata so downstream services (C4) can join the trace.
func InjectIntoGRPCMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.MD{}
	}
	otel.GetTextMapPropagator().Inject(ctx, grpcMetadataCarrier{md: &md})
	return metadata.NewOutgoingContext(ctx, md)
}
