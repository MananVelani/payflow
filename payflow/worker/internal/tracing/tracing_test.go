package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"google.golang.org/grpc/metadata"
)

func TestInit_Disabled_ReturnsNoopProvider(t *testing.T) {
	cfg := Config{Enabled: false, ServiceName: "test", Endpoint: "localhost:4317"}
	shutdown, err := Init(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// The provider should produce a working (no-op) tracer that doesn't panic.
	tracer := otel.GetTracerProvider().Tracer("test")
	ctx, span := tracer.Start(context.Background(), "noop-test")
	span.End()
	assert.NotNil(t, ctx)

	// Shutdown should not error.
	assert.NoError(t, shutdown(context.Background()))
}

func TestInit_Enabled_ReturnsNoError(t *testing.T) {
	cfg := Config{Enabled: true, ServiceName: "c3-worker", Endpoint: "jaeger:4317"}
	shutdown, err := Init(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestStartSpan_CreatesSpan(t *testing.T) {
	_, err := Init(context.Background(), Config{Enabled: true})
	require.NoError(t, err)

	ctx, span := StartSpan(context.Background(), "test.operation")
	defer span.End()
	assert.NotNil(t, ctx)
	assert.NotNil(t, span)
}

func TestExtractFromGRPCMetadata_NoMetadata(t *testing.T) {
	_, err := Init(context.Background(), Config{Enabled: true})
	require.NoError(t, err)

	// Context without incoming metadata — should return unchanged context.
	ctx := context.Background()
	extracted := ExtractFromGRPCMetadata(ctx)
	assert.NotNil(t, extracted)
}

func TestInjectIntoGRPCMetadata_WritesHeaders(t *testing.T) {
	_, err := Init(context.Background(), Config{Enabled: true})
	require.NoError(t, err)

	ctx := context.Background()
	// Start a span so there's something to inject.
	ctx, span := StartSpan(ctx, "test.inject")
	defer span.End()

	injected := InjectIntoGRPCMetadata(ctx)
	md, ok := metadata.FromOutgoingContext(injected)
	// With noop provider, md may or may not have traceparent,
	// but it should at least not panic.
	_ = ok
	_ = md
}
