package stream_test

import (
	"testing"
	"time"

	"github.com/your-org/payflow/worker/internal/stream"
)

func TestKeepaliveParams(t *testing.T) {
	params := stream.ClientParams()
	
	if params.Time != 10*time.Second {
		t.Errorf("expected 10s ping time, got %v", params.Time)
	}
	
	if params.Timeout != 5*time.Second {
		t.Errorf("expected 5s timeout, got %v", params.Timeout)
	}
	
	if !params.PermitWithoutStream {
		t.Error("expected PermitWithoutStream to be true")
	}
}
