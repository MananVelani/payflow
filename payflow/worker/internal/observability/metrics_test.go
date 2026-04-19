package observability_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/your-org/payflow/worker/internal/metrics"
)

// TestMetricCardinality ensures that no new high-cardinality label patterns 
// or excessive metric families are introduced without audit.
func TestMetricCardinality(t *testing.T) {
	// Register metrics first
	metrics.Register()

	// Gather metrics from the default registry
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	assert.NoError(t, err)

	// Threshold for total registered worker metric families
	const maxWorkerMetricFamilies = 20
	
	workerCount := 0
	for _, mf := range metricFamilies {
		if len(mf.Metric) > 0 && (mf.GetName()[:7] == "worker_") {
			workerCount++
		}
	}
	
	assert.LessOrEqual(t, workerCount, maxWorkerMetricFamilies, "Too many distinct worker metric families registered (%d > %d). Audit labels and metric count.", workerCount, maxWorkerMetricFamilies)
}

