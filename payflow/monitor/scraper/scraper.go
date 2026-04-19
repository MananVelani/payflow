// Package scraper polls Prometheus /metrics endpoints from all PayFlow services
// and aggregates results into a ClusterSnapshot. It parses raw Prometheus text
// exposition format, computes histogram percentiles, and detects coordinator
// election state transitions by tracking epoch changes across scrape cycles.
package scraper

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/payflow/monitor/config"
	"github.com/payflow/monitor/metrics"
)

// CoordinatorState holds parsed metrics and derived state for one coordinator node.
type CoordinatorState struct {
	NodeID          string    `json:"node_id"`
	Address         string    `json:"address"`
	IsLeader        bool      `json:"is_leader"`
	Epoch           int64     `json:"epoch"`
	ElectionCount   int64     `json:"election_count"`
	QueueDepth      int64     `json:"queue_depth"`
	WorkerCount     int64     `json:"worker_count"`
	HeartbeatMisses int64     `json:"heartbeat_misses"`
	State           string    `json:"state"`
	Reachable       bool      `json:"reachable"`
	LastSeen        time.Time `json:"last_seen"`
}

// WorkerState holds parsed metrics and derived state for one worker node.
type WorkerState struct {
	WorkerID       string    `json:"worker_id"`
	Address        string    `json:"address"`
	Alive          bool      `json:"alive"`
	TasksProcessed int64     `json:"tasks_processed"`
	TasksFailed    int64     `json:"tasks_failed"`
	LatencyP50Ms   float64   `json:"latency_p50_ms"`
	LatencyP99Ms   float64   `json:"latency_p99_ms"`
	Reachable      bool      `json:"reachable"`
	LastSeen       time.Time `json:"last_seen"`
}

// PaymentLogState holds parsed metrics for the C4 payment log service.
type PaymentLogState struct {
	LogAppendTotal    int64     `json:"log_append_total"`
	LogSizeBytes      int64     `json:"log_size_bytes"`
	IdempotencyHits   int64     `json:"idempotency_hits"`
	IdempotencyMisses int64     `json:"idempotency_misses"`
	Reachable         bool      `json:"reachable"`
	LastSeen          time.Time `json:"last_seen"`
}

// GatewayState holds parsed metrics for the C1 API gateway service.
type GatewayState struct {
	Reachable  bool               `json:"reachable"`
	LastSeen   time.Time          `json:"last_seen"`
	RawMetrics map[string]float64 `json:"raw_metrics,omitempty"`
}

// ClusterSnapshot is the aggregated state of the entire PayFlow cluster
// at a single point in time. It is built from Prometheus scrape results.
type ClusterSnapshot struct {
	ScrapedAt    time.Time       `json:"scraped_at"`
	Coordinators []CoordinatorState `json:"coordinators"`
	Workers      []WorkerState      `json:"workers"`
	PaymentLog   PaymentLogState    `json:"payment_log"`
	Gateway      GatewayState       `json:"gateway"`
}

// LeaderNode returns the coordinator with IsLeader==true, or nil if none.
func (s ClusterSnapshot) LeaderNode() *CoordinatorState {
	for i := range s.Coordinators {
		if s.Coordinators[i].IsLeader {
			return &s.Coordinators[i]
		}
	}
	return nil
}

// TotalQueueDepth returns QueueDepth from the leader, or 0 if no leader.
func (s ClusterSnapshot) TotalQueueDepth() int64 {
	leader := s.LeaderNode()
	if leader != nil {
		return leader.QueueDepth
	}
	return 0
}

// LiveWorkerCount returns count of workers with Alive==true and Reachable==true.
func (s ClusterSnapshot) LiveWorkerCount() int {
	count := 0
	for _, w := range s.Workers {
		if w.Alive && w.Reachable {
			count++
		}
	}
	return count
}

// TargetConfig describes a single scrape target with its role in the cluster.
type TargetConfig struct {
	Name string
	URL  string
	Role string // "coordinator", "worker", "payment-log", "gateway"
}

// Scraper manages periodic metric collection from all configured PayFlow services.
type Scraper struct {
	targets    []TargetConfig
	client     *http.Client
	m          *metrics.Metrics
	interval   time.Duration
	mu         sync.RWMutex
	snapshot   ClusterSnapshot
	prevEpochs map[string]int64
	onChange   []func(ClusterSnapshot)
	onChangeMu sync.Mutex
}

// New creates a Scraper configured from the provided config and metrics instances.
// It parses the scrape target URLs to determine each target's role in the cluster.
func New(cfg *config.Config, m *metrics.Metrics) *Scraper {
	targets := make([]TargetConfig, 0, len(cfg.ScrapeTargets))
	for _, rawURL := range cfg.ScrapeTargets {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			log.Printf("[scraper] warning: cannot parse target URL %q: %v", rawURL, err)
			continue
		}

		hostname := parsed.Hostname()
		name := hostname
		role := "unknown"

		switch {
		case strings.Contains(hostname, "coordinator"):
			role = "coordinator"
		case strings.Contains(hostname, "worker"):
			role = "worker"
		case strings.Contains(hostname, "payment-log"):
			role = "payment-log"
		case strings.Contains(hostname, "gateway") || strings.Contains(hostname, "api-gateway"):
			role = "gateway"
		}

		targets = append(targets, TargetConfig{
			Name: name,
			URL:  rawURL,
			Role: role,
		})
	}

	log.Printf("[scraper] initialized with %d targets", len(targets))
	for _, t := range targets {
		log.Printf("[scraper]   %s [%s] → %s", t.Name, t.Role, t.URL)
	}

	return &Scraper{
		targets:    targets,
		client:     &http.Client{Timeout: 5 * time.Second},
		m:          m,
		interval:   cfg.ScrapeInterval,
		snapshot:   ClusterSnapshot{},
		prevEpochs: make(map[string]int64),
	}
}

// Subscribe registers a callback that is invoked with a copy of the ClusterSnapshot
// every time a new scrape cycle completes. Each callback runs in its own goroutine
// to prevent slow subscribers from blocking the scrape loop.
func (s *Scraper) Subscribe(fn func(ClusterSnapshot)) {
	s.onChangeMu.Lock()
	defer s.onChangeMu.Unlock()
	s.onChange = append(s.onChange, fn)
}

// Latest returns a copy of the most recent ClusterSnapshot under read lock.
// The returned value is safe to use without synchronization.
func (s *Scraper) Latest() ClusterSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := s.snapshot
	// Deep copy slices to prevent data races
	snap.Coordinators = make([]CoordinatorState, len(s.snapshot.Coordinators))
	copy(snap.Coordinators, s.snapshot.Coordinators)
	snap.Workers = make([]WorkerState, len(s.snapshot.Workers))
	copy(snap.Workers, s.snapshot.Workers)
	return snap
}

// GetResults returns a simplified map of scrape results for backward compatibility.
// Keyed by target URL.
func (s *Scraper) GetResults() map[string]ScrapeResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	results := make(map[string]ScrapeResult)
	for _, t := range s.targets {
		reachable := false
		switch t.Role {
		case "coordinator":
			for _, c := range s.snapshot.Coordinators {
				if c.Address == t.URL {
					reachable = c.Reachable
				}
			}
		case "worker":
			for _, w := range s.snapshot.Workers {
				if w.Address == t.URL {
					reachable = w.Reachable
				}
			}
		case "payment-log":
			reachable = s.snapshot.PaymentLog.Reachable
		case "gateway":
			reachable = s.snapshot.Gateway.Reachable
		}
		results[t.URL] = ScrapeResult{
			Target:     t.URL,
			Up:         reachable,
			LastScrape: s.snapshot.ScrapedAt,
			LatencyMs:  0,
			Error:      "",
		}
	}
	return results
}

// ScrapeResult holds the outcome of a single scrape attempt (backward compat).
type ScrapeResult struct {
	Target     string
	Up         bool
	LastScrape time.Time
	LatencyMs  float64
	Error      string
}

// Start begins the periodic scrape loop. It runs until the provided context is cancelled.
func (s *Scraper) Start(ctx context.Context) {
	log.Printf("[scraper] starting scrape loop: %d targets, interval %s", len(s.targets), s.interval)

	s.scrapeAll(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[scraper] context cancelled, stopping scrape loop")
			return
		case <-ticker.C:
			s.scrapeAll(ctx)
		}
	}
}

// scrapeResult is an internal type holding raw parsed metrics for one target.
type scrapeRawResult struct {
	target  TargetConfig
	metrics map[string]float64
	err     error
	latency time.Duration
}

// scrapeAll performs one full scrape cycle across all targets concurrently,
// then builds a new ClusterSnapshot from the results.
func (s *Scraper) scrapeAll(ctx context.Context) {
	roundCtx, roundCancel := context.WithTimeout(ctx, 10*time.Second)
	defer roundCancel()

	var mu sync.Mutex
	rawResults := make([]scrapeRawResult, 0, len(s.targets))

	var wg sync.WaitGroup
	for _, target := range s.targets {
		wg.Add(1)
		go func(t TargetConfig) {
			defer wg.Done()
			start := time.Now()
			parsed, err := s.scrapeOne(roundCtx, t)
			latency := time.Since(start)

			s.m.ScrapeDuration.WithLabelValues(t.URL).Observe(latency.Seconds())

			if err != nil {
				s.m.ScrapeTargetsUp.WithLabelValues(t.URL).Set(0)
				s.m.ScrapeErrors.WithLabelValues(t.URL, err.Error()).Inc()
				mu.Lock()
				rawResults = append(rawResults, scrapeRawResult{target: t, metrics: nil, err: err, latency: latency})
				mu.Unlock()
				return
			}

			s.m.ScrapeTargetsUp.WithLabelValues(t.URL).Set(1)
			mu.Lock()
			rawResults = append(rawResults, scrapeRawResult{target: t, metrics: parsed, err: nil, latency: latency})
			mu.Unlock()
		}(target)
	}
	wg.Wait()

	// Build raw map keyed by URL
	rawMap := make(map[string]map[string]float64)
	reachableMap := make(map[string]bool)
	for _, r := range rawResults {
		rawMap[r.target.URL] = r.metrics
		reachableMap[r.target.URL] = r.err == nil
	}

	newSnapshot := s.buildSnapshot(rawMap, reachableMap)

	s.mu.Lock()
	s.snapshot = newSnapshot
	s.mu.Unlock()

	s.notifyOnChange(newSnapshot)
}

// scrapeOne performs a single HTTP GET against the specified target and parses the response.
func (s *Scraper) scrapeOne(ctx context.Context, t TargetConfig) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		log.Printf("[scraper] ❌ %s — creating request: %v", t.Name, err)
		return nil, fmt.Errorf("scraper: request %s: %w", t.Name, err)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("[scraper] ❌ %s — HTTP failed: %v", t.Name, err)
		return nil, fmt.Errorf("scraper: HTTP %s: %w", t.Name, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[scraper] ❌ %s — status %d", t.Name, resp.StatusCode)
		return nil, fmt.Errorf("scraper: %s returned status %d", t.Name, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("scraper: reading body from %s: %w", t.Name, err)
	}

	parsed, err := parsePrometheusText(body)
	if err != nil {
		return nil, fmt.Errorf("scraper: parsing metrics from %s: %w", t.Name, err)
	}

	log.Printf("[scraper] ✅ %s — %d metrics parsed", t.Name, len(parsed))
	return parsed, nil
}

// parsePrometheusText parses Prometheus text exposition format into a map of metric names to values.
// Lines starting with "#" (HELP, TYPE) are skipped. Empty lines are skipped.
// Each data line is split on whitespace: first token is metric+labels, second is value.
func parsePrometheusText(body []byte) (map[string]float64, error) {
	result := make(map[string]float64)
	scanner := bufio.NewScanner(bytes.NewReader(body))

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip comment lines (HELP, TYPE declarations)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split on space — first token is metric name+labels, rest is value [timestamp]
		parts := strings.Fields(line)
		if len(parts) < 2 {
			log.Printf("[scraper] warning: unparseable metric line: %q", line)
			continue
		}

		metricKey := parts[0]
		val, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			log.Printf("[scraper] warning: unparseable value in line: %q", line)
			continue
		}

		result[metricKey] = val
	}

	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("scanner error: %w", err)
	}

	return result, nil
}

// buildSnapshot constructs a ClusterSnapshot from raw scraped metric maps.
func (s *Scraper) buildSnapshot(raw map[string]map[string]float64, reachable map[string]bool) ClusterSnapshot {
	now := time.Now()
	snap := ClusterSnapshot{
		ScrapedAt:    now,
		Coordinators: make([]CoordinatorState, 0, 3),
		Workers:      make([]WorkerState, 0, 5),
	}

	// Build coordinator states
	for _, t := range s.targets {
		if t.Role != "coordinator" {
			continue
		}

		m := raw[t.URL]
		isReachable := reachable[t.URL]

		cs := CoordinatorState{
			NodeID:    t.Name,
			Address:   t.URL,
			Reachable: isReachable,
			LastSeen:  now,
		}

		if isReachable && m != nil {
			cs.IsLeader = getMetricVal(m, "payflow_coordinator_is_leader") == 1
			cs.Epoch = int64(getMetricVal(m, "payflow_coordinator_epoch"))
			cs.ElectionCount = int64(getMetricVal(m, "payflow_coordinator_elections_total"))
			cs.QueueDepth = int64(getMetricVal(m, "payflow_coordinator_queue_depth"))
			cs.WorkerCount = int64(getMetricVal(m, "payflow_coordinator_live_workers"))

			// Sum heartbeat misses across all worker labels
			cs.HeartbeatMisses = 0
			for key, val := range m {
				if strings.HasPrefix(key, "payflow_heartbeat_miss_total{") {
					cs.HeartbeatMisses += int64(val)
				}
			}

			// Determine state — check prevEpochs BEFORE updating
			prevEpoch := s.prevEpochs[t.Name]
			if cs.IsLeader {
				cs.State = "LEADER"
			} else if cs.Epoch > prevEpoch && prevEpoch > 0 {
				cs.State = "CANDIDATE"
			} else {
				cs.State = "FOLLOWER"
			}
			// Update prevEpochs AFTER state is determined
			s.prevEpochs[t.Name] = cs.Epoch
		} else {
			cs.State = "DEAD"
		}

		snap.Coordinators = append(snap.Coordinators, cs)
	}

	// Build worker states
	for _, t := range s.targets {
		if t.Role != "worker" {
			continue
		}

		m := raw[t.URL]
		isReachable := reachable[t.URL]

		ws := WorkerState{
			WorkerID:  t.Name,
			Address:   t.URL,
			Reachable: isReachable,
			LastSeen:  now,
		}

		if isReachable && m != nil {
			ws.Alive = getMetricVal(m, "payflow_worker_status") == 1
			ws.TasksProcessed = int64(getMetricVal(m, "payflow_tasks_processed_total"))
			ws.TasksFailed = int64(getMetricVal(m, "payflow_tasks_failed_total"))
			ws.LatencyP50Ms = calcPercentile(m, 0.50)
			ws.LatencyP99Ms = calcPercentile(m, 0.99)
		}

		snap.Workers = append(snap.Workers, ws)
	}

	// Build payment-log state
	for _, t := range s.targets {
		if t.Role != "payment-log" {
			continue
		}
		m := raw[t.URL]
		isReachable := reachable[t.URL]
		snap.PaymentLog = PaymentLogState{
			Reachable: isReachable,
			LastSeen:  now,
		}
		if isReachable && m != nil {
			snap.PaymentLog.LogAppendTotal = int64(getMetricVal(m, "payflow_log_append_total"))
			snap.PaymentLog.LogSizeBytes = int64(getMetricVal(m, "payflow_log_size_bytes"))
			snap.PaymentLog.IdempotencyHits = int64(getMetricVal(m, "payflow_idempotency_hit_total"))
			snap.PaymentLog.IdempotencyMisses = int64(getMetricVal(m, "payflow_idempotency_miss_total"))
		}
	}

	// Build gateway state
	for _, t := range s.targets {
		if t.Role != "gateway" {
			continue
		}
		m := raw[t.URL]
		isReachable := reachable[t.URL]
		snap.Gateway = GatewayState{
			Reachable:  isReachable,
			LastSeen:   now,
			RawMetrics: m,
		}
	}

	return snap
}

// getMetricVal looks up a metric value by exact name, returning 0 if not found.
func getMetricVal(m map[string]float64, name string) float64 {
	if v, ok := m[name]; ok {
		return v
	}
	return 0
}

// bucketEntry represents one bucket in a Prometheus histogram.
type bucketEntry struct {
	le    float64
	count float64
}

// calcPercentile computes a percentile from Prometheus histogram bucket data.
// It extracts _bucket{le="..."} entries, sorts by le, and linearly interpolates
// to find the value at the given percentile p (e.g. 0.50 for P50, 0.99 for P99).
// Returns the result in milliseconds (input buckets are in seconds).
func calcPercentile(m map[string]float64, p float64) float64 {
	const bucketPrefix = "payflow_task_duration_seconds_bucket{le=\""

	var buckets []bucketEntry
	for key, val := range m {
		if !strings.HasPrefix(key, bucketPrefix) {
			continue
		}

		// Extract le value from key like: payflow_task_duration_seconds_bucket{le="0.1"}
		leStart := strings.Index(key, "le=\"") + 4
		leEnd := strings.Index(key[leStart:], "\"")
		if leEnd < 0 {
			continue
		}
		leStr := key[leStart : leStart+leEnd]

		var le float64
		if leStr == "+Inf" {
			le = math.MaxFloat64
		} else {
			var err error
			le, err = strconv.ParseFloat(leStr, 64)
			if err != nil {
				continue
			}
		}

		buckets = append(buckets, bucketEntry{le: le, count: val})
	}

	if len(buckets) == 0 {
		return 0
	}

	// Sort by le ascending
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].le < buckets[j].le
	})

	// Get total count
	totalCount := getMetricVal(m, "payflow_task_duration_seconds_count")
	if totalCount == 0 {
		return 0
	}

	target := p * totalCount

	// Walk buckets to find the interpolation range
	var prevBound float64
	var prevCount float64

	for _, b := range buckets {
		if b.count >= target {
			// Linear interpolation
			bucketWidth := b.le - prevBound
			if b.le == math.MaxFloat64 {
				// +Inf bucket — use previous bound as estimate
				return prevBound * 1000
			}
			countInBucket := b.count - prevCount
			if countInBucket == 0 {
				return b.le * 1000
			}
			fraction := (target - prevCount) / countInBucket
			result := prevBound + fraction*bucketWidth
			return result * 1000 // seconds → milliseconds
		}
		if b.le < math.MaxFloat64 {
			prevBound = b.le
		}
		prevCount = b.count
	}

	// Fallback: return highest finite bucket bound
	if len(buckets) > 0 {
		for i := len(buckets) - 1; i >= 0; i-- {
			if buckets[i].le < math.MaxFloat64 {
				return buckets[i].le * 1000
			}
		}
	}
	return 0
}

// notifyOnChange invokes all registered subscribers with a copy of the snapshot.
// Each subscriber runs in its own goroutine to prevent blocking the scrape loop.
func (s *Scraper) notifyOnChange(snap ClusterSnapshot) {
	s.onChangeMu.Lock()
	listeners := make([]func(ClusterSnapshot), len(s.onChange))
	copy(listeners, s.onChange)
	s.onChangeMu.Unlock()

	for _, fn := range listeners {
		go fn(snap)
	}
}
