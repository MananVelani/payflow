package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"payflow/sdk"
)

// Config for load test
const (
	TotalRequests = 1000
	Concurrency   = 100
	GatewayURL    = "http://localhost:8080"
)

func main() {
	log.Printf("Starting Load Test: %d requests at concurrency %d", TotalRequests, Concurrency)
	
	client := sdk.NewClient(GatewayURL)
	client.SetMaxRetries(3)

	var wg sync.WaitGroup
	requests := make(chan int, TotalRequests)
	
	for i := 0; i < TotalRequests; i++ {
		requests <- i
	}
	close(requests)

	var successCount int32
	var failureCount int32

	latencies := make(chan time.Duration, TotalRequests)

	// Start Time
	start := time.Now()

	// Spawn Worker Pool
	for i := 0; i < Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for reqIndex := range requests {
				reqStart := time.Now()

				// Build payload
				payload := sdk.PaymentRequest{
					Amount:         10.50,
					Currency:       "USD",
					MerchantID:     "M_TEST",
					IdempotencyKey: fmt.Sprintf("TXN_LOAD_%d_%d_%d", workerID, reqIndex, time.Now().UnixNano()),
				}

				_, err := client.SubmitPayment(context.Background(), payload)
				
				duration := time.Since(reqStart)
				latencies <- duration

				if err != nil {
					atomic.AddInt32(&failureCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}(i)
	}

	// Wait for pool to drain
	wg.Wait()
	close(latencies)
	totalDuration := time.Since(start)

	// Calculate Telemetry
	var allDurations []time.Duration
	for l := range latencies {
		allDurations = append(allDurations, l)
	}

	sort.Slice(allDurations, func(i, j int) bool {
		return allDurations[i] < allDurations[j]
	})

	p50 := allDurations[int(float64(len(allDurations))*0.50)]
	p95 := allDurations[int(float64(len(allDurations))*0.95)]
	p99 := allDurations[int(float64(len(allDurations))*0.99)]
	
	throughput := float64(TotalRequests) / totalDuration.Seconds()

	log.Println("======================================")
	log.Println("Load Test Results")
	log.Println("======================================")
	log.Printf("Total Time:     %v\n", totalDuration)
	log.Printf("Total Requests: %d\n", TotalRequests)
	log.Printf("Successful:     %d\n", successCount)
	log.Printf("Failed:         %d\n", failureCount)
	log.Printf("Throughput:     %.2f req/sec\n", throughput)
	log.Println("--- Latency Distribution ---")
	log.Printf("p50:            %v\n", p50)
	log.Printf("p95:            %v\n", p95)
	log.Printf("p99:            %v\n", p99)
	log.Println("======================================")
	
	if p99 > 500*time.Millisecond {
		log.Println("WARNING: p99 SLA missed (> 500ms)")
	}
	if throughput < 200 {
		log.Println("WARNING: Throughput SLA missed (< 200 req/sec)")
	}
}
