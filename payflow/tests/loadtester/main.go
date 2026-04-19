package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"payflow/sdk"
)

func main() {
	log.Println("Starting PayFlow Integration Load Tester...")

	// 1. Initialize Member 3's SDK Client
	client := sdk.NewClient("http://localhost:8080")
	client.SetMaxRetries(5)

	var successCount uint64
	var errorCount uint64
	var totalLatency time.Duration
	var mu sync.Mutex

	totalRequests := 100
	concurrency := 20
	
	log.Printf("Bombarding Gateway with %d concurrent payments (Total: %d)...\n", concurrency, totalRequests)
	
	start := time.Now()
	
	// Create a buffered channel to act as a worker pool
	jobs := make(chan int, totalRequests)
	for i := 1; i <= totalRequests; i++ {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for jobID := range jobs {
				
				// Generate random payment payload
				req := sdk.PaymentRequest{
					Amount:         math.Round((rand.Float64()*1000)*100) / 100, // random amount up to $1000
					Currency:       "USD",
					MerchantID:     fmt.Sprintf("merch-%d", rand.Intn(50)+1),
					IdempotencyKey: fmt.Sprintf("idemp-%d-%d", jobID, time.Now().UnixNano()),
				}

				reqStart := time.Now()
				
				// Send via Member 3's beautifully written SDK
				resp, err := client.SubmitPayment(context.Background(), req)
				
				duration := time.Since(reqStart)
				
				mu.Lock()
				totalLatency += duration
				mu.Unlock()

				if err != nil {
					atomic.AddUint64(&errorCount, 1)
					log.Printf("[Worker %d] Failed Txn %d: %v", workerID, jobID, err)
				} else {
					atomic.AddUint64(&successCount, 1)
					log.Printf("[Worker %d] Success Txn %s | Status: %s", workerID, resp.TxnID, resp.Status)
				}
			}
		}(w)
	}

	// Wait for all requests to finish
	wg.Wait()
	duration := time.Since(start)

	log.Println("\n=======================================================")
	log.Println("LOAD TEST COMPLETE - PERFORMANCE RESULTS")
	log.Println("=======================================================")
	log.Printf("Total Time Elapsed:   %v\n", duration)
	log.Printf("Successful Payments:  %d\n", atomic.LoadUint64(&successCount))
	log.Printf("Failed Payments:      %d\n", atomic.LoadUint64(&errorCount))
	
	if totalRequests > 0 {
		avgLatency := time.Duration(int64(totalLatency) / int64(totalRequests))
		log.Printf("Average Latency:      %v\n", avgLatency)
		throughput := float64(totalRequests) / duration.Seconds()
		log.Printf("Throughput (TPS):     %.2f req/sec\n", throughput)
	}
	log.Println("=======================================================")
}
