package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// OpenTelemetry and Jaeger
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"

	pb "payflow/proto/payment"
)

// PaymentRequest represents the incoming JSON payload
type PaymentRequest struct {
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
	MerchantID     string  `json:"merchant_id"`
	IdempotencyKey string  `json:"idempotency_key"`
}

type Gateway struct {
	leaderAddr string
	conn       *grpc.ClientConn
	client     pb.PaymentGatewayClient
	mu         sync.Mutex
}

var gatewayRequestTotal uint64

var coordinatorCandidates = []string{
	"coordinator-1:50051",
	"coordinator-2:50052",
	"coordinator-3:50053",
}

func initTracer() (*sdktrace.TracerProvider, error) {
	// A05: C1 Distributed Tracing (Jaeger) using jaeger:14268 based on typical Docker compose layout
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint("http://jaeger:14268/api/traces")))
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("C1-API-Gateway"),
		)),
	)

	otel.SetTracerProvider(tp)
	// Propagator ensures trace_id is forwarded
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return tp, nil
}

func NewGateway(initialLeader string) *Gateway {
	gw := &Gateway{
		leaderAddr: initialLeader,
	}
	gw.connect(initialLeader)
	return gw
}

func (gw *Gateway) connect(addr string) {
	if gw.conn != nil {
		gw.conn.Close()
	}
	// A05 trace_id propagation via gRPC metadata
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Printf("Failed to connect to coordinator at %s: %v", addr, err)
	}
	gw.conn = conn
	gw.client = pb.NewPaymentGatewayClient(conn)
	gw.leaderAddr = addr
	log.Printf("Gateway connected to leader: %s", addr)
}

func (gw *Gateway) updateLeader(addr string) {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	if gw.leaderAddr != addr {
		log.Printf("Leader redirect received. Updating leader to: %s", addr)
		gw.connect(addr)
	}
}

func (gw *Gateway) rotateLeader() {
	gw.mu.Lock()
	defer gw.mu.Unlock()

	idx := -1
	for i, addr := range coordinatorCandidates {
		if addr == gw.leaderAddr {
			idx = i
			break
		}
	}

	nextIdx := 0
	if idx >= 0 {
		nextIdx = (idx + 1) % len(coordinatorCandidates)
	}

	next := coordinatorCandidates[nextIdx]
	log.Printf("Leader unknown, rotating coordinator target to: %s", next)
	gw.connect(next)
}

func (gw *Gateway) getClient() pb.PaymentGatewayClient {
	gw.mu.Lock()
	defer gw.mu.Unlock()
	return gw.client
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": "api-gateway",
	})
}

func handleMetrics(w http.ResponseWriter, _ *http.Request) {
	total := atomic.LoadUint64(&gatewayRequestTotal)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "# HELP payflow_gateway_requests_total Total HTTP requests handled by the API gateway.\n")
	fmt.Fprintf(w, "# TYPE payflow_gateway_requests_total counter\n")
	fmt.Fprintf(w, "payflow_gateway_requests_total %d\n", total)
}

func main() {
	tp, err := initTracer()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()

	tracer := otel.Tracer("payflow/gateway")
	gw := NewGateway("coordinator-1:50051")

	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/metrics", handleMetrics)

	http.HandleFunc("/v1/payments", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&gatewayRequestTotal, 1)
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, "POST /v1/payments", trace.WithAttributes(attribute.String("http.route", "/v1/payments")))
		defer span.End()

		var req PaymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			span.RecordError(err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		maxRetries := 3
		var resp *pb.SubmitTaskResponse

		for i := 0; i < maxRetries; i++ {
			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			client := gw.getClient()
			var err error
			resp, err = client.SubmitTask(reqCtx, &pb.SubmitTaskRequest{
				Epoch:          1,
				Amount:         req.Amount,
				Currency:       req.Currency,
				MerchantId:     req.MerchantID,
				IdempotencyKey: req.IdempotencyKey,
			})
			cancel()

			if err != nil {
				span.RecordError(err)
				errMsg := strings.ToUpper(err.Error())
				if strings.Contains(errMsg, "NOT_LEADER") {
					parts := strings.Split(err.Error(), "|")
					if len(parts) > 1 {
						gw.updateLeader(parts[1])
						continue
					}
					gw.rotateLeader()
					continue
				}
				if strings.Contains(strings.ToLower(err.Error()), "not the leader") {
					gw.rotateLeader()
					continue
				}
				http.Error(w, fmt.Sprintf("Coordinator error: %v", err), http.StatusServiceUnavailable)
				return
			}

			if resp.GetLeaderAddress() != "" {
				gw.updateLeader(resp.GetLeaderAddress())
				continue
			}

			if resp.GetErrorMessage() != "" {
				errMsg := strings.ToUpper(resp.GetErrorMessage())
				if strings.Contains(errMsg, "NOT_LEADER") || strings.Contains(strings.ToLower(resp.GetErrorMessage()), "not the leader") {
					gw.rotateLeader()
					continue
				}
				span.SetAttributes(attribute.String("error.msg", resp.GetErrorMessage()))
				http.Error(w, fmt.Sprintf("Processing error: %v", resp.GetErrorMessage()), http.StatusInternalServerError)
				return
			}
			break
		}

		if resp == nil {
			http.Error(w, "Max retries exceeded", http.StatusServiceUnavailable)
			return
		}

		span.SetAttributes(attribute.String("txn_id", resp.GetTxnId()))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"txn_id":   resp.GetTxnId(),
			"status":   "QUEUED",
			"trace_id": span.SpanContext().TraceID().String(),
		})
	})

	http.HandleFunc("/v1/payments/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&gatewayRequestTotal, 1)
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, "GET /v1/payments/{id}")
		defer span.End()

		txnID := strings.TrimPrefix(r.URL.Path, "/v1/payments/")
		if txnID == "" {
			http.Error(w, "Missing txn_id", http.StatusBadRequest)
			return
		}
		span.SetAttributes(attribute.String("txn_id", txnID))

		maxRetries := 3
		var resp *pb.GetStatusResponse

		for i := 0; i < maxRetries; i++ {
			reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			client := gw.getClient()
			var err error
			resp, err = client.GetPaymentStatus(reqCtx, &pb.GetStatusRequest{
				Epoch: 1,
				TxnId: txnID,
			})
			cancel()

			if err != nil {
				span.RecordError(err)
				errMsg := strings.ToUpper(err.Error())
				if strings.Contains(errMsg, "NOT_LEADER") {
					parts := strings.Split(err.Error(), "|")
					if len(parts) > 1 {
						gw.updateLeader(parts[1])
						continue
					}
					gw.rotateLeader()
					continue
				}
				if strings.Contains(strings.ToLower(err.Error()), "not the leader") {
					gw.rotateLeader()
					continue
				}
				http.Error(w, fmt.Sprintf("Coordinator error: %v", err), http.StatusServiceUnavailable)
				return
			}

			if resp.GetLeaderAddress() != "" {
				gw.updateLeader(resp.GetLeaderAddress())
				continue
			}
			break
		}

		if resp == nil {
			http.Error(w, "Max retries exceeded", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"txn_id":   resp.GetTxnId(),
			"status":   resp.GetStatus(),
			"trace_id": span.SpanContext().TraceID().String(),
		})
	})

	http.HandleFunc("/v1/batch", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint64(&gatewayRequestTotal, 1)
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, "POST /v1/batch")
		defer span.End()

		var reqs []PaymentRequest
		if err := json.NewDecoder(r.Body).Decode(&reqs); err != nil {
			span.RecordError(err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		var pbReqs []*pb.SubmitTaskRequest
		for _, req := range reqs {
			pbReqs = append(pbReqs, &pb.SubmitTaskRequest{
				Epoch:          1,
				Amount:         req.Amount,
				Currency:       req.Currency,
				MerchantId:     req.MerchantID,
				IdempotencyKey: req.IdempotencyKey,
			})
		}

		maxRetries := 3
		var resp *pb.SubmitBatchResponse

		for i := 0; i < maxRetries; i++ {
			reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			client := gw.getClient()
			var err error
			resp, err = client.SubmitBatch(reqCtx, &pb.SubmitBatchRequest{
				Epoch: 1,
				Tasks: pbReqs,
			})
			cancel()

			if err != nil {
				span.RecordError(err)
				errMsg := strings.ToUpper(err.Error())
				if strings.Contains(errMsg, "NOT_LEADER") {
					parts := strings.Split(err.Error(), "|")
					if len(parts) > 1 {
						gw.updateLeader(parts[1])
						continue
					}
					gw.rotateLeader()
					continue
				}
				if strings.Contains(strings.ToLower(err.Error()), "not the leader") {
					gw.rotateLeader()
					continue
				}
				http.Error(w, fmt.Sprintf("Coordinator error: %v", err), http.StatusServiceUnavailable)
				return
			}

			if resp.GetLeaderAddress() != "" {
				gw.updateLeader(resp.GetLeaderAddress())
				continue
			}
			break
		}

		if resp == nil {
			http.Error(w, "Max retries exceeded", http.StatusServiceUnavailable)
			return
		}

		results := make([]map[string]interface{}, 0)
		for _, r := range resp.Responses {
			results = append(results, map[string]interface{}{
				"txn_id":  r.GetTxnId(),
				"success": r.GetSuccess(),
				"error":   r.GetErrorMessage(),
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"batch_results": results,
			"trace_id":      span.SpanContext().TraceID().String(),
		})
	})

	log.Println("C1 API Gateway starting on port 8080. Trace exporter configured.")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start gateway: %v", err)
	}
}
