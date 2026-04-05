// Package dashboard serves the real-time WebSocket dashboard and embedded HTML UI
// for the C5 monitor. It broadcasts ClusterSnapshot state over WebSocket to all
// connected browser clients and exposes a /api/state JSON endpoint for debugging.
package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/payflow/monitor/metrics"
	"github.com/payflow/monitor/scraper"
)

//go:embed static/index.html
var staticFS embed.FS

// WSMessage is the JSON envelope sent over WebSocket to dashboard clients.
type WSMessage struct {
	Type      string      `json:"type"`
	Timestamp string      `json:"ts"`
	Data      interface{} `json:"data,omitempty"`
}

// SnapshotData is the payload inside a WSMessage of type "snapshot".
type SnapshotData struct {
	Coordinators []CoordPanel  `json:"coordinators"`
	Workers      []WorkerPanel `json:"workers"`
	QueueDepth   int64         `json:"queue_depth"`
	LiveWorkers  int           `json:"live_workers"`
	TotalWorkers int           `json:"total_workers"`
	ThroughputPM float64       `json:"throughput_per_min"`
	ScrapeAgeSecs float64      `json:"scrape_age_secs"`
	Stale         bool         `json:"stale"`
}

// CoordPanel is the coordinator state sent to the dashboard.
type CoordPanel struct {
	NodeID    string `json:"node_id"`
	State     string `json:"state"`
	Epoch     int64  `json:"epoch"`
	Reachable bool   `json:"reachable"`
}

// WorkerPanel is the worker state sent to the dashboard.
type WorkerPanel struct {
	WorkerID  string  `json:"worker_id"`
	Alive     bool    `json:"alive"`
	Tasks     int64   `json:"tasks_done"`
	Failed    int64   `json:"tasks_failed"`
	P50Ms     float64 `json:"p50_ms"`
	P99Ms     float64 `json:"p99_ms"`
	Reachable bool    `json:"reachable"`
}

// hub manages WebSocket client connections. The clients map is owned exclusively
// by the hub.run() goroutine — no external goroutine may read or write it.
type hub struct {
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	broadcast  chan []byte
	clients    map[*websocket.Conn]bool
}

// Server manages the dashboard HTTP handlers and WebSocket broadcasting.
type Server struct {
	scraper         *scraper.Scraper
	metrics         *metrics.Metrics
	upgrader        websocket.Upgrader
	hub             *hub
	prevTasksTotal  int64
	prevThroughTime time.Time
	throughputMu    sync.Mutex
}

// NewServer creates a dashboard Server backed by the provided scraper and metrics.
func NewServer(s *scraper.Scraper, m *metrics.Metrics) *Server {
	return &Server{
		scraper:  s,
		metrics:  m,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins in dev mode
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		hub: &hub{
			register:   make(chan *websocket.Conn, 8),
			unregister: make(chan *websocket.Conn, 8),
			broadcast:  make(chan []byte, 256),
			clients:    make(map[*websocket.Conn]bool),
		},
		prevThroughTime: time.Now(),
	}
}

// RegisterRoutes attaches the dashboard HTTP handlers to the provided mux.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/api/state", s.HandleAPIState)
}

// Start starts the hub goroutine, subscribes to scraper updates, and runs
// a ping ticker. It blocks until ctx is cancelled.
func (s *Server) Start(ctx context.Context) {
	go s.hub.run(ctx, s)

	s.scraper.Subscribe(s.OnSnapshot)

	// Ping ticker keeps WebSocket connections alive
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[dashboard] Start loop stopped")
			return
		case <-ticker.C:
			pingMsg := WSMessage{
				Type:      "ping",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}
			data, err := json.Marshal(pingMsg)
			if err != nil {
				continue
			}
			select {
			case s.hub.broadcast <- data:
			default:
				// Drop if channel full
			}
		}
	}
}

// OnSnapshot is the callback invoked by the scraper when new metrics arrive.
// It converts the snapshot to dashboard format and sends it to the hub.
func (s *Server) OnSnapshot(snap scraper.ClusterSnapshot) {
	sd := s.buildSnapshotData(snap)
	msg := WSMessage{
		Type:      "snapshot",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      sd,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[dashboard] failed to marshal snapshot: %v", err)
		return
	}
	// Non-blocking send — drop if channel full (lag is acceptable, crash is not)
	select {
	case s.hub.broadcast <- data:
	default:
		log.Println("[dashboard] broadcast channel full, dropping snapshot")
	}
}

// buildSnapshotData converts a ClusterSnapshot into the dashboard-specific SnapshotData.
func (s *Server) buildSnapshotData(snap scraper.ClusterSnapshot) SnapshotData {
	coords := make([]CoordPanel, len(snap.Coordinators))
	for i, c := range snap.Coordinators {
		coords[i] = CoordPanel{
			NodeID:    c.NodeID,
			State:     c.State,
			Epoch:     c.Epoch,
			Reachable: c.Reachable,
		}
	}

	workers := make([]WorkerPanel, len(snap.Workers))
	var currentTasksTotal int64
	for i, w := range snap.Workers {
		workers[i] = WorkerPanel{
			WorkerID:  w.WorkerID,
			Alive:     w.Alive,
			Tasks:     w.TasksProcessed,
			Failed:    w.TasksFailed,
			P50Ms:     w.LatencyP50Ms,
			P99Ms:     w.LatencyP99Ms,
			Reachable: w.Reachable,
		}
		currentTasksTotal += w.TasksProcessed
	}

	// Calculate throughput per minute
	s.throughputMu.Lock()
	var throughputPM float64
	elapsed := time.Since(s.prevThroughTime).Minutes()
	if elapsed > 0 {
		delta := currentTasksTotal - s.prevTasksTotal
		if delta < 0 {
			delta = 0
		}
		throughputPM = float64(delta) / elapsed
	}
	s.prevTasksTotal = currentTasksTotal
	s.prevThroughTime = time.Now()
	s.throughputMu.Unlock()

	scrapeAge := time.Since(snap.ScrapedAt).Seconds()

	return SnapshotData{
		Coordinators:  coords,
		Workers:       workers,
		QueueDepth:    snap.TotalQueueDepth(),
		LiveWorkers:   snap.LiveWorkerCount(),
		TotalWorkers:  len(snap.Workers),
		ThroughputPM:  throughputPM,
		ScrapeAgeSecs: scrapeAge,
		Stale:         scrapeAge > 30,
	}
}

// run is the hub's main loop. It is the ONLY goroutine that reads or writes
// the clients map. Uses select with ctx.Done() for clean shutdown.
func (h *hub) run(ctx context.Context, srv *Server) {
	for {
		select {
		case <-ctx.Done():
			for conn := range h.clients {
				conn.Close()
			}
			log.Println("[hub] shutdown, closed all clients")
			return

		case conn := <-h.register:
			h.clients[conn] = true
			log.Printf("[hub] client registered, total: %d", len(h.clients))
			// Send latest snapshot immediately to new client
			go func() {
				snap := srv.scraper.Latest()
				sd := srv.buildSnapshotData(snap)
				msg := WSMessage{
					Type:      "snapshot",
					Timestamp: time.Now().UTC().Format(time.RFC3339),
					Data:      sd,
				}
				data, err := json.Marshal(msg)
				if err != nil {
					return
				}
				conn.WriteMessage(websocket.TextMessage, data)
			}()

		case conn := <-h.unregister:
			if _, ok := h.clients[conn]; ok {
				delete(h.clients, conn)
				conn.Close()
				log.Printf("[hub] client unregistered, total: %d", len(h.clients))
			}

		case msg := <-h.broadcast:
			for conn := range h.clients {
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					log.Printf("[hub] write error, removing client: %v", err)
					// Non-blocking send to unregister
					select {
					case h.unregister <- conn:
					default:
					}
				}
			}
		}
	}
}

// handleWebSocket upgrades the HTTP connection to WebSocket.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[dashboard] websocket upgrade failed: %v", err)
		return
	}

	s.hub.register <- conn
	s.metrics.WebSocketConnections.Inc()

	defer func() {
		s.hub.unregister <- conn
		s.metrics.WebSocketConnections.Dec()
	}()

	// Read pump — handles pong/close frames
	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("[dashboard] read error: %v", err)
			}
			break
		}
	}
}

// handleIndex serves the embedded dashboard HTML page.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "dashboard HTML not found", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

// HandleAPIState returns the current ClusterSnapshot as JSON for debugging.
func (s *Server) HandleAPIState(w http.ResponseWriter, r *http.Request) {
	snap := s.scraper.Latest()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap)
}
