package grpctransport

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/your-org/payflow/worker/internal/interceptor"
	"github.com/your-org/payflow/worker/internal/service"
	pb "github.com/your-org/payflow/worker/proto/worker"
)

type Server struct {
	grpc     *grpc.Server
	health   *health.Server
	logger   *zap.Logger
	listener net.Listener
	service  service.WorkerService
}

func NewServer(svc service.WorkerService, logger *zap.Logger, serverOptions ...grpc.ServerOption) (*Server, error) {
	s := &Server{
		logger:  logger,
		service: svc,
	}
	
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			interceptor.UnaryServerVersion(logger),
		),
		grpc.ChainStreamInterceptor(
			interceptor.StreamServerVersion(logger),
		),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Second,
			Time:              5 * time.Second,
			Timeout:           1 * time.Second,
		}),
	}
	opts = append(opts, serverOptions...)
	
	s.grpc = grpc.NewServer(opts...)
	s.health = health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.grpc, s.health)

	// Register Worker service (specifically for RevokeTask)
	pb.RegisterWorkerManagementServer(s.grpc, &WorkerGRPCService{
		workerService: svc,
		logger:        logger,
	})

	reflection.Register(s.grpc)
	return s, nil
}

func (s *Server) Listen(port int) (int, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return 0, fmt.Errorf("listen :%d: %w", port, err)
	}
	s.listener = lis
	return lis.Addr().(*net.TCPAddr).Port, nil
}

func (s *Server) Serve() error {
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	s.logger.Info("gRPC server starting", zap.String("addr", s.listener.Addr().String()))
	return s.grpc.Serve(s.listener)
}

func (s *Server) GRPCServer() *grpc.Server { return s.grpc }

func (s *Server) Stop() {
	s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	s.logger.Info("gRPC server shutting down gracefully")
	stopped := make(chan struct{})
	go func() { s.grpc.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
		s.logger.Info("gRPC server stopped cleanly")
	case <-time.After(5 * time.Second):
		s.logger.Warn("graceful stop timed out — forcing stop")
		s.grpc.Stop()
	}
}

// WorkerGRPCService implements the server-side of worker.proto (RevokeTask).
type WorkerGRPCService struct {
	pb.UnimplementedWorkerManagementServer
	workerService service.WorkerService
	logger        *zap.Logger
}

func (s *WorkerGRPCService) RevokeTask(ctx context.Context, req *pb.RevokeRequest) (*pb.RevokeAck, error) {
	s.logger.Warn("received RevokeTask request from C2", zap.String("task_id", req.TaskId))
	err := s.workerService.RevokeTask(ctx, req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "revoke failed: %v", err)
	}
	return &pb.RevokeAck{Acknowledged: true}, nil
}
