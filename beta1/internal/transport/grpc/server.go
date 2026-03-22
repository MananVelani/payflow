package grpctransport

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type Server struct {
	grpc     *grpc.Server
	health   *health.Server
	logger   *zap.Logger
	listener net.Listener
}

func NewServer(logger *zap.Logger) (*Server, error) {
	s := &Server{logger: logger}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.loggingInterceptor, s.recoveryInterceptor),
		grpc.ChainStreamInterceptor(s.streamLoggingInterceptor, s.streamRecoveryInterceptor),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Second,
			Time:              5 * time.Second,
			Timeout:           1 * time.Second,
		}),
	}
	s.grpc = grpc.NewServer(opts...)
	s.health = health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.grpc, s.health)
	reflection.Register(s.grpc) // enable grpcurl in dev
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

func (s *Server) loggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	s.logger.Info("gRPC unary call", zap.String("method", info.FullMethod), zap.Duration("duration", time.Since(start)), zap.Error(err))
	return resp, err
}

func (s *Server) recoveryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in gRPC handler", zap.String("method", info.FullMethod), zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			err = status.Errorf(codes.Internal, "internal server error")
		}
	}()
	return handler(ctx, req)
}

func (s *Server) streamLoggingInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	s.logger.Info("gRPC stream started", zap.String("method", info.FullMethod))
	err := handler(srv, ss)
	s.logger.Info("gRPC stream ended", zap.String("method", info.FullMethod), zap.Duration("duration", time.Since(start)), zap.Error(err))
	return err
}

func (s *Server) streamRecoveryInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("panic in gRPC stream", zap.String("method", info.FullMethod), zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			err = status.Errorf(codes.Internal, "internal server error")
		}
	}()
	return handler(srv, ss)
}
