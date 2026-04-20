package interceptor

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	apperrors "github.com/your-org/payflow/worker/internal/errors"
	"github.com/your-org/payflow/worker/internal/version"
)

// UnaryServerVersion validates the contract version for unary calls.
func UnaryServerVersion(logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			logger.Warn("missing metadata in gRPC call")
			return handler(ctx, req)
		}

		versions := md.Get("x-contract-version")
		if len(versions) == 0 {
			logger.Warn("missing contract version header from peer")
			return handler(ctx, req)
		}

		clientVersion := versions[0]
		if clientVersion != version.ContractVersion {
			contractErr := fmt.Errorf("contract version mismatch: expected %s got %s: %w", version.ContractVersion, clientVersion, apperrors.ErrContractVersion)
			logger.Error("contract version mismatch",
				zap.String("expected", version.ContractVersion),
				zap.String("got", clientVersion),
				zap.String("method", info.FullMethod),
				zap.Error(contractErr))
			return nil, status.Error(codes.FailedPrecondition, contractErr.Error())
		}

		return handler(ctx, req)
	}
}

// StreamServerVersion validates the contract version for streaming calls.
func StreamServerVersion(logger *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		md, ok := metadata.FromIncomingContext(ss.Context())
		if !ok {
			logger.Warn("missing metadata in gRPC stream")
			return handler(srv, ss)
		}

		versions := md.Get("x-contract-version")
		if len(versions) == 0 {
			logger.Warn("missing contract version header from peer")
			return handler(srv, ss)
		}

		clientVersion := versions[0]
		if clientVersion != version.ContractVersion {
			contractErr := fmt.Errorf("contract version mismatch: expected %s got %s: %w", version.ContractVersion, clientVersion, apperrors.ErrContractVersion)
			logger.Error("contract version mismatch",
				zap.String("expected", version.ContractVersion),
				zap.String("got", clientVersion),
				zap.String("method", info.FullMethod),
				zap.Error(contractErr))
			return status.Error(codes.FailedPrecondition, contractErr.Error())
		}

		return handler(srv, ss)
	}
}
