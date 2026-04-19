package stream

import (
	"time"

	"google.golang.org/grpc/keepalive"
)

// ClientParams returns keepalive settings for long-lived gRPC streams.
// Prevents "connection reset by peer" during idle periods and ensures
// rapid detection of network partitions.
func ClientParams() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10s if there's no activity
		Timeout:             5 * time.Second,  // wait 5s for ping ack before closing connection
		PermitWithoutStream: true,            // send pings even without active streams
	}
}
