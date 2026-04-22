// Command chaos is the PayFlow fault injection CLI.
// It provides subcommands to kill containers, inject network latency,
// and simulate network partitions for testing distributed fault tolerance.
package main

import (
	"log"

	"github.com/payflow/chaos/cmd"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if err := cmd.Execute(); err != nil {
		log.Fatalf("chaos CLI error: %v", err)
	}
}
