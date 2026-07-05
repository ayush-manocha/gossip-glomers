package main

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Keep a local node counter
	var counter atomic.Int64

	// Register a handler for the "generate" message that replies with "generate_ok" + unique ID
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unique id is "{node_id}-{local counter}"
		id := fmt.Sprintf("%s-%d", n.ID(), counter.Add(1))

		body := map[string]any{
			"type": "generate_ok",
			"id":   id,
		}

		return n.Reply(msg, body)
	})

	// Execute the node's message loop, running until STDIN is closed
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
