package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Register a handler for the "echo" message that responds with an "echo_ok"
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as a loosely-typed map
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type
		return n.Reply(msg, body)
	})

	// Execute the node's message loop, running until STDIN is closed
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
