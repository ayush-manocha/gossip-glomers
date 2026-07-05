package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
    "strings"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Values that this node has seen
	values := make(map[int]struct{})

	// Neighboring nodes
	var neighbors []string

	var mu sync.Mutex

	// Helper to gossip to a neighboring node
	gossip := func(dest string, message int) {
		body := map[string]any{
			"type":    "broadcast",
			"message": message,
		}
		if err := n.Send(dest, body); err != nil {
			log.Printf("gossip to %s failed: %s", dest, err)
		}
	}

	// Handler for the "broadcast" message that stores the value and ACKs
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// If I've already seen this value, then I've forwarded it and can ignore it
		// If not, store and forward it
		mu.Lock()
		_, seen := values[body.Message]
		var nbrs []string
		if !seen {
			values[body.Message] = struct{}{}
			nbrs = neighbors
		}
		mu.Unlock()
		for _, nbor := range nbrs {
			gossip(nbor, body.Message)
		}

        // Don't need to ack node-to-node gossip
        if strings.HasPrefix(msg.Src, "n") {
            return nil
        }

		res := map[string]any{
			"type": "broadcast_ok",
		}

		return n.Reply(msg, res)
	})

	// Handler for the "read" message that responds with all seen values
	n.Handle("read", func(msg maelstrom.Message) error {
		// Get all seen values as a list
		mu.Lock()
		valLst := make([]int, 0, len(values))
		for val := range values {
			valLst = append(valLst, val)
		}
		mu.Unlock()

		body := map[string]any{
			"type":     "read_ok",
			"messages": valLst,
		}

		return n.Reply(msg, body)
	})

	// Handler for the "topology" message that stores node neighbors and ACKs
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body struct {
			Topology map[string][]string `json:"topology"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Get my neighbors
		mu.Lock()
		neighbors = body.Topology[n.ID()]
		mu.Unlock()

		res := map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, res)
	})

	// Execute the node's message loop, running until STDIN is closed
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
