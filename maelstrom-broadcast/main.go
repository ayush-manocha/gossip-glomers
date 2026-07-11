package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// Values that this node has seen
	values := make(map[int]struct{})

	// Neighboring nodes
	var neighbors []string

	// Messages per neighbor that have not yet been acked
	unacked := make(map[string]map[int]struct{})

	var mu sync.Mutex

	// Helper to gossip to a neighboring node
	gossip := func(dest string, message int) {
		body := map[string]any{
			"type":    "broadcast",
			"message": message,
		}
		if err := n.RPC(dest, body, func(reply maelstrom.Message) error {
			// Remove message from unacked upon reply
			mu.Lock()
			delete(unacked[dest], message)
			mu.Unlock()
			return nil
		}); err != nil {
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
			for _, nbor := range nbrs {
				if unacked[nbor] == nil {
					unacked[nbor] = make(map[int]struct{})
				}
				unacked[nbor][body.Message] = struct{}{}
			}
		}
		mu.Unlock()
		for _, nbor := range nbrs {
			gossip(nbor, body.Message)
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
		for _, nbor := range neighbors {
			unacked[nbor] = make(map[int]struct{})
		}
		mu.Unlock()

		res := map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, res)
	})

	// At a regular interval, resend all unacked messages
	go func() {
		for range time.Tick(500 * time.Millisecond) {
			mu.Lock()
			snapshot := make(map[string][]int, len(unacked))
			for nbor, owed := range unacked {
				vals := make([]int, 0, len(owed))
				for v := range owed {
					vals = append(vals, v)
				}
				snapshot[nbor] = vals
			}
			mu.Unlock()

			for nbor, vals := range snapshot {
				for _, v := range vals {
					gossip(nbor, v)
				}
			}
		}
	}()

	// Execute the node's message loop, running until STDIN is closed
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
