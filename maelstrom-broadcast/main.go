package main

import (
	"encoding/json"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Create tree topology with 5 children per node
func setNeighbors(id string, nodeIDs []string) ([]string, map[string]map[int]struct{}) {
	i := slices.Index(nodeIDs, id)
	n := len(nodeIDs)

	nborIdxs := []int{5*i + 1, 5*i + 2, 5*i + 3, 5*i + 4, 5*i + 5, (i - 1) / 5}
	var neighbors []string

	for _, v := range nborIdxs {
		if v >= 0 && v < n && v != i {
			neighbors = append(neighbors, nodeIDs[v])
		}
	}

	unacked := make(map[string]map[int]struct{})
	for _, nbor := range neighbors {
		unacked[nbor] = make(map[int]struct{})
	}

	return neighbors, unacked
}

func main() {
	n := maelstrom.NewNode()

	// Values that this node has seen
	values := make(map[int]struct{})

	// Neighboring nodes
	var neighbors []string

	// Messages per neighbor that have not yet been acked
	var unacked map[string]map[int]struct{}

	var mu sync.Mutex

	// Initialize neighbors and unacked message tracker
	n.Handle("init", func(msg maelstrom.Message) error {
		mu.Lock()
		neighbors, unacked = setNeighbors(n.ID(), n.NodeIDs())
		mu.Unlock()
		return nil
	})

	// Helper to gossip to a neighboring node
	gossip := func(dest string, msgs []int) {
		body := map[string]any{
			"type":     "gossip",
			"messages": msgs,
		}
		if err := n.RPC(dest, body, func(reply maelstrom.Message) error {
			// Remove messages from unacked upon reply
			mu.Lock()
			for _, v := range msgs {
				delete(unacked[dest], v)
			}
			mu.Unlock()
			return nil
		}); err != nil {
			log.Printf("gossip to %s failed: %s", dest, err)
		}
	}

	// Handler for the "broadcast" message from a client that stores the value and ACKs
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body struct {
			Message int `json:"message"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// If I've already seen this value, then I've forwarded it and can ignore it
		mu.Lock()
		_, seen := values[body.Message]
		// Note down unseen value as unacked by all neighbors
		if !seen {
			values[body.Message] = struct{}{}
			for _, nbor := range neighbors {
				if unacked[nbor] == nil {
					unacked[nbor] = make(map[int]struct{})
				}
				unacked[nbor][body.Message] = struct{}{}
			}
		}
		mu.Unlock()

		res := map[string]any{
			"type": "broadcast_ok",
		}

		return n.Reply(msg, res)
	})

	// Handler for the "gossip" message from another node that stores a batch of values and ACKs
	n.Handle("gossip", func(msg maelstrom.Message) error {
		var body struct {
			Messages []int `json:"messages"`
		}
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// For each of the values, check if I've already seen it
		// If so, then I've forwarded it and can ignore it
		mu.Lock()
		for _, v := range body.Messages {
			_, seen := values[v]
			// Note down unseen value as unacked by all neighbors
			if !seen {
				values[v] = struct{}{}
				for _, nbor := range neighbors {
					if nbor == msg.Src {
						continue
					}
					if unacked[nbor] == nil {
						unacked[nbor] = make(map[int]struct{})
					}
					unacked[nbor][v] = struct{}{}
				}
			}
		}
		mu.Unlock()

		res := map[string]any{
			"type": "gossip_ok",
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

	// Handler for the "topology" message that ignores topology suggestion and ACKs
	n.Handle("topology", func(msg maelstrom.Message) error {
		res := map[string]any{
			"type": "topology_ok",
		}

		return n.Reply(msg, res)
	})

	// At a regular interval, send each neighbor all unacked messages
	go func() {
		for range time.Tick(200 * time.Millisecond) {
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
				if len(vals) > 0 {
					gossip(nbor, vals)
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
