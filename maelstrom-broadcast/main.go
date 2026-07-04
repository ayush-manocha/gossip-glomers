package main

import (
    "encoding/json"
    "log"
    "os"
    "sync"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()

    // Values that this node has seen
    values := make(map[int]struct{})

    // Neighboring nodes
    var neighbors []string

    var mu sync.Mutex

    // Handler for the "broadcast" message that stores the value and ACKs
    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body struct {
            Message int `json:"message"`
        }
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        // Add value to set of seen values
        mu.Lock()
        values[body.Message] = struct{}{}
        mu.Unlock()

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
            "type": "read_ok",
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

        _ = neighbors // TODO: used in 3b for gossip; putting this here to silence compiler unused error

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
