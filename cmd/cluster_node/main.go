package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

func main() {
	id := flag.Int("id", 1, "Node ID (1-4)")
	configFile := flag.String("config", "config.json", "Path to config file")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	// Setup logging prefixed by Node ID
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("[Node %d] ", *id))

	log.Printf("Starting Cluster Node %d...", *id)

	// Load configuration
	data, err := os.ReadFile(*configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var peers []Peer
	if err := json.Unmarshal(data, &peers); err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

	// Initialize the Node
	node := NewNode(*id, peers)

	// Start the HTTP server in a goroutine
	go node.StartServer()

	// Wait a moment for all nodes to potentially start, then initiate election
	time.Sleep(2 * time.Second)
	go node.StartElection()

	// Start interactive CLI blocking the main thread
	node.StartCLI()
}
