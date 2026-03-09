package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type ElectionMsg struct {
	SenderID int `json:"sender_id"`
}

func (n *Node) StartElection() {
	n.mu.Lock()
	n.LeaderID = -1 // Reset leader
	n.mu.Unlock()

	log.Printf("Starting Bully Election...")

	higherNodes := make([]int, 0)
	for id := range n.Peers {
		if id > n.ID {
			higherNodes = append(higherNodes, id)
		}
	}

	if len(higherNodes) == 0 {
		// I am the highest, so I am the leader
		n.becomeLeader()
		return
	}

	// Send an election message to all higher nodes
	gotOk := false
	msg := ElectionMsg{SenderID: n.ID}

	for _, id := range higherNodes {
		go func(peerID int) {
			_, err := n.sendRPC(peerID, "/election", msg)
			if err == nil {
				n.mu.Lock()
				gotOk = true
				n.mu.Unlock()
			}
		}(id)
	}

	// Wait a bit to see if we get any OKs
	time.Sleep(1500 * time.Millisecond)

	n.mu.Lock()
	defer n.mu.Unlock()

	if !gotOk {
		// No one responded, I am the leader
		go n.becomeLeader()
	} else {
		log.Printf("Received OK from higher node, waiting for coordinator message...")
	}
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	n.LeaderID = n.ID
	log.Printf("I am the new Leader (NameNode)")
	n.mu.Unlock()

	// Load existing fsimage if it exists
	n.loadFSImage()

	msg := ElectionMsg{SenderID: n.ID}
	for id := range n.Peers {
		if id < n.ID {
			go n.sendRPC(id, "/coordinator", msg)
		}
	}
}

// HTTP handler for an incoming /election request
func (n *Node) handleElection(w http.ResponseWriter, r *http.Request) {
	var msg ElectionMsg
	_ = json.NewDecoder(r.Body).Decode(&msg)

	log.Printf("Received ELECTION from Node %d", msg.SenderID)

	// Since we got this, our ID is higher. We reply OK (200 status)
	w.WriteHeader(http.StatusOK)

	// And we must start our own election to take over
	go n.StartElection()
}

// HTTP handler for an incoming /coordinator request
func (n *Node) handleCoordinator(w http.ResponseWriter, r *http.Request) {
	var msg ElectionMsg
	_ = json.NewDecoder(r.Body).Decode(&msg)

	n.mu.Lock()
	n.LeaderID = msg.SenderID
	log.Printf("Acknowledged Node %d as the new Leader (NameNode)", msg.SenderID)
	n.mu.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (n *Node) handlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Ping the leader periodically to see if it is still alive
func (n *Node) monitorLeader() {
	ticker := time.NewTicker(3 * time.Second)
	for range ticker.C {
		n.mu.Lock()
		leader := n.LeaderID
		n.mu.Unlock()

		if leader == -1 || leader == n.ID {
			continue // I am leader or election in progress
		}

		log.Printf("Sending 3-second heartbeat ping to Leader %d...", leader)
		_, err := n.sendRPC(leader, "/ping", nil)
		if err != nil {
			log.Printf("Leader %d unresponsive! Triggering election...", leader)
			n.StartElection()
		}
	}
}
