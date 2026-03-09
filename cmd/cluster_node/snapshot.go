package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

type SnapshotMarkerReq struct {
	InitiatorID int `json:"initiator_id"`
}

type SnapshotMarkerResp struct {
	NodeID      int      `json:"node_id"`
	LocalChunks []string `json:"local_chunks"`
}

// CLI command to trigger snapshot
func (n *Node) triggerSnapshot() {
	n.mu.Lock()
	if n.LeaderID != n.ID {
		n.mu.Unlock()
		log.Println("Only the Leader (NameNode) can initiate a Global Snapshot.")
		return
	}
	n.SnapshotRunning = true
	var globalState SnapshotData
	globalState.NodeID = n.ID
	globalState.IsLeader = true

	// Copy NameNode state
	globalState.GlobalFileState = make(map[string]FileRecord)
	for k, v := range n.FileMetadata {
		globalState.GlobalFileState[k] = v
	}
	
	n.mu.Unlock()

	log.Println("[Snapshot] Initiating Distributed Snapshot...")

	// Get local DataNode state (if the leader is also storing blocks)
	localChunks := n.getLocalChunks()
	globalState.LocalChunks = localChunks

	// Send marker to all DataNodes
	req := SnapshotMarkerReq{InitiatorID: n.ID}
	
	results := make(chan SnapshotMarkerResp, len(n.Peers))

	for id := range n.Peers {
		go func(peerID int) {
			respData, err := n.sendRPC(peerID, "/snapshot_marker", req)
			if err != nil {
				log.Printf("[Snapshot] Node %d failed to respond: %v", peerID, err)
				results <- SnapshotMarkerResp{NodeID: peerID}
			} else {
				var resp SnapshotMarkerResp
				json.Unmarshal(respData, &resp)
				results <- resp
			}
		}(id)
	}

	// Collect responses
	collectedStates := []SnapshotMarkerResp{
		{NodeID: n.ID, LocalChunks: localChunks},
	}
	for i := 0; i < len(n.Peers); i++ {
		res := <-results
		collectedStates = append(collectedStates, res)
	}

	n.mu.Lock()
	n.SnapshotRunning = false
	n.mu.Unlock()

	// Display Snapshot
	fmt.Println("\n================ GLOBAL SNAPSHOT ================")
	fmt.Println("=== NAMENODE FILE METADATA ===")
	str, _ := json.MarshalIndent(globalState.GlobalFileState, "", "  ")
	fmt.Println(string(str))

	fmt.Println("\n=== DATANODE STORAGE STATE ===")
	for _, st := range collectedStates {
		fmt.Printf("Node %d: %v chunks\n", st.NodeID, st.LocalChunks)
	}
	fmt.Println("=================================================")
}

// DataNode HTTP handler for receiving a snapshot marker
func (n *Node) handleSnapshotMarker(w http.ResponseWriter, r *http.Request) {
	var req SnapshotMarkerReq
	_ = json.NewDecoder(r.Body).Decode(&req)

	log.Printf("[Snapshot] Received MARKER from Node %d. Capturing local state...", req.InitiatorID)

	localChunks := n.getLocalChunks()
	resp := SnapshotMarkerResp{
		NodeID:      n.ID,
		LocalChunks: localChunks,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func (n *Node) getLocalChunks() []string {
	var chunks []string
	entries, err := os.ReadDir(n.StorageDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() && e.Name() != ".DS_Store" {
				chunks = append(chunks, e.Name())
			}
		}
	}
	return chunks
}
