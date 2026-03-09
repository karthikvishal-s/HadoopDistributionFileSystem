package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Peer struct {
	ID      int    `json:"id"`
	Address string `json:"address"`
}

type Node struct {
	ID        int
	Address   string
	Peers     map[int]Peer
	LeaderID  int
	mu        sync.Mutex
	client    *http.Client
	serverMux *http.ServeMux

	// HDFS State
	StorageDir   string
	FileMetadata map[string]FileRecord // Only NameNode uses this

	// Snapshot State
	SnapshotRunning bool
	SnapshotState   *SnapshotData
}

type FileRecord struct {
	FileName string
	Size     int64
	Chunks   []ChunkRecord
}

type ChunkRecord struct {
	ChunkID   string
	DataNodes []int
}

type SnapshotData struct {
	NodeID          int
	IsLeader        bool
	LocalChunks     []string
	GlobalFileState map[string]FileRecord
}

func NewNode(id int, peerList []Peer) *Node {
	peers := make(map[int]Peer)
	var myAddress string

	for _, p := range peerList {
		if p.ID == id {
			myAddress = p.Address
		} else {
			peers[p.ID] = p
		}
	}

	storageDir := fmt.Sprintf("./storage_node_%d", id)
	os.MkdirAll(storageDir, 0755)

	n := &Node{
		ID:           id,
		Address:      myAddress,
		Peers:        peers,
		LeaderID:     -1, // -1 means unknown
		client:       &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
			},
		},
		serverMux:    http.NewServeMux(),
		StorageDir:   storageDir,
		FileMetadata: make(map[string]FileRecord),
	}

	n.setupRoutes()
	return n
}

func (n *Node) setupRoutes() {
	// Bully Algorithm endpoints
	n.serverMux.HandleFunc("/election", n.handleElection)
	n.serverMux.HandleFunc("/coordinator", n.handleCoordinator)
	n.serverMux.HandleFunc("/ping", n.handlePing)

	// HDFS endpoints
	n.serverMux.HandleFunc("/upload_req", n.handleUploadRequest)
	n.serverMux.HandleFunc("/receive_chunk", n.handleReceiveChunk)
	n.serverMux.HandleFunc("/read_req", n.handleReadRequest)
	n.serverMux.HandleFunc("/read_chunk", n.handleReadChunk)

	// Snapshot endpoints
	n.serverMux.HandleFunc("/snapshot_marker", n.handleSnapshotMarker)
}

func (n *Node) StartServer() {
	port := strings.Split(n.Address, ":")[2]
	log.Printf("HTTP Server listening on port %s", port)
	if err := http.ListenAndServe(":"+port, n.serverMux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

// sendRPC is a helper to send JSON data over HTTP
func (n *Node) sendRPC(peerID int, endpoint string, payload interface{}) ([]byte, error) {
	peer, exists := n.Peers[peerID]
	if !exists {
		return nil, fmt.Errorf("peer %d not found", peerID)
	}

	var reqBody io.Reader
	if payload != nil {
		data, _ := json.Marshal(payload)
		reqBody = bytes.NewBuffer(data)
	}

	url := fmt.Sprintf("%s%s", peer.Address, endpoint)
	req, _ := http.NewRequest("POST", url, reqBody)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (n *Node) StartCLI() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("\n==============================================")
	fmt.Printf("Node %d Interactive Shell\n", n.ID)
	fmt.Println("Commands:")
	fmt.Println("  status      - Show node status and current leader")
	fmt.Println("  upload <f>  - Upload a local file to HDFS")
	fmt.Println("  read <f>    - Read a file from HDFS")
	fmt.Println("  snapshot    - Trigger a distributed snapshot")
	fmt.Println("  kill        - Simulate node crash (exits program)")
	fmt.Println("==============================================")

	for {
		fmt.Printf("node-%d> ", n.ID)
		if !scanner.Scan() {
			break
		}
		cmd := strings.TrimSpace(scanner.Text())
		if cmd == "" {
			continue
		}

		parts := strings.Split(cmd, " ")
		switch parts[0] {
		case "status":
			n.mu.Lock()
			leader := n.LeaderID
			n.mu.Unlock()
			role := "Follower/DataNode"
			if leader == n.ID {
				role = "Leader/NameNode"
			}
			fmt.Printf("Status: ID=%d, Role=%s, LeaderID=%d\n", n.ID, role, leader)
		case "upload":
			if len(parts) < 2 {
				fmt.Println("Usage: upload <filename>")
				continue
			}
			n.uploadFile(parts[1])
		case "read":
			if len(parts) < 2 {
				fmt.Println("Usage: read <filename>")
				continue
			}
			n.readFile(parts[1])
		case "snapshot":
			n.triggerSnapshot()
		case "kill":
			log.Println("Simulating node crash. Exiting...")
			os.Exit(0)
		default:
			fmt.Println("Unknown command")
		}
	}
}
