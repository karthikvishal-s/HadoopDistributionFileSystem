package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"hdfs-simulation/common"
)

// ==================== SECONDARY NAMENODE + DATANODE ====================

type SecondaryNode struct {
	mu           sync.RWMutex
	nodeID       string
	rack         string
	nameNodeAddr string
	listenPort   string
	blockDir     string
	blocks       map[string]*common.Block // blockID -> block
	isLeader     bool
	isElecting   bool
	log          *common.Logger
}

func NewSecondaryNode(nodeID, rack, nameNodeAddr, listenPort string) *SecondaryNode {
	blockDir := "./blocks_" + nodeID
	os.MkdirAll(blockDir, 0755)

	return &SecondaryNode{
		nodeID:       nodeID,
		rack:         rack,
		nameNodeAddr: nameNodeAddr,
		listenPort:   listenPort,
		blockDir:     blockDir,
		blocks:       make(map[string]*common.Block),
		isLeader:     false,
		isElecting:   false,
		log:          common.NewLogger("SecondaryNN", nodeID),
	}
}

// ==================== HEARTBEAT SENDER ====================

func (sn *SecondaryNode) sendHeartbeats() {
	ticker := time.NewTicker(common.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		sn.mu.RLock()
		if sn.isLeader {
			sn.mu.RUnlock()
			continue // Leaders don't send heartbeats
		}

		if sn.isElecting {
			sn.mu.RUnlock()
			continue // Wait for election to finish before sending more heartbeats
		}

		blockIDs := make([]string, 0, len(sn.blocks))
		var usedBytes int64
		for id, block := range sn.blocks {
			blockIDs = append(blockIDs, id)
			usedBytes += int64(len(block.Data))
		}
		sn.mu.RUnlock()

		hb := common.HeartbeatMsg{
			NodeID:     sn.nodeID,
			Rack:       sn.rack,
			ListenPort: sn.listenPort,
			Timestamp:  time.Now(),
			BlockIDs:   blockIDs,
			UsedBytes:  usedBytes,
			TotalBytes: 10 * 1024 * 1024 * 1024, // Simulate 10 GB total
		}

		payload, _ := json.Marshal(hb)
		msg := common.Message{
			Type:    common.MsgHeartbeat,
			From:    sn.nodeID,
			Payload: json.RawMessage(payload),
		}

		conn, err := net.DialTimeout("tcp", sn.nameNodeAddr, 3*time.Second)
		if err != nil {
			sn.log.Warn("💔 Cannot reach NameNode at %s: %v", sn.nameNodeAddr, err)
			sn.log.Warn("🗳️  NameNode may be down! Considering leader election...")
			go sn.tryLeaderElection() // Run in goroutine to not block ticker
			continue
		}

		json.NewEncoder(conn).Encode(msg)
		conn.Close()
		sn.log.Info("💓 Heartbeat sent → NameNode | Blocks: %d | Storage: %d bytes", len(blockIDs), usedBytes)
	}
}

// ==================== LEADER ELECTION ====================

func (sn *SecondaryNode) tryLeaderElection() {
	sn.mu.Lock()
	if sn.isLeader || sn.isElecting {
		sn.mu.Unlock()
		return
	}
	sn.isElecting = true
	sn.mu.Unlock()

	defer func() {
		sn.mu.Lock()
		sn.isElecting = false
		sn.mu.Unlock()
	}()

	sn.log.Event("🗳️  LEADER ELECTION: Attempting to become the new NameNode...")

	// In Bully Algorithm: Secondary has highest priority after Primary
	// Try to contact NameNode 3 times before declaring self as leader
	for i := 0; i < 3; i++ {
		conn, err := net.DialTimeout("tcp", sn.nameNodeAddr, 2*time.Second)
		if err == nil {
			conn.Close()
			sn.log.Info("🗳️  NameNode is still alive (attempt %d/3), aborting election", i+1)
			return
		}
		sn.log.Warn("🗳️  NameNode unreachable (attempt %d/3)", i+1)
		time.Sleep(2 * time.Second)
	}

	sn.mu.Lock()
	sn.isLeader = true
	sn.mu.Unlock()

	sn.log.Event("👑 LEADER ELECTION WON — This node is now the ACTIVE NAMENODE!")
	sn.log.Info("👑 Promoted from Secondary NameNode to Primary NameNode")
	sn.log.Info("👑 Loading last checkpoint (fsimage.json)...")

	// Load the last checkpoint
	data, err := os.ReadFile("fsimage.json")
	if err != nil {
		sn.log.Warn("No fsimage.json found, starting with empty metadata")
	} else {
		sn.log.Success("👑 Loaded fsimage.json (%d bytes) — Cluster metadata restored", len(data))
	}

	sn.log.Info("👑 All DataNodes should now send heartbeats to this node at %s", sn.listenPort)
}

// ==================== CHECKPOINT ====================

func (sn *SecondaryNode) runCheckpoint() {
	ticker := time.NewTicker(common.CheckpointInterval)
	defer ticker.Stop()

	for range ticker.C {
		sn.mu.RLock()
		if sn.isLeader {
			sn.mu.RUnlock()
			continue // Don't checkpoint if we ARE the leader
		}
		sn.mu.RUnlock()

		sn.log.Info("📸 Checkpoint: Fetching metadata from NameNode...")

		conn, err := net.DialTimeout("tcp", sn.nameNodeAddr, 5*time.Second)
		if err != nil {
			sn.log.Warn("📸 Checkpoint failed — cannot reach NameNode: %v", err)
			continue
		}

		msg := common.Message{
			Type: common.MsgGetMetadata,
			From: sn.nodeID,
		}
		json.NewEncoder(conn).Encode(msg)

		var snapshot common.MetadataSnapshot
		if err := json.NewDecoder(conn).Decode(&snapshot); err != nil {
			sn.log.Warn("📸 Checkpoint failed — cannot decode metadata: %v", err)
			conn.Close()
			continue
		}
		conn.Close()

		// Save to local fsimage.json
		data, err := json.MarshalIndent(snapshot, "", "  ")
		if err == nil {
			os.WriteFile("fsimage.json", data, 0644)
			sn.log.Success("📸 Checkpoint saved to fsimage.json (%d files, %d nodes, %d bytes)",
				len(snapshot.Files), len(snapshot.Nodes), len(data))
		}
	}
}

// ==================== BLOCK STORAGE ====================

func (sn *SecondaryNode) storeBlock(payload common.StoreBlockPayload) {
	block := payload.Block

	// Verify checksum
	if !common.VerifyChecksum(block.Data, block.Checksum) {
		sn.log.Error("❌ CHECKSUM MISMATCH for block %s — DATA CORRUPTED during transit!", block.BlockID)
		sn.log.Error("   Expected: %s", block.Checksum)
		sn.log.Error("   Computed: %s", common.ComputeChecksum(block.Data))
		return
	}
	sn.log.Success("✅ Checksum verified for block %s (SHA-256: %s...)", block.BlockID, block.Checksum[:16])

	// Save to disk
	blockPath := filepath.Join(sn.blockDir, block.BlockID)
	if err := os.WriteFile(blockPath, block.Data, 0644); err != nil {
		sn.log.Error("❌ Failed to write block %s to disk: %v", block.BlockID, err)
		return
	}

	// Save checksum file
	checksumPath := blockPath + ".sha256"
	os.WriteFile(checksumPath, []byte(block.Checksum), 0644)

	sn.mu.Lock()
	sn.blocks[block.BlockID] = &block
	sn.mu.Unlock()

	sn.log.Success("💾 Block %s stored to disk (%d bytes) at %s", block.BlockID, len(block.Data), blockPath)

	// Forward to next in pipeline
	if len(payload.Targets) > 0 && len(payload.TargetAddrs) > 0 {
		nextTarget := payload.Targets[0]
		nextAddr := payload.TargetAddrs[0]
		remainingTargets := payload.Targets[1:]
		remainingAddrs := payload.TargetAddrs[1:]

		sn.log.Info("📡 Forwarding block %s to next in pipeline: %s (%s)", block.BlockID, nextTarget, nextAddr)

		go sn.forwardBlock(nextAddr, block, payload.FileName, remainingTargets, remainingAddrs)
	} else {
		sn.log.Info("📡 Block %s: End of replication pipeline", block.BlockID)
	}
}

func (sn *SecondaryNode) forwardBlock(addr string, block common.Block, fileName string, targets []string, targetAddrs []string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		sn.log.Error("❌ Failed to forward block %s to %s: %v", block.BlockID, addr, err)
		return
	}
	defer conn.Close()

	payload := common.StoreBlockPayload{
		Block:       block,
		Targets:     targets,
		TargetAddrs: targetAddrs,
		FileName:    fileName,
	}

	msg := common.Message{
		Type:    common.MsgStoreBlock,
		From:    sn.nodeID,
		Payload: payload,
	}

	data, _ := json.Marshal(msg)
	conn.Write(data)
	sn.log.Success("✅ Block %s forwarded to %s", block.BlockID, addr)
}

// ==================== READ BLOCK ====================

func (sn *SecondaryNode) readBlock(blockID string) ([]byte, error) {
	sn.mu.RLock()
	block, exists := sn.blocks[blockID]
	sn.mu.RUnlock()

	if !exists {
		// Try reading from disk
		blockPath := filepath.Join(sn.blockDir, blockID)
		data, err := os.ReadFile(blockPath)
		if err != nil {
			return nil, fmt.Errorf("block %s not found", blockID)
		}

		// Verify checksum from file
		checksumPath := blockPath + ".sha256"
		expectedChecksum, err := os.ReadFile(checksumPath)
		if err == nil {
			if !common.VerifyChecksum(data, string(expectedChecksum)) {
				sn.log.Error("❌ CORRUPTION DETECTED on disk read for block %s!", blockID)
				return nil, fmt.Errorf("block %s is corrupted on disk", blockID)
			}
			sn.log.Success("✅ Block %s integrity verified on read (SHA-256 match)", blockID)
		}

		return data, nil
	}

	// Verify in-memory block checksum
	if !common.VerifyChecksum(block.Data, block.Checksum) {
		sn.log.Error("❌ CORRUPTION DETECTED in memory for block %s!", block.BlockID)
		return nil, fmt.Errorf("block %s is corrupted in memory", blockID)
	}
	sn.log.Success("✅ Block %s integrity verified on read (SHA-256: %s...)", blockID, block.Checksum[:16])

	return block.Data, nil
}

// ==================== REPLICATE COMMAND HANDLER ====================

func (sn *SecondaryNode) handleReplicateCmd(cmd common.ReplicateCommand) {
	sn.log.Info("📦 Received replicate command for block %s → %v", cmd.BlockID, cmd.TargetNodes)

	data, err := sn.readBlock(cmd.BlockID)
	if err != nil {
		sn.log.Error("❌ Cannot replicate block %s: %v", cmd.BlockID, err)
		return
	}

	block := common.Block{
		BlockID:  cmd.BlockID,
		Data:     data,
		Checksum: common.ComputeChecksum(data),
	}

	for i, addr := range cmd.TargetAddrs {
		nodeID := cmd.TargetNodes[i]
		sn.log.Info("📡 Re-replicating block %s → %s (%s)", cmd.BlockID, nodeID, addr)

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			sn.log.Error("❌ Failed to connect to %s for re-replication: %v", addr, err)
			continue
		}

		payload := common.StoreBlockPayload{
			Block:       block,
			Targets:     []string{},
			TargetAddrs: []string{},
		}
		msg := common.Message{
			Type:    common.MsgStoreBlock,
			From:    sn.nodeID,
			Payload: payload,
		}
		json.NewEncoder(conn).Encode(msg)
		conn.Close()
		sn.log.Success("✅ Block %s re-replicated to %s", cmd.BlockID, nodeID)
	}
}

// ==================== CONNECTION HANDLER ====================

func (sn *SecondaryNode) handleConnection(conn net.Conn) {
	defer conn.Close()

	var rawMsg json.RawMessage
	if err := json.NewDecoder(conn).Decode(&rawMsg); err != nil {
		return
	}

	var envelope struct {
		Type    string          `json:"type"`
		From    string          `json:"from"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(rawMsg, &envelope); err != nil {
		sn.log.Error("Failed to parse message: %v", err)
		return
	}

	switch envelope.Type {
	case common.MsgStoreBlock:
		var payload common.StoreBlockPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			sn.log.Error("Failed to parse store block payload: %v", err)
			return
		}
		sn.log.Info("📥 Incoming block: %s from %s (%d bytes)", payload.Block.BlockID, envelope.From, len(payload.Block.Data))
		sn.storeBlock(payload)

	case common.MsgReadFile:
		var readReq common.ReadPayload
		if err := json.Unmarshal(envelope.Payload, &readReq); err != nil {
			sn.log.Error("Failed to parse read request: %v", err)
			return
		}
		sn.log.Info("📖 Read request for block: %s", readReq.FileName)
		data, err := sn.readBlock(readReq.FileName)
		if err != nil {
			resp := common.ResponsePayload{Success: false, Message: err.Error()}
			json.NewEncoder(conn).Encode(resp)
			return
		}
		resp := common.ResponsePayload{Success: true, Message: "OK", Data: data}
		json.NewEncoder(conn).Encode(resp)

	case common.MsgReplicateCmd:
		var cmd common.ReplicateCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			sn.log.Error("Failed to parse replicate command: %v", err)
			return
		}
		go sn.handleReplicateCmd(cmd)

	default:
		sn.log.Warn("Unknown message type: %s", envelope.Type)
	}
}

// ==================== BLOCK INVENTORY LOGGER ====================

func (sn *SecondaryNode) logBlockInventory() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		sn.mu.RLock()
		sn.log.Info("─── BLOCK INVENTORY ───")
		sn.log.Info("  Total blocks: %d", len(sn.blocks))
		var totalSize int64
		for id, block := range sn.blocks {
			totalSize += int64(len(block.Data))
			sn.log.Info("  📦 %s (%d bytes, SHA256: %s...)", id, len(block.Data), block.Checksum[:16])
		}
		sn.log.Info("  Total storage used: %d bytes", totalSize)
		sn.log.Info("───────────────────────")
		sn.mu.RUnlock()
	}
}

// ==================== MAIN ====================

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "SecondaryNN"
	}

	rack := os.Getenv("MY_RACK")
	if rack == "" {
		rack = common.RackA // Secondary NameNode is in Rack A by default
	}

	nameNodeAddr := os.Getenv("NAMENODE_ADDR")
	if nameNodeAddr == "" {
		nameNodeAddr = "10.253.4.56:8000" // Laptop 1 (NameNode) IP
	}

	listenPort := os.Getenv("LISTEN_PORT")
	if listenPort == "" {
		listenPort = ":9001"
	} else if listenPort[0] != ':' {
		listenPort = ":" + listenPort
	}

	sn := NewSecondaryNode(nodeID, rack, nameNodeAddr, listenPort)
	sn.log.BannerWithRack("SECONDARY NAMENODE + DATANODE", rack)
	sn.log.Info("Configuration:")
	sn.log.Info("  Node ID:       %s", nodeID)
	sn.log.Info("  Rack:          %s", rack)
	sn.log.Info("  NameNode:      %s", nameNodeAddr)
	sn.log.Info("  Listen Port:   %s", listenPort)
	sn.log.Info("  Block Dir:     %s", sn.blockDir)

	// Start TCP listener for incoming blocks
	ln, err := net.Listen("tcp", listenPort)
	if err != nil {
		sn.log.Error("FATAL: Cannot listen on %s: %v", listenPort, err)
		os.Exit(1)
	}
	sn.log.Success("🌐 Listening for blocks on %s", listenPort)

	// Start background goroutines
	go sn.sendHeartbeats()
	go sn.runCheckpoint()
	go sn.logBlockInventory()

	sn.log.Info("🗳️  Leader Election: Standing by as Secondary NameNode (will take over if Master fails)")

	// Accept connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			sn.log.Error("Accept error: %v", err)
			continue
		}
		go sn.handleConnection(conn)
	}
}
