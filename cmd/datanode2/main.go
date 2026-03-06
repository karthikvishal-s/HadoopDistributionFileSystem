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

// ==================== DATANODE 2 (Rack B, Laptop 4) ====================

type DataNode struct {
	mu           sync.RWMutex
	nodeID       string
	rack         string
	nameNodeAddr string
	listenPort   string
	blockDir     string
	blocks       map[string]*common.Block
	log          *common.Logger
}

func NewDataNode(nodeID, rack, nameNodeAddr, listenPort string) *DataNode {
	blockDir := "./blocks_" + nodeID
	os.MkdirAll(blockDir, 0755)

	return &DataNode{
		nodeID:       nodeID,
		rack:         rack,
		nameNodeAddr: nameNodeAddr,
		listenPort:   listenPort,
		blockDir:     blockDir,
		blocks:       make(map[string]*common.Block),
		log:          common.NewLogger("DataNode", nodeID),
	}
}

// ==================== HEARTBEAT SENDER ====================

func (dn *DataNode) sendHeartbeats() {
	time.Sleep(2 * time.Second) // Stagger from DataNode1

	ticker := time.NewTicker(common.HeartbeatInterval)
	defer ticker.Stop()

	heartbeatCount := 0

	for range ticker.C {
		heartbeatCount++

		dn.mu.RLock()
		blockIDs := make([]string, 0, len(dn.blocks))
		var usedBytes int64
		for id, block := range dn.blocks {
			blockIDs = append(blockIDs, id)
			usedBytes += int64(len(block.Data))
		}
		dn.mu.RUnlock()

		hb := common.HeartbeatMsg{
			NodeID:     dn.nodeID,
			Rack:       dn.rack,
			ListenPort: dn.listenPort,
			Timestamp:  time.Now(),
			BlockIDs:   blockIDs,
			UsedBytes:  usedBytes,
			TotalBytes: 10 * 1024 * 1024 * 1024, // Simulate 10 GB
		}

		payload, _ := json.Marshal(hb)
		msg := common.Message{
			Type:    common.MsgHeartbeat,
			From:    dn.nodeID,
			Payload: json.RawMessage(payload),
		}

		conn, err := net.DialTimeout("tcp", dn.nameNodeAddr, 3*time.Second)
		if err != nil {
			dn.log.Error("💔 Heartbeat #%d FAILED — Cannot reach NameNode at %s: %v", heartbeatCount, dn.nameNodeAddr, err)
			continue
		}

		json.NewEncoder(conn).Encode(msg)
		conn.Close()
		dn.log.Info("💓 Heartbeat #%d sent → NameNode | Blocks: %d | Used: %d bytes | Rack: %s",
			heartbeatCount, len(blockIDs), usedBytes, dn.rack)
	}
}

// ==================== BLOCK STORAGE ====================

func (dn *DataNode) storeBlock(payload common.StoreBlockPayload) {
	block := payload.Block

	dn.log.Info("📥 Receiving block %s (%d bytes)...", block.BlockID, len(block.Data))

	// Step 1: Verify SHA-256 checksum
	dn.log.Info("🔍 Verifying SHA-256 checksum for block %s...", block.BlockID)
	computedChecksum := common.ComputeChecksum(block.Data)
	if computedChecksum != block.Checksum {
		dn.log.Error("❌ CHECKSUM MISMATCH for block %s!", block.BlockID)
		dn.log.Error("   Expected: %s", block.Checksum)
		dn.log.Error("   Computed: %s", computedChecksum)
		dn.log.Error("   ⚠️ DATA CORRUPTION DETECTED — Block rejected!")
		return
	}
	dn.log.Success("✅ SHA-256 checksum PASSED for block %s", block.BlockID)
	dn.log.Info("   Checksum: %s", block.Checksum)

	// Step 2: Save to disk
	blockPath := filepath.Join(dn.blockDir, block.BlockID)
	if err := os.WriteFile(blockPath, block.Data, 0644); err != nil {
		dn.log.Error("❌ Failed to write block %s to disk: %v", block.BlockID, err)
		return
	}

	// Step 3: Save checksum metadata
	checksumPath := blockPath + ".sha256"
	os.WriteFile(checksumPath, []byte(block.Checksum), 0644)

	// Step 4: Store in memory
	dn.mu.Lock()
	dn.blocks[block.BlockID] = &block
	blockCount := len(dn.blocks)
	dn.mu.Unlock()

	dn.log.Success("💾 Block %s STORED successfully", block.BlockID)
	dn.log.Info("   Path: %s", blockPath)
	dn.log.Info("   Size: %d bytes", len(block.Data))
	dn.log.Info("   Total blocks on this node: %d", blockCount)

	// Step 5: Forward to next in replication pipeline (if any)
	if len(payload.Targets) > 0 && len(payload.TargetAddrs) > 0 {
		nextTarget := payload.Targets[0]
		nextAddr := payload.TargetAddrs[0]
		remainingTargets := payload.Targets[1:]
		remainingAddrs := payload.TargetAddrs[1:]

		dn.log.Info("📡 Replication Pipeline: Forwarding block %s to %s (%s)", block.BlockID, nextTarget, nextAddr)
		dn.log.Info("   Remaining pipeline: %v", remainingTargets)

		go dn.forwardBlock(nextAddr, block, payload.FileName, remainingTargets, remainingAddrs)
	} else {
		dn.log.Success("📡 Block %s: ✅ End of replication pipeline — all replicas complete", block.BlockID)
	}
}

func (dn *DataNode) forwardBlock(addr string, block common.Block, fileName string, targets []string, targetAddrs []string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		dn.log.Error("❌ Pipeline BROKEN — Failed to forward block %s to %s: %v", block.BlockID, addr, err)
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
		From:    dn.nodeID,
		Payload: payload,
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		dn.log.Error("❌ Failed to encode block %s for forwarding: %v", block.BlockID, err)
		return
	}
	dn.log.Success("✅ Block %s forwarded to %s — pipeline continues", block.BlockID, addr)
}

// ==================== READ BLOCK ====================

func (dn *DataNode) readBlock(blockID string) ([]byte, error) {
	dn.log.Info("📖 Reading block %s...", blockID)

	dn.mu.RLock()
	block, exists := dn.blocks[blockID]
	dn.mu.RUnlock()

	if !exists {
		blockPath := filepath.Join(dn.blockDir, blockID)
		data, err := os.ReadFile(blockPath)
		if err != nil {
			dn.log.Error("❌ Block %s not found (not in memory or on disk)", blockID)
			return nil, fmt.Errorf("block %s not found", blockID)
		}

		checksumPath := blockPath + ".sha256"
		expectedChecksum, err := os.ReadFile(checksumPath)
		if err == nil {
			dn.log.Info("🔍 Verifying on-disk integrity for block %s...", blockID)
			if !common.VerifyChecksum(data, string(expectedChecksum)) {
				dn.log.Error("❌ DISK CORRUPTION DETECTED for block %s!", blockID)
				return nil, fmt.Errorf("block %s is corrupted on disk", blockID)
			}
			dn.log.Success("✅ On-disk integrity verified for block %s", blockID)
		}

		dn.log.Success("📖 Block %s read from disk (%d bytes)", blockID, len(data))
		return data, nil
	}

	dn.log.Info("🔍 Verifying in-memory integrity for block %s...", blockID)
	if !common.VerifyChecksum(block.Data, block.Checksum) {
		dn.log.Error("❌ MEMORY CORRUPTION DETECTED for block %s!", block.BlockID)
		return nil, fmt.Errorf("block %s is corrupted in memory", blockID)
	}
	dn.log.Success("✅ In-memory integrity verified for block %s (SHA-256: %s...)", blockID, block.Checksum[:16])
	dn.log.Success("📖 Block %s read from memory (%d bytes)", blockID, len(block.Data))

	return block.Data, nil
}

// ==================== REPLICATE COMMAND HANDLER ====================

func (dn *DataNode) handleReplicateCmd(cmd common.ReplicateCommand) {
	dn.log.Event("🔄 RE-REPLICATION: Block %s → targets: %v", cmd.BlockID, cmd.TargetNodes)

	data, err := dn.readBlock(cmd.BlockID)
	if err != nil {
		dn.log.Error("❌ Cannot re-replicate block %s: %v", cmd.BlockID, err)
		return
	}

	block := common.Block{
		BlockID:  cmd.BlockID,
		Data:     data,
		Checksum: common.ComputeChecksum(data),
	}

	for i, addr := range cmd.TargetAddrs {
		nodeID := cmd.TargetNodes[i]
		dn.log.Info("📡 Re-replicating block %s → %s (%s)", cmd.BlockID, nodeID, addr)

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			dn.log.Error("❌ Failed to connect to %s for re-replication: %v", addr, err)
			continue
		}

		payload := common.StoreBlockPayload{
			Block:       block,
			Targets:     []string{},
			TargetAddrs: []string{},
		}
		msg := common.Message{
			Type:    common.MsgStoreBlock,
			From:    dn.nodeID,
			Payload: payload,
		}
		json.NewEncoder(conn).Encode(msg)
		conn.Close()
		dn.log.Success("✅ Block %s re-replicated to %s", cmd.BlockID, nodeID)
	}

	dn.log.Success("🔄 Re-replication of block %s complete", cmd.BlockID)
}

// ==================== CONNECTION HANDLER ====================

func (dn *DataNode) handleConnection(conn net.Conn) {
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()

	var rawMsg json.RawMessage
	if err := json.NewDecoder(conn).Decode(&rawMsg); err != nil {
		dn.log.Warn("Invalid message from %s: %v", remoteAddr, err)
		return
	}

	var envelope struct {
		Type    string          `json:"type"`
		From    string          `json:"from"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(rawMsg, &envelope); err != nil {
		dn.log.Error("Failed to parse message from %s: %v", remoteAddr, err)
		return
	}

	dn.log.Info("📨 Message received: type=%s from=%s via=%s", envelope.Type, envelope.From, remoteAddr)

	switch envelope.Type {
	case common.MsgStoreBlock:
		var payload common.StoreBlockPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			dn.log.Error("Failed to parse StoreBlock payload: %v", err)
			return
		}
		dn.storeBlock(payload)

	case common.MsgReadFile:
		var readReq common.ReadPayload
		if err := json.Unmarshal(envelope.Payload, &readReq); err != nil {
			dn.log.Error("Failed to parse ReadFile payload: %v", err)
			return
		}
		data, err := dn.readBlock(readReq.FileName)
		if err != nil {
			resp := common.ResponsePayload{Success: false, Message: err.Error()}
			json.NewEncoder(conn).Encode(resp)
			dn.log.Error("📖 Block read FAILED: %v", err)
			return
		}
		resp := common.ResponsePayload{Success: true, Message: "OK", Data: data}
		json.NewEncoder(conn).Encode(resp)
		dn.log.Success("📖 Block read SUCCESS: %s (%d bytes)", readReq.FileName, len(data))

	case common.MsgReplicateCmd:
		var cmd common.ReplicateCommand
		if err := json.Unmarshal(envelope.Payload, &cmd); err != nil {
			dn.log.Error("Failed to parse ReplicateCmd payload: %v", err)
			return
		}
		go dn.handleReplicateCmd(cmd)

	case common.MsgNewLeader:
		var election common.ElectionPayload
		if err := json.Unmarshal(envelope.Payload, &election); err != nil {
			dn.log.Error("Failed to parse NewLeader payload: %v", err)
			return
		}
		// Redirect heartbeats to the new leader
		host, _, _ := net.SplitHostPort(remoteAddr)
		port := election.Address
		if port != "" && port[0] == ':' {
			port = port[1:]
		}
		newAddr := host + ":" + port
		dn.mu.Lock()
		dn.nameNodeAddr = newAddr
		dn.mu.Unlock()
		dn.log.Event("👑 NEW LEADER: %s at %s — Redirecting heartbeats to new NameNode", election.CandidateID, newAddr)

	default:
		dn.log.Warn("Unknown message type: %s", envelope.Type)
	}
}

// ==================== BLOCK INVENTORY LOGGER ====================

func (dn *DataNode) logBlockInventory() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		dn.mu.RLock()
		dn.log.Info("─── BLOCK INVENTORY (%s, %s) ───", dn.nodeID, dn.rack)
		if len(dn.blocks) == 0 {
			dn.log.Info("  (no blocks stored)")
		} else {
			var totalSize int64
			for id, block := range dn.blocks {
				totalSize += int64(len(block.Data))
				checksumPreview := block.Checksum
				if len(checksumPreview) > 16 {
					checksumPreview = checksumPreview[:16] + "..."
				}
				dn.log.Info("  📦 %s | %d bytes | SHA256: %s", id, len(block.Data), checksumPreview)
			}
			dn.log.Info("  Total: %d blocks, %d bytes", len(dn.blocks), totalSize)
		}
		dn.log.Info("─────────────────────────────────")
		dn.mu.RUnlock()
	}
}

// ==================== MAIN ====================

func main() {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "DataNode2"
	}

	rack := os.Getenv("MY_RACK")
	if rack == "" {
		rack = common.RackB // DataNode2 is in Rack B by default
	}

	nameNodeAddr := os.Getenv("NAMENODE_ADDR")
	if nameNodeAddr == "" {
		nameNodeAddr = "localhost:8000"
	}

	listenPort := os.Getenv("LISTEN_PORT")
	if listenPort == "" {
		listenPort = ":9003"
	} else if listenPort[0] != ':' {
		listenPort = ":" + listenPort
	}

	dn := NewDataNode(nodeID, rack, nameNodeAddr, listenPort)
	dn.log.BannerWithRack("DATANODE 2 (Rack B — Backup)", rack)
	dn.log.Info("Configuration:")
	dn.log.Info("  Node ID:       %s", nodeID)
	dn.log.Info("  Rack:          %s", rack)
	dn.log.Info("  NameNode:      %s", nameNodeAddr)
	dn.log.Info("  Listen Port:   %s", listenPort)
	dn.log.Info("  Block Dir:     %s", dn.blockDir)
	dn.log.Info("  Heartbeat:     every %v", common.HeartbeatInterval)
	dn.log.Info("  Checksum:      SHA-256")

	// Load existing blocks from disk
	entries, err := os.ReadDir(dn.blockDir)
	if err == nil {
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) != ".sha256" {
				blockID := entry.Name()
				data, err := os.ReadFile(filepath.Join(dn.blockDir, blockID))
				if err == nil {
					checksumData, _ := os.ReadFile(filepath.Join(dn.blockDir, blockID+".sha256"))
					checksum := string(checksumData)
					if checksum == "" {
						checksum = common.ComputeChecksum(data)
					}
					dn.blocks[blockID] = &common.Block{
						BlockID:  blockID,
						Data:     data,
						Checksum: checksum,
					}
					dn.log.Info("  📦 Loaded block from disk: %s (%d bytes)", blockID, len(data))
				}
			}
		}
		if len(dn.blocks) > 0 {
			dn.log.Success("  Loaded %d blocks from disk", len(dn.blocks))
		}
	}

	// Start TCP listener
	ln, err := net.Listen("tcp", listenPort)
	if err != nil {
		dn.log.Error("FATAL: Cannot listen on %s: %v", listenPort, err)
		os.Exit(1)
	}
	dn.log.Success("🌐 DataNode TCP listener started on %s", listenPort)

	// Start background goroutines
	go dn.sendHeartbeats()
	go dn.logBlockInventory()

	dn.log.Info("⏳ DataNode %s ready — sending heartbeats to NameNode every %v", nodeID, common.HeartbeatInterval)

	// Accept connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			dn.log.Error("Accept error: %v", err)
			continue
		}
		go dn.handleConnection(conn)
	}
}
