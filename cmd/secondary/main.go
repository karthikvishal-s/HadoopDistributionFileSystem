package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

	// === NameNode state (used after leader election) ===
	files         map[string]*common.FileMetadata // filename -> metadata
	clusterNodes  map[string]*common.NodeInfo     // nodeID -> info
	blockToNodes  map[string][]string             // blockID -> list of nodeIDs
	dataNodeAddrs map[string]string               // nodeID -> address
	safeMode      bool
}

func NewSecondaryNode(nodeID, rack, nameNodeAddr, listenPort string) *SecondaryNode {
	blockDir := "./blocks_" + nodeID
	os.MkdirAll(blockDir, 0755)

	return &SecondaryNode{
		nodeID:        nodeID,
		rack:          rack,
		nameNodeAddr:  nameNodeAddr,
		listenPort:    listenPort,
		blockDir:      blockDir,
		blocks:        make(map[string]*common.Block),
		isLeader:      false,
		isElecting:    false,
		log:           common.NewLogger("SecondaryNN", nodeID),
		files:         make(map[string]*common.FileMetadata),
		clusterNodes:  make(map[string]*common.NodeInfo),
		blockToNodes:  make(map[string][]string),
		dataNodeAddrs: make(map[string]string),
		safeMode:      false,
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

// ==================== LEADER ELECTION (BULLY ALGORITHM) ====================

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

	// Load the last checkpoint and parse it into NameNode state
	sn.loadMetadataFromCheckpoint()

	// Start NameNode background goroutines
	go sn.leaderMonitorHeartbeats()
	go sn.leaderRunBalancer()
	go sn.leaderPrintStatus()
	go sn.leaderRunCLI()

	// Notify all known DataNodes about the new leader
	sn.notifyDataNodesNewLeader()

	sn.log.Event("👑 FAILOVER COMPLETE — Secondary NameNode is now fully operational as Primary")
	sn.log.Info("👑 All DataNodes should now send heartbeats to this node at %s", sn.listenPort)
}

// ==================== METADATA LOADING (AFTER ELECTION) ====================

func (sn *SecondaryNode) loadMetadataFromCheckpoint() {
	sn.log.Info("👑 Loading last checkpoint (fsimage.json)...")

	data, err := os.ReadFile("fsimage.json")
	if err != nil {
		sn.log.Warn("No fsimage.json found, starting with empty metadata")
		return
	}

	var snapshot common.MetadataSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		sn.log.Warn("Failed to parse fsimage.json: %v", err)
		return
	}

	sn.mu.Lock()
	sn.files = snapshot.Files
	if sn.files == nil {
		sn.files = make(map[string]*common.FileMetadata)
	}
	// Load node info (mark all as not-alive until they send heartbeats)
	if snapshot.Nodes != nil {
		for nodeID, node := range snapshot.Nodes {
			node.Alive = false // Will come back alive when heartbeat arrives
			sn.clusterNodes[nodeID] = node
			if node.Address != "" {
				sn.dataNodeAddrs[nodeID] = node.Address
			}
		}
	}
	// Rebuild block-to-nodes mapping
	for _, file := range sn.files {
		for blockID, nodeIDs := range file.BlockLocations {
			sn.blockToNodes[blockID] = nodeIDs
		}
	}
	sn.safeMode = true // Enter safe mode until DataNodes report in
	sn.mu.Unlock()

	sn.log.Success("👑 Loaded fsimage.json — %d files, %d known nodes", len(sn.files), len(sn.clusterNodes))
	sn.log.Event("🔒 ENTERING SAFE MODE — Waiting for DataNodes to report in...")
}

// ==================== NOTIFY DATANODES ABOUT NEW LEADER ====================

func (sn *SecondaryNode) notifyDataNodesNewLeader() {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	sn.log.Info("👑 Notifying DataNodes about new leader...")

	for nodeID, addr := range sn.dataNodeAddrs {
		if nodeID == sn.nodeID {
			continue // Don't notify ourselves
		}
		go func(nid, address string) {
			conn, err := net.DialTimeout("tcp", address, 3*time.Second)
			if err != nil {
				sn.log.Warn("  Could not notify %s at %s: %v", nid, address, err)
				return
			}
			defer conn.Close()

			msg := common.Message{
				Type: common.MsgNewLeader,
				From: sn.nodeID,
				Payload: common.ElectionPayload{
					CandidateID: sn.nodeID,
					Address:     sn.listenPort,
				},
			}
			json.NewEncoder(conn).Encode(msg)
			sn.log.Success("  ✅ Notified %s about new leader", nid)
		}(nodeID, addr)
	}
}

// ==================== LEADER: HEARTBEAT HANDLER ====================

func (sn *SecondaryNode) leaderHandleHeartbeat(hb common.HeartbeatMsg, remoteAddr string) {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	node, exists := sn.clusterNodes[hb.NodeID]
	if !exists {
		node = &common.NodeInfo{
			NodeID: hb.NodeID,
			Rack:   hb.Rack,
			Alive:  true,
			Blocks: []string{},
		}
		sn.clusterNodes[hb.NodeID] = node
		sn.log.Event("🆕 New DataNode registered: %s (Rack: %s)", hb.NodeID, hb.Rack)
	}

	wasAlive := node.Alive
	node.Alive = true
	node.LastHeartbeat = hb.Timestamp
	node.Rack = hb.Rack
	node.UsedBytes = hb.UsedBytes
	node.TotalBytes = hb.TotalBytes
	node.Blocks = hb.BlockIDs

	if !wasAlive {
		sn.log.Success("💚 DataNode %s is BACK ONLINE (Rack: %s)", hb.NodeID, hb.Rack)
	}

	// Update block-to-node mapping from block report
	for _, blockID := range hb.BlockIDs {
		found := false
		for _, nid := range sn.blockToNodes[blockID] {
			if nid == hb.NodeID {
				found = true
				break
			}
		}
		if !found {
			sn.blockToNodes[blockID] = append(sn.blockToNodes[blockID], hb.NodeID)
		}
	}

	// Store address from connection
	host, _, _ := net.SplitHostPort(remoteAddr)
	port := hb.ListenPort
	if port == "" {
		port = "9001"
	} else if port[0] == ':' {
		port = port[1:]
	}
	sn.dataNodeAddrs[hb.NodeID] = host + ":" + port

	sn.log.Info("💓 Heartbeat from %s (Rack: %s) | Blocks: %d | Storage: %d/%d bytes",
		hb.NodeID, hb.Rack, len(hb.BlockIDs), hb.UsedBytes, hb.TotalBytes)
}

// ==================== LEADER: SAFE MODE CHECK ====================

func (sn *SecondaryNode) leaderCheckSafeMode() {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	if !sn.safeMode {
		return
	}

	aliveCount := 0
	for _, node := range sn.clusterNodes {
		if node.Alive {
			aliveCount++
		}
	}

	// Exit safe mode if at least 1 DataNode is online (since we lost the primary, be lenient)
	if aliveCount >= 1 {
		sn.safeMode = false
		sn.log.Event("🟢 SAFE MODE EXITED — %d DataNode(s) online. New Primary is READY for read/write operations.", aliveCount)
	} else {
		sn.log.Info("⏳ Safe Mode: %d DataNodes online. Waiting for nodes...", aliveCount)
	}
}

// ==================== LEADER: HEARTBEAT MONITOR ====================

func (sn *SecondaryNode) leaderMonitorHeartbeats() {
	ticker := time.NewTicker(common.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		sn.mu.RLock()
		if !sn.isLeader {
			sn.mu.RUnlock()
			return
		}
		sn.mu.RUnlock()

		type deadNodeInfo struct {
			nodeID string
			blocks []string
		}
		var deadNodes []deadNodeInfo

		sn.mu.Lock()
		now := time.Now()
		for nodeID, node := range sn.clusterNodes {
			if nodeID == sn.nodeID {
				continue // Don't monitor ourselves
			}
			if node.Alive && now.Sub(node.LastHeartbeat) > common.DeadNodeTimeout {
				node.Alive = false
				sn.log.Event("💀 DataNode %s DECLARED DEAD — No heartbeat for %v (Rack: %s)",
					nodeID, common.DeadNodeTimeout, node.Rack)

				blocksCopy := make([]string, len(node.Blocks))
				copy(blocksCopy, node.Blocks)
				deadNodes = append(deadNodes, deadNodeInfo{nodeID: nodeID, blocks: blocksCopy})
			}
		}

		// Remove dead nodes from block locations
		for _, dn := range deadNodes {
			for _, blockID := range dn.blocks {
				newNodes := []string{}
				for _, nid := range sn.blockToNodes[blockID] {
					if nid != dn.nodeID {
						newNodes = append(newNodes, nid)
					}
				}
				sn.blockToNodes[blockID] = newNodes
			}
		}
		sn.mu.Unlock()

		// Trigger re-replication outside the lock
		for _, dn := range deadNodes {
			sn.log.Warn("🔄 Triggering re-replication for %d blocks on dead node %s...", len(dn.blocks), dn.nodeID)
			sn.leaderReReplicateBlocks(dn.blocks)
		}

		sn.leaderCheckSafeMode()
	}
}

// ==================== LEADER: RE-REPLICATION ====================

func (sn *SecondaryNode) leaderReReplicateBlocks(blockIDs []string) {
	sn.mu.Lock()
	defer sn.mu.Unlock()

	for _, blockID := range blockIDs {
		currentNodes := sn.blockToNodes[blockID]
		currentCount := len(currentNodes)
		needed := common.ReplicationFactor - currentCount

		if needed <= 0 {
			continue
		}

		sn.log.Warn("  Block %s: %d replicas remaining, need %d more", blockID, currentCount, needed)

		targets := sn.leaderPickReplicationTargets(currentNodes, needed)
		if len(targets) == 0 {
			sn.log.Error("  ❌ Cannot find targets for re-replicating block %s", blockID)
			continue
		}

		if len(currentNodes) > 0 {
			sourceNode := currentNodes[0]
			sourceAddr := sn.dataNodeAddrs[sourceNode]

			targetAddrs := []string{}
			for _, t := range targets {
				targetAddrs = append(targetAddrs, sn.dataNodeAddrs[t])
			}

			sn.log.Info("  📦 Re-replicate block %s: %s → %v", blockID, sourceNode, targets)

			// Update metadata
			sn.blockToNodes[blockID] = append(sn.blockToNodes[blockID], targets...)

			go sn.leaderSendReplicateCommand(sourceAddr, blockID, targets, targetAddrs)
		}
	}
}

func (sn *SecondaryNode) leaderPickReplicationTargets(excludeNodes []string, count int) []string {
	excluded := make(map[string]bool)
	for _, n := range excludeNodes {
		excluded[n] = true
	}

	var targets []string
	for nodeID, node := range sn.clusterNodes {
		if len(targets) >= count {
			break
		}
		if !excluded[nodeID] && node.Alive {
			found := false
			for _, t := range targets {
				if t == nodeID {
					found = true
					break
				}
			}
			if !found {
				targets = append(targets, nodeID)
			}
		}
	}
	return targets
}

func (sn *SecondaryNode) leaderSendReplicateCommand(sourceAddr, blockID string, targets, targetAddrs []string) {
	conn, err := net.DialTimeout("tcp", sourceAddr, 5*time.Second)
	if err != nil {
		sn.log.Error("Failed to connect to %s for re-replication: %v", sourceAddr, err)
		return
	}
	defer conn.Close()

	cmd := common.ReplicateCommand{
		BlockID:     blockID,
		TargetNodes: targets,
		TargetAddrs: targetAddrs,
	}

	msg := common.Message{
		Type:    common.MsgReplicateCmd,
		From:    sn.nodeID,
		Payload: cmd,
	}

	json.NewEncoder(conn).Encode(msg)
	sn.log.Success("  ✅ Replicate command sent for block %s to %s", blockID, sourceAddr)
}

// ==================== LEADER: BLOCK PLACEMENT (RACK-AWARE) ====================

func (sn *SecondaryNode) leaderPickBlockPlacement() []string {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	rackNodes := make(map[string][]string)
	for nodeID, node := range sn.clusterNodes {
		if node.Alive {
			rackNodes[node.Rack] = append(rackNodes[node.Rack], nodeID)
		}
	}

	var targets []string

	sn.log.Info("🏗️  Block Placement Algorithm starting...")
	for rack, nodes := range rackNodes {
		sn.log.Info("     %s: %v", rack, nodes)
	}

	// Step 1: First replica on any rack
	localRack := ""
	for rack := range rackNodes {
		localRack = rack
		break
	}

	if nodes, ok := rackNodes[localRack]; ok && len(nodes) > 0 {
		idx := rand.Intn(len(nodes))
		targets = append(targets, nodes[idx])
		sn.log.Info("   Replica 1 → %s (rack: %s)", nodes[idx], localRack)
		rackNodes[localRack] = append(nodes[:idx], nodes[idx+1:]...)
	}

	// Step 2: Second replica on a DIFFERENT rack
	remoteRack := ""
	for rack := range rackNodes {
		if rack != localRack && len(rackNodes[rack]) > 0 {
			remoteRack = rack
			break
		}
	}

	if remoteRack != "" && len(rackNodes[remoteRack]) > 0 {
		nodes := rackNodes[remoteRack]
		idx := rand.Intn(len(nodes))
		targets = append(targets, nodes[idx])
		sn.log.Info("   Replica 2 → %s (remote rack: %s)", nodes[idx], remoteRack)
		rackNodes[remoteRack] = append(nodes[:idx], nodes[idx+1:]...)
	}

	// Step 3: Third replica on the SAME remote rack, DIFFERENT node
	if remoteRack != "" && len(rackNodes[remoteRack]) > 0 {
		nodes := rackNodes[remoteRack]
		idx := rand.Intn(len(nodes))
		targets = append(targets, nodes[idx])
		sn.log.Info("   Replica 3 → %s (same remote rack: %s)", nodes[idx], remoteRack)
	} else {
		// Fallback: any available node
		for rack, nodes := range rackNodes {
			if len(nodes) > 0 {
				idx := rand.Intn(len(nodes))
				targets = append(targets, nodes[idx])
				sn.log.Info("   Replica 3 → %s (fallback rack: %s)", nodes[idx], rack)
				break
			}
		}
	}

	sn.log.Success("🏗️  Block placement decided: %v", targets)
	return targets
}

// ==================== LEADER: FILE UPLOAD ====================

func (sn *SecondaryNode) leaderHandleUpload(conn net.Conn, payload json.RawMessage) {
	var upload common.UploadPayload
	if err := json.Unmarshal(payload, &upload); err != nil {
		sn.log.Error("Failed to parse upload payload: %v", err)
		sn.leaderSendResponse(conn, false, "Invalid upload payload", nil)
		return
	}

	upload.FileName = filepath.Base(upload.FileName)

	sn.mu.RLock()
	inSafe := sn.safeMode
	sn.mu.RUnlock()

	if inSafe {
		sn.log.Warn("🚫 Upload rejected — cluster is in SAFE MODE")
		sn.leaderSendResponse(conn, false, "Cluster is in Safe Mode. Cannot write.", nil)
		return
	}

	sn.log.Event("📤 FILE UPLOAD: '%s' (%d bytes)", upload.FileName, len(upload.Data))

	data := upload.Data
	var blockIDs []string
	blockLocations := make(map[string][]string)
	blockNum := 0

	for len(data) > 0 {
		chunkSize := common.BlockSize
		if chunkSize > len(data) {
			chunkSize = len(data)
		}
		chunk := data[:chunkSize]
		data = data[chunkSize:]

		blockID := fmt.Sprintf("%s_block_%d", filepath.Base(upload.FileName), blockNum)
		checksum := common.ComputeChecksum(chunk)

		sn.log.Info("  📦 Block %d: ID=%s Size=%d bytes SHA256=%s...", blockNum, blockID, len(chunk), checksum[:16])

		targets := sn.leaderPickBlockPlacement()

		if len(targets) == 0 {
			sn.log.Error("  ❌ No available DataNodes for block %s", blockID)
			sn.leaderSendResponse(conn, false, "No available DataNodes", nil)
			return
		}

		block := common.Block{
			BlockID:  blockID,
			Data:     chunk,
			Checksum: checksum,
		}

		// Send block to first target in pipeline
		sn.mu.RLock()
		firstTarget := targets[0]
		firstAddr := sn.dataNodeAddrs[firstTarget]
		remainingTargets := targets[1:]
		remainingAddrs := []string{}
		for _, t := range remainingTargets {
			remainingAddrs = append(remainingAddrs, sn.dataNodeAddrs[t])
		}
		sn.mu.RUnlock()

		sn.log.Info("  📡 Sending block %s to pipeline: %v", blockID, targets)

		err := sn.leaderSendBlockToDataNode(firstAddr, block, upload.FileName, remainingTargets, remainingAddrs)
		if err != nil {
			sn.log.Error("  ❌ Failed to send block %s to %s: %v", blockID, firstTarget, err)
			sn.leaderSendResponse(conn, false, fmt.Sprintf("Failed to store block %s", blockID), nil)
			return
		}

		blockIDs = append(blockIDs, blockID)
		blockLocations[blockID] = targets
		blockNum++

		sn.log.Success("  ✅ Block %s stored across %v", blockID, targets)
	}

	// Store metadata
	sn.mu.Lock()
	sn.files[upload.FileName] = &common.FileMetadata{
		FileName:       upload.FileName,
		FileSize:       int64(len(upload.Data)),
		BlockIDs:       blockIDs,
		BlockLocations: blockLocations,
		CreatedAt:      time.Now(),
	}
	for blockID, nodes := range blockLocations {
		sn.blockToNodes[blockID] = nodes
	}
	sn.mu.Unlock()

	sn.log.Success("📤 FILE UPLOAD COMPLETE: '%s' → %d blocks, replication factor=%d", upload.FileName, len(blockIDs), common.ReplicationFactor)

	sn.leaderSaveMetadata()
	sn.leaderSendResponse(conn, true, fmt.Sprintf("File '%s' uploaded: %d blocks across cluster", upload.FileName, len(blockIDs)), nil)
}

func (sn *SecondaryNode) leaderSendBlockToDataNode(addr string, block common.Block, fileName string, targets []string, targetAddrs []string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
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

	return json.NewEncoder(conn).Encode(msg)
}

// ==================== LEADER: FILE READ ====================

func (sn *SecondaryNode) leaderHandleRead(conn net.Conn, payload json.RawMessage) {
	var readReq common.ReadPayload
	if err := json.Unmarshal(payload, &readReq); err != nil {
		sn.log.Error("Failed to parse read payload: %v", err)
		sn.leaderSendResponse(conn, false, "Invalid read payload", nil)
		return
	}

	readReq.FileName = filepath.Base(readReq.FileName)

	sn.mu.RLock()
	fileMeta, exists := sn.files[readReq.FileName]
	sn.mu.RUnlock()

	if !exists {
		sn.log.Warn("📖 File '%s' not found", readReq.FileName)
		sn.leaderSendResponse(conn, false, fmt.Sprintf("File '%s' not found", readReq.FileName), nil)
		return
	}

	sn.log.Event("📖 FILE READ: '%s' (%d blocks)", readReq.FileName, len(fileMeta.BlockIDs))

	var fullData []byte
	for i, blockID := range fileMeta.BlockIDs {
		sn.mu.RLock()
		nodeIDs := sn.blockToNodes[blockID]
		sn.mu.RUnlock()

		if len(nodeIDs) == 0 {
			sn.log.Error("  ❌ No nodes have block %s", blockID)
			sn.leaderSendResponse(conn, false, fmt.Sprintf("Block %s is unavailable", blockID), nil)
			return
		}

		var blockData []byte
		var fetchErr error
		for _, nodeID := range nodeIDs {
			sn.mu.RLock()
			addr := sn.dataNodeAddrs[nodeID]
			sn.mu.RUnlock()

			sn.log.Info("  📥 Reading block %d (%s) from %s...", i, blockID, nodeID)
			blockData, fetchErr = sn.leaderFetchBlockFromDataNode(addr, blockID)
			if fetchErr == nil {
				sn.log.Success("  ✅ Block %s read from %s (%d bytes)", blockID, nodeID, len(blockData))
				break
			}
			sn.log.Warn("  ⚠️ Failed to read block %s from %s: %v, trying next replica...", blockID, nodeID, fetchErr)
		}

		if fetchErr != nil {
			sn.log.Error("  ❌ All replicas for block %s are unavailable", blockID)
			sn.leaderSendResponse(conn, false, fmt.Sprintf("Block %s unreadable from all replicas", blockID), nil)
			return
		}

		fullData = append(fullData, blockData...)
	}

	sn.log.Success("📖 FILE READ COMPLETE: '%s' (%d bytes from %d blocks)", readReq.FileName, len(fullData), len(fileMeta.BlockIDs))
	sn.leaderSendResponse(conn, true, "File read successful", fullData)
}

func (sn *SecondaryNode) leaderFetchBlockFromDataNode(addr, blockID string) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	msg := common.Message{
		Type:    common.MsgReadFile,
		From:    sn.nodeID,
		Payload: json.RawMessage(fmt.Sprintf(`{"file_name":"%s"}`, blockID)),
	}
	json.NewEncoder(conn).Encode(msg)

	var resp common.ResponsePayload
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("datanode error: %s", resp.Message)
	}

	return resp.Data, nil
}

// ==================== LEADER: METADATA PERSISTENCE ====================

func (sn *SecondaryNode) leaderSaveMetadata() {
	sn.mu.RLock()
	snapshot := common.MetadataSnapshot{
		Files:     sn.files,
		Nodes:     sn.clusterNodes,
		Timestamp: time.Now(),
	}
	sn.mu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		sn.log.Error("Failed to marshal metadata: %v", err)
		return
	}

	if err := os.WriteFile("fsimage.json", data, 0644); err != nil {
		sn.log.Error("Failed to save fsimage.json: %v", err)
		return
	}

	sn.log.Info("💾 Metadata saved to fsimage.json (%d files, %d nodes)", len(snapshot.Files), len(snapshot.Nodes))
}

// ==================== LEADER: BALANCER ====================

func (sn *SecondaryNode) leaderRunBalancer() {
	ticker := time.NewTicker(common.BalancerInterval)
	defer ticker.Stop()

	for range ticker.C {
		sn.mu.RLock()
		if !sn.isLeader || sn.safeMode {
			sn.mu.RUnlock()
			if !sn.isLeader {
				return
			}
			continue
		}

		type nodeUtil struct {
			nodeID string
			ratio  float64
		}

		var utils []nodeUtil
		for nodeID, node := range sn.clusterNodes {
			if node.Alive && node.TotalBytes > 0 {
				ratio := float64(node.UsedBytes) / float64(node.TotalBytes)
				utils = append(utils, nodeUtil{nodeID, ratio})
			}
		}
		sn.mu.RUnlock()

		if len(utils) < 2 {
			continue
		}

		sort.Slice(utils, func(i, j int) bool { return utils[i].ratio > utils[j].ratio })

		maxUtil := utils[0]
		minUtil := utils[len(utils)-1]

		if maxUtil.ratio-minUtil.ratio > common.BalanceThreshold {
			sn.log.Event("⚖️  BALANCER: Imbalance detected! %s=%.0f%% vs %s=%.0f%%",
				maxUtil.nodeID, maxUtil.ratio*100, minUtil.nodeID, minUtil.ratio*100)

			sn.mu.RLock()
			overNode := sn.clusterNodes[maxUtil.nodeID]
			if overNode != nil && len(overNode.Blocks) > 0 {
				blockToMove := overNode.Blocks[0]
				sourceAddr := sn.dataNodeAddrs[maxUtil.nodeID]
				targetAddr := sn.dataNodeAddrs[minUtil.nodeID]
				sn.mu.RUnlock()

				if sourceAddr != "" && targetAddr != "" {
					sn.log.Info("  ⚖️  Moving block %s from %s → %s", blockToMove, maxUtil.nodeID, minUtil.nodeID)
					go sn.leaderSendReplicateCommand(sourceAddr, blockToMove, []string{minUtil.nodeID}, []string{targetAddr})

					sn.mu.Lock()
					sn.blockToNodes[blockToMove] = append(sn.blockToNodes[blockToMove], minUtil.nodeID)
					sn.mu.Unlock()
				}
			} else {
				sn.mu.RUnlock()
			}
		} else {
			sn.log.Info("⚖️  Balancer: Cluster is balanced (spread: %.1f%%)", (maxUtil.ratio-minUtil.ratio)*100)
		}
	}
}

// ==================== LEADER: STATUS DISPLAY ====================

func (sn *SecondaryNode) leaderPrintStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		sn.mu.RLock()
		if !sn.isLeader {
			sn.mu.RUnlock()
			return
		}

		sn.log.Info("─── CLUSTER STATUS (NEW PRIMARY) ───")
		if sn.safeMode {
			sn.log.Warn("  Mode: 🔒 SAFE MODE (read-only)")
		} else {
			sn.log.Success("  Mode: 🟢 ACTIVE (read-write)")
		}

		aliveCount := 0
		for nodeID, node := range sn.clusterNodes {
			status := "🟢 ALIVE"
			if !node.Alive {
				status = "🔴 DEAD"
			} else {
				aliveCount++
			}
			sn.log.Info("  %s %s (Rack: %s, Blocks: %d, LastHB: %s)",
				status, nodeID, node.Rack, len(node.Blocks),
				node.LastHeartbeat.Format("15:04:05"))
		}
		sn.log.Info("  Files: %d | Live Nodes: %d/%d", len(sn.files), aliveCount, len(sn.clusterNodes))
		sn.log.Info("─────────────────────────────────────")
		sn.mu.RUnlock()
	}
}

// ==================== LEADER: INTERACTIVE CLI ====================

func (sn *SecondaryNode) leaderRunCLI() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println()
	sn.log.Info("📝 [NEW PRIMARY] Interactive CLI ready. Commands: upload <file>, read <file>, status, files, exit")

	for {
		fmt.Print("\nhdfs> ")
		if !scanner.Scan() {
			break
		}
		input := strings.TrimSpace(scanner.Text())
		parts := strings.SplitN(input, " ", 2)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "upload":
			if len(parts) < 2 {
				fmt.Println("Usage: upload <filepath>")
				continue
			}
			filePath := parts[1]
			data, err := os.ReadFile(filePath)
			if err != nil {
				sn.log.Error("Cannot read file %s: %v", filePath, err)
				continue
			}
			// Create a local connection to self
			conn, err := net.Dial("tcp", "localhost"+sn.listenPort)
			if err != nil {
				sn.log.Error("Self-connect failed: %v", err)
				continue
			}
			payload, _ := json.Marshal(common.UploadPayload{FileName: filePath, Data: data})
			msg := common.Message{Type: common.MsgUploadFile, From: "CLI", Payload: json.RawMessage(payload)}
			json.NewEncoder(conn).Encode(msg)
			var resp common.ResponsePayload
			json.NewDecoder(conn).Decode(&resp)
			conn.Close()
			if resp.Success {
				sn.log.Success("CLI: %s", resp.Message)
			} else {
				sn.log.Error("CLI: %s", resp.Message)
			}

		case "read":
			if len(parts) < 2 {
				fmt.Println("Usage: read <filename>")
				continue
			}
			fileName := parts[1]
			conn, err := net.Dial("tcp", "localhost"+sn.listenPort)
			if err != nil {
				sn.log.Error("Self-connect failed: %v", err)
				continue
			}
			payload, _ := json.Marshal(common.ReadPayload{FileName: fileName})
			msg := common.Message{Type: common.MsgReadFile, From: "CLI", Payload: json.RawMessage(payload)}
			json.NewEncoder(conn).Encode(msg)
			var resp common.ResponsePayload
			json.NewDecoder(conn).Decode(&resp)
			conn.Close()
			if resp.Success {
				sn.log.Success("CLI: File read (%d bytes)", len(resp.Data))
				if len(resp.Data) < 1024 {
					fmt.Printf("Content:\n%s\n", string(resp.Data))
				} else {
					fmt.Printf("Content (first 1024 bytes):\n%s\n...\n", string(resp.Data[:1024]))
				}
			} else {
				sn.log.Error("CLI: %s", resp.Message)
			}

		case "status":
			sn.mu.RLock()
			fmt.Println("\n══════════ CLUSTER STATUS (NEW PRIMARY) ══════════")
			if sn.safeMode {
				fmt.Println("Mode: 🔒 SAFE MODE (read-only)")
			} else {
				fmt.Println("Mode: 🟢 ACTIVE (read-write)")
			}
			fmt.Println("\nDataNodes:")
			for nodeID, node := range sn.clusterNodes {
				status := "🟢 ALIVE"
				if !node.Alive {
					status = "🔴 DEAD"
				}
				fmt.Printf("  %s %s | Rack: %s | Blocks: %d | Storage: %d/%d bytes | Last HB: %s\n",
					status, nodeID, node.Rack, len(node.Blocks),
					node.UsedBytes, node.TotalBytes,
					node.LastHeartbeat.Format("15:04:05"))
			}
			fmt.Println("═══════════════════════════════════════════════════")
			sn.mu.RUnlock()

		case "files":
			sn.mu.RLock()
			fmt.Println("\n══════════ FILE LISTING ══════════")
			if len(sn.files) == 0 {
				fmt.Println("  (no files)")
			}
			for name, meta := range sn.files {
				fmt.Printf("  📄 %s (%d bytes, %d blocks)\n", name, meta.FileSize, len(meta.BlockIDs))
				for _, bid := range meta.BlockIDs {
					nodes := meta.BlockLocations[bid]
					fmt.Printf("     └─ %s → %v\n", bid, nodes)
				}
			}
			fmt.Println("══════════════════════════════════")
			sn.mu.RUnlock()

		case "exit", "quit":
			sn.log.Info("Shutting down...")
			os.Exit(0)

		case "":
			continue

		default:
			fmt.Println("Unknown command. Available: upload <file>, read <file>, status, files, exit")
		}
	}
}

// ==================== LEADER: RESPONSE HELPER ====================

func (sn *SecondaryNode) leaderSendResponse(conn net.Conn, success bool, message string, data []byte) {
	resp := common.ResponsePayload{
		Success: success,
		Message: message,
		Data:    data,
	}
	json.NewEncoder(conn).Encode(resp)
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

// ==================== BLOCK STORAGE (DataNode role) ====================

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

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		sn.log.Error("❌ Failed to encode block %s for forwarding: %v", block.BlockID, err)
		return
	}
	sn.log.Success("✅ Block %s forwarded to %s", block.BlockID, addr)
}

// ==================== READ BLOCK (DataNode role) ====================

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
	remoteAddr := conn.RemoteAddr().String()

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

	sn.mu.RLock()
	amLeader := sn.isLeader
	sn.mu.RUnlock()

	// ===== LEADER MODE: Handle NameNode-style requests =====
	if amLeader {
		switch envelope.Type {
		case common.MsgHeartbeat:
			var hb common.HeartbeatMsg
			if err := json.Unmarshal(envelope.Payload, &hb); err != nil {
				sn.log.Error("Failed to parse heartbeat: %v", err)
				return
			}
			sn.leaderHandleHeartbeat(hb, remoteAddr)
			return

		case common.MsgUploadFile:
			sn.leaderHandleUpload(conn, envelope.Payload)
			return

		case common.MsgReadFile:
			// Check if this is a NameNode-level file read or a DataNode block read
			var readReq common.ReadPayload
			if err := json.Unmarshal(envelope.Payload, &readReq); err != nil {
				sn.log.Error("Failed to parse read request: %v", err)
				return
			}
			// If we have the file in metadata, handle as NameNode
			sn.mu.RLock()
			_, isFile := sn.files[filepath.Base(readReq.FileName)]
			sn.mu.RUnlock()
			if isFile {
				sn.leaderHandleRead(conn, envelope.Payload)
				return
			}
			// Otherwise fall through to DataNode block read below

		case common.MsgGetMetadata:
			sn.mu.RLock()
			snapshot := common.MetadataSnapshot{
				Files:     sn.files,
				Nodes:     sn.clusterNodes,
				Timestamp: time.Now(),
			}
			sn.mu.RUnlock()
			json.NewEncoder(conn).Encode(snapshot)
			sn.log.Info("📋 Metadata snapshot sent to %s", envelope.From)
			return
		}
	}

	// ===== DATANODE MODE: Handle DataNode-style requests =====
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
