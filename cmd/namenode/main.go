package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"hdfs-simulation/common"
)

// ==================== NAMENODE STATE ====================

type NameNode struct {
	mu            sync.RWMutex
	files         map[string]*common.FileMetadata // filename -> metadata
	nodes         map[string]*common.NodeInfo     // nodeID -> info
	blockToNodes  map[string][]string             // blockID -> list of nodeIDs
	safeMode      bool
	expectedNodes int
	listenPort    string
	log           *common.Logger
	dataNodeAddrs map[string]string // nodeID -> address
}

func NewNameNode(port string, expectedNodes int) *NameNode {
	nn := &NameNode{
		files:         make(map[string]*common.FileMetadata),
		nodes:         make(map[string]*common.NodeInfo),
		blockToNodes:  make(map[string][]string),
		safeMode:      true,
		expectedNodes: expectedNodes,
		listenPort:    port,
		log:           common.NewLogger("NameNode", "Master"),
		dataNodeAddrs: make(map[string]string),
	}
	return nn
}

// ==================== SAFE MODE ====================

func (nn *NameNode) checkSafeMode() {
	nn.mu.RLock()
	defer nn.mu.RUnlock()

	if !nn.safeMode {
		return
	}

	aliveCount := 0
	for _, node := range nn.nodes {
		if node.Alive {
			aliveCount++
		}
	}

	if aliveCount >= nn.expectedNodes {
		nn.mu.RUnlock()
		nn.mu.Lock()
		nn.safeMode = false
		nn.mu.Unlock()
		nn.mu.RLock()
		nn.log.Event("🟢 SAFE MODE EXITED — All %d/%d DataNodes are online! Cluster is READY for read/write operations.", aliveCount, nn.expectedNodes)
	} else {
		nn.log.Info("⏳ Safe Mode: %d/%d DataNodes online. Waiting for all nodes...", aliveCount, nn.expectedNodes)
	}
}

// ==================== HEARTBEAT HANDLER ====================

func (nn *NameNode) handleHeartbeat(hb common.HeartbeatMsg) {
	nn.mu.Lock()
	defer nn.mu.Unlock()

	node, exists := nn.nodes[hb.NodeID]
	if !exists {
		node = &common.NodeInfo{
			NodeID: hb.NodeID,
			Rack:   hb.Rack,
			Alive:  true,
			Blocks: []string{},
		}
		nn.nodes[hb.NodeID] = node
		nn.log.Event("🆕 New DataNode registered: %s (Rack: %s)", hb.NodeID, hb.Rack)
	}

	wasAlive := node.Alive
	node.Alive = true
	node.LastHeartbeat = hb.Timestamp
	node.Rack = hb.Rack
	node.UsedBytes = hb.UsedBytes
	node.TotalBytes = hb.TotalBytes
	node.Blocks = hb.BlockIDs

	if !wasAlive {
		nn.log.Success("💚 DataNode %s is BACK ONLINE (Rack: %s)", hb.NodeID, hb.Rack)
	}

	// Update block-to-node mapping from block report
	for _, blockID := range hb.BlockIDs {
		found := false
		for _, nid := range nn.blockToNodes[blockID] {
			if nid == hb.NodeID {
				found = true
				break
			}
		}
		if !found {
			nn.blockToNodes[blockID] = append(nn.blockToNodes[blockID], hb.NodeID)
		}
	}

	nn.log.Info("💓 Heartbeat from %s (Rack: %s) | Blocks: %d | Storage: %d/%d bytes",
		hb.NodeID, hb.Rack, len(hb.BlockIDs), hb.UsedBytes, hb.TotalBytes)

	// Store address
	if addr, ok := nn.dataNodeAddrs[hb.NodeID]; ok {
		_ = addr // already stored
	}
}

// ==================== DEAD NODE DETECTION ====================

func (nn *NameNode) monitorHeartbeats() {
	ticker := time.NewTicker(common.HeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		nn.mu.Lock()
		now := time.Now()
		for nodeID, node := range nn.nodes {
			if node.Alive && now.Sub(node.LastHeartbeat) > common.DeadNodeTimeout {
				node.Alive = false
				nn.log.Event("💀 DataNode %s DECLARED DEAD — No heartbeat for %v (Rack: %s)",
					nodeID, common.DeadNodeTimeout, node.Rack)
				nn.log.Warn("🔄 Triggering re-replication for blocks on dead node %s...", nodeID)

				// Find blocks that need re-replication
				deadBlocks := make([]string, len(node.Blocks))
				copy(deadBlocks, node.Blocks)

				// Remove dead node from block locations
				for _, blockID := range deadBlocks {
					newNodes := []string{}
					for _, nid := range nn.blockToNodes[blockID] {
						if nid != nodeID {
							newNodes = append(newNodes, nid)
						}
					}
					nn.blockToNodes[blockID] = newNodes
				}

				nn.mu.Unlock()
				// Trigger re-replication
				nn.reReplicateBlocks(deadBlocks)
				nn.mu.Lock()
			}
		}
		nn.mu.Unlock()

		// Check safe mode after heartbeat scan
		nn.checkSafeMode()
	}
}

// ==================== RE-REPLICATION ====================

func (nn *NameNode) reReplicateBlocks(blockIDs []string) {
	nn.mu.RLock()
	defer nn.mu.RUnlock()

	for _, blockID := range blockIDs {
		currentNodes := nn.blockToNodes[blockID]
		currentCount := len(currentNodes)
		needed := common.ReplicationFactor - currentCount

		if needed <= 0 {
			nn.log.Info("  Block %s already has %d replicas, no re-replication needed", blockID, currentCount)
			continue
		}

		nn.log.Warn("  Block %s: %d replicas remaining, need %d more", blockID, currentCount, needed)

		// Find target nodes using rack awareness
		targets := nn.pickReplicationTargets(currentNodes, needed)
		if len(targets) == 0 {
			nn.log.Error("  ❌ Cannot find targets for re-replicating block %s", blockID)
			continue
		}

		// Pick a source node
		if len(currentNodes) > 0 {
			sourceNode := currentNodes[0]
			sourceAddr := nn.dataNodeAddrs[sourceNode]

			targetAddrs := []string{}
			for _, t := range targets {
				targetAddrs = append(targetAddrs, nn.dataNodeAddrs[t])
			}

			nn.log.Info("  📦 Re-replicate block %s: %s → %v", blockID, sourceNode, targets)

			// Send replicate command to source node
			go nn.sendReplicateCommand(sourceAddr, blockID, targets, targetAddrs)
		}
	}
}

func (nn *NameNode) sendReplicateCommand(sourceAddr, blockID string, targets, targetAddrs []string) {
	conn, err := net.DialTimeout("tcp", sourceAddr, 5*time.Second)
	if err != nil {
		nn.log.Error("Failed to connect to %s for re-replication: %v", sourceAddr, err)
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
		From:    "NameNode",
		Payload: cmd,
	}

	json.NewEncoder(conn).Encode(msg)
	nn.log.Success("  ✅ Replicate command sent for block %s to %s", blockID, sourceAddr)
}

// ==================== BLOCK PLACEMENT ALGORITHM (RACK-AWARE) ====================

func (nn *NameNode) pickBlockPlacement(clientRack string) []string {
	nn.mu.RLock()
	defer nn.mu.RUnlock()

	// Categorize alive nodes by rack
	rackNodes := make(map[string][]string)
	for nodeID, node := range nn.nodes {
		if node.Alive {
			rackNodes[node.Rack] = append(rackNodes[node.Rack], nodeID)
		}
	}

	var targets []string

	// Log the placement algorithm process
	nn.log.Info("🏗️  Block Placement Algorithm starting...")
	nn.log.Info("   Available nodes by rack:")
	for rack, nodes := range rackNodes {
		nn.log.Info("     %s: %v", rack, nodes)
	}

	// Step 1: First replica on the same rack as client (or any rack if client rack unknown)
	localRack := clientRack
	if localRack == "" {
		for rack := range rackNodes {
			localRack = rack
			break
		}
	}

	if nodes, ok := rackNodes[localRack]; ok && len(nodes) > 0 {
		idx := rand.Intn(len(nodes))
		targets = append(targets, nodes[idx])
		nn.log.Info("   Replica 1 → %s (local rack: %s)", nodes[idx], localRack)
		// Remove used node
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
		nn.log.Info("   Replica 2 → %s (remote rack: %s) ← Cross-rack for fault tolerance", nodes[idx], remoteRack)
		rackNodes[remoteRack] = append(nodes[:idx], nodes[idx+1:]...)
	}

	// Step 3: Third replica on the SAME remote rack, DIFFERENT node
	if remoteRack != "" && len(rackNodes[remoteRack]) > 0 {
		nodes := rackNodes[remoteRack]
		idx := rand.Intn(len(nodes))
		targets = append(targets, nodes[idx])
		nn.log.Info("   Replica 3 → %s (same remote rack: %s) ← Same rack, different node", nodes[idx], remoteRack)
	} else {
		// Fallback: any available node
		for rack, nodes := range rackNodes {
			if len(nodes) > 0 {
				idx := rand.Intn(len(nodes))
				targets = append(targets, nodes[idx])
				nn.log.Info("   Replica 3 → %s (fallback rack: %s)", nodes[idx], rack)
				break
			}
		}
	}

	nn.log.Success("🏗️  Block placement decided: %v", targets)
	return targets
}

func (nn *NameNode) pickReplicationTargets(excludeNodes []string, count int) []string {
	excluded := make(map[string]bool)
	for _, n := range excludeNodes {
		excluded[n] = true
	}

	// Get racks of existing replicas
	existingRacks := make(map[string]bool)
	for _, n := range excludeNodes {
		if node, ok := nn.nodes[n]; ok {
			existingRacks[node.Rack] = true
		}
	}

	var targets []string

	// Prefer nodes from racks that don't have a replica yet
	for nodeID, node := range nn.nodes {
		if len(targets) >= count {
			break
		}
		if !excluded[nodeID] && node.Alive && !existingRacks[node.Rack] {
			targets = append(targets, nodeID)
		}
	}

	// If still need more, pick from any alive node
	for nodeID, node := range nn.nodes {
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

// ==================== FILE UPLOAD ====================

func (nn *NameNode) handleUpload(conn net.Conn, payload json.RawMessage) {
	var upload common.UploadPayload
	if err := json.Unmarshal(payload, &upload); err != nil {
		nn.log.Error("Failed to parse upload payload: %v", err)
		sendResponse(conn, false, "Invalid upload payload", nil)
		return
	}

	nn.mu.RLock()
	inSafe := nn.safeMode
	nn.mu.RUnlock()

	if inSafe {
		nn.log.Warn("🚫 Upload rejected — cluster is in SAFE MODE")
		sendResponse(conn, false, "Cluster is in Safe Mode. Cannot write.", nil)
		return
	}

	nn.log.Event("📤 FILE UPLOAD: '%s' (%d bytes)", upload.FileName, len(upload.Data))

	// Split file into blocks
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

		blockID := fmt.Sprintf("%s_block_%d", upload.FileName, blockNum)
		checksum := common.ComputeChecksum(chunk)

		nn.log.Info("  📦 Block %d: ID=%s Size=%d bytes SHA256=%s...", blockNum, blockID, len(chunk), checksum[:16])

		// Pick placement using rack-aware algorithm
		targets := nn.pickBlockPlacement(common.RackA) // client is on rack A (NameNode's rack)

		if len(targets) == 0 {
			nn.log.Error("  ❌ No available DataNodes for block %s", blockID)
			sendResponse(conn, false, "No available DataNodes", nil)
			return
		}

		block := common.Block{
			BlockID:  blockID,
			Data:     chunk,
			Checksum: checksum,
		}

		// Send block to first target, which pipelines to the rest
		nn.mu.RLock()
		firstTarget := targets[0]
		firstAddr := nn.dataNodeAddrs[firstTarget]
		remainingTargets := targets[1:]
		remainingAddrs := []string{}
		for _, t := range remainingTargets {
			remainingAddrs = append(remainingAddrs, nn.dataNodeAddrs[t])
		}
		nn.mu.RUnlock()

		nn.log.Info("  📡 Sending block %s to pipeline: %v", blockID, targets)

		err := nn.sendBlockToDataNode(firstAddr, block, upload.FileName, remainingTargets, remainingAddrs)
		if err != nil {
			nn.log.Error("  ❌ Failed to send block %s to %s: %v", blockID, firstTarget, err)
			sendResponse(conn, false, fmt.Sprintf("Failed to store block %s", blockID), nil)
			return
		}

		blockIDs = append(blockIDs, blockID)
		blockLocations[blockID] = targets
		blockNum++

		nn.log.Success("  ✅ Block %s stored across %v", blockID, targets)
	}

	// Store metadata
	nn.mu.Lock()
	nn.files[upload.FileName] = &common.FileMetadata{
		FileName:       upload.FileName,
		FileSize:       int64(len(upload.Data)),
		BlockIDs:       blockIDs,
		BlockLocations: blockLocations,
		CreatedAt:      time.Now(),
	}
	for blockID, nodes := range blockLocations {
		nn.blockToNodes[blockID] = nodes
	}
	nn.mu.Unlock()

	nn.log.Success("📤 FILE UPLOAD COMPLETE: '%s' → %d blocks, replication factor=%d", upload.FileName, len(blockIDs), common.ReplicationFactor)

	// Save metadata to disk
	nn.saveMetadata()

	sendResponse(conn, true, fmt.Sprintf("File '%s' uploaded: %d blocks across cluster", upload.FileName, len(blockIDs)), nil)
}

func (nn *NameNode) sendBlockToDataNode(addr string, block common.Block, fileName string, targets []string, targetAddrs []string) error {
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
		From:    "NameNode",
		Payload: payload,
	}

	return json.NewEncoder(conn).Encode(msg)
}

// ==================== FILE READ ====================

func (nn *NameNode) handleRead(conn net.Conn, payload json.RawMessage) {
	var readReq common.ReadPayload
	if err := json.Unmarshal(payload, &readReq); err != nil {
		nn.log.Error("Failed to parse read payload: %v", err)
		sendResponse(conn, false, "Invalid read payload", nil)
		return
	}

	nn.mu.RLock()
	fileMeta, exists := nn.files[readReq.FileName]
	nn.mu.RUnlock()

	if !exists {
		nn.log.Warn("📖 File '%s' not found", readReq.FileName)
		sendResponse(conn, false, fmt.Sprintf("File '%s' not found", readReq.FileName), nil)
		return
	}

	nn.log.Event("📖 FILE READ: '%s' (%d blocks)", readReq.FileName, len(fileMeta.BlockIDs))

	var fullData []byte
	for i, blockID := range fileMeta.BlockIDs {
		nn.mu.RLock()
		nodeIDs := nn.blockToNodes[blockID]
		nn.mu.RUnlock()

		if len(nodeIDs) == 0 {
			nn.log.Error("  ❌ No nodes have block %s", blockID)
			sendResponse(conn, false, fmt.Sprintf("Block %s is unavailable", blockID), nil)
			return
		}

		// Try each node until we get a valid block
		var blockData []byte
		var fetchErr error
		for _, nodeID := range nodeIDs {
			nn.mu.RLock()
			addr := nn.dataNodeAddrs[nodeID]
			nn.mu.RUnlock()

			nn.log.Info("  📥 Reading block %d (%s) from %s...", i, blockID, nodeID)
			blockData, fetchErr = nn.fetchBlockFromDataNode(addr, blockID)
			if fetchErr == nil {
				nn.log.Success("  ✅ Block %s read from %s (%d bytes)", blockID, nodeID, len(blockData))
				break
			}
			nn.log.Warn("  ⚠️ Failed to read block %s from %s: %v, trying next replica...", blockID, nodeID, fetchErr)
		}

		if fetchErr != nil {
			nn.log.Error("  ❌ All replicas for block %s are unavailable", blockID)
			sendResponse(conn, false, fmt.Sprintf("Block %s unreadable from all replicas", blockID), nil)
			return
		}

		fullData = append(fullData, blockData...)
	}

	nn.log.Success("📖 FILE READ COMPLETE: '%s' (%d bytes reconstructed from %d blocks)", readReq.FileName, len(fullData), len(fileMeta.BlockIDs))
	sendResponse(conn, true, "File read successful", fullData)
}

func (nn *NameNode) fetchBlockFromDataNode(addr, blockID string) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	msg := common.Message{
		Type:    common.MsgReadFile,
		From:    "NameNode",
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

// ==================== BALANCER ====================

func (nn *NameNode) runBalancer() {
	ticker := time.NewTicker(common.BalancerInterval)
	defer ticker.Stop()

	for range ticker.C {
		nn.mu.RLock()
		if nn.safeMode {
			nn.mu.RUnlock()
			continue
		}

		type nodeUtil struct {
			nodeID string
			ratio  float64
			used   int64
			total  int64
		}

		var utils []nodeUtil
		for nodeID, node := range nn.nodes {
			if node.Alive && node.TotalBytes > 0 {
				ratio := float64(node.UsedBytes) / float64(node.TotalBytes)
				utils = append(utils, nodeUtil{nodeID, ratio, node.UsedBytes, node.TotalBytes})
			}
		}
		nn.mu.RUnlock()

		if len(utils) < 2 {
			continue
		}

		sort.Slice(utils, func(i, j int) bool { return utils[i].ratio > utils[j].ratio })

		maxUtil := utils[0]
		minUtil := utils[len(utils)-1]

		if maxUtil.ratio-minUtil.ratio > common.BalanceThreshold {
			nn.log.Event("⚖️  BALANCER: Imbalance detected! %s=%.0f%% vs %s=%.0f%%",
				maxUtil.nodeID, maxUtil.ratio*100, minUtil.nodeID, minUtil.ratio*100)

			nn.mu.RLock()
			overNode := nn.nodes[maxUtil.nodeID]
			if overNode != nil && len(overNode.Blocks) > 0 {
				blockToMove := overNode.Blocks[0]
				nn.log.Info("  ⚖️  Moving block %s from %s → %s", blockToMove, maxUtil.nodeID, minUtil.nodeID)
			}
			nn.mu.RUnlock()
		} else {
			nn.log.Info("⚖️  Balancer: Cluster is balanced (spread: %.1f%%)", (maxUtil.ratio-minUtil.ratio)*100)
		}
	}
}

// ==================== METADATA PERSISTENCE ====================

func (nn *NameNode) saveMetadata() {
	nn.mu.RLock()
	snapshot := common.MetadataSnapshot{
		Files:     nn.files,
		Nodes:     nn.nodes,
		Timestamp: time.Now(),
	}
	nn.mu.RUnlock()

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		nn.log.Error("Failed to marshal metadata: %v", err)
		return
	}

	if err := os.WriteFile("fsimage.json", data, 0644); err != nil {
		nn.log.Error("Failed to save fsimage.json: %v", err)
		return
	}

	nn.log.Info("💾 Metadata saved to fsimage.json (%d files, %d nodes)", len(snapshot.Files), len(snapshot.Nodes))
}

func (nn *NameNode) loadMetadata() {
	data, err := os.ReadFile("fsimage.json")
	if err != nil {
		nn.log.Info("No existing fsimage.json found, starting fresh")
		return
	}

	var snapshot common.MetadataSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		nn.log.Warn("Failed to parse fsimage.json: %v", err)
		return
	}

	nn.mu.Lock()
	nn.files = snapshot.Files
	if nn.files == nil {
		nn.files = make(map[string]*common.FileMetadata)
	}
	// Rebuild block-to-nodes mapping
	for _, file := range nn.files {
		for blockID, nodeIDs := range file.BlockLocations {
			nn.blockToNodes[blockID] = nodeIDs
		}
	}
	nn.mu.Unlock()

	nn.log.Success("💾 Loaded metadata from fsimage.json (%d files)", len(nn.files))
}

// ==================== CONNECTION HANDLER ====================

func (nn *NameNode) handleConnection(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)

	var rawMsg json.RawMessage
	if err := decoder.Decode(&rawMsg); err != nil {
		return
	}

	// Parse just the envelope to get the type
	var envelope struct {
		Type    string          `json:"type"`
		From    string          `json:"from"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(rawMsg, &envelope); err != nil {
		nn.log.Error("Failed to parse message envelope: %v", err)
		return
	}

	switch envelope.Type {
	case common.MsgHeartbeat:
		var hb common.HeartbeatMsg
		if err := json.Unmarshal(envelope.Payload, &hb); err != nil {
			nn.log.Error("Failed to parse heartbeat: %v", err)
			return
		}
		// Store the remote address for this datanode
		remoteAddr := conn.RemoteAddr().String()
		host, _, _ := net.SplitHostPort(remoteAddr)
		nn.mu.Lock()
		// The DataNode listens on its own port, we need to know that port
		// Convention: DataNodes listen on port 9000 + their index
		if _, exists := nn.dataNodeAddrs[hb.NodeID]; !exists {
			// Default: assume DataNode listens on port 9001
			nn.dataNodeAddrs[hb.NodeID] = host + ":9001"
		}
		nn.mu.Unlock()
		nn.handleHeartbeat(hb)

	case common.MsgUploadFile:
		nn.handleUpload(conn, envelope.Payload)

	case common.MsgReadFile:
		nn.handleRead(conn, envelope.Payload)

	case common.MsgGetMetadata:
		nn.mu.RLock()
		snapshot := common.MetadataSnapshot{
			Files:     nn.files,
			Nodes:     nn.nodes,
			Timestamp: time.Now(),
		}
		nn.mu.RUnlock()
		json.NewEncoder(conn).Encode(snapshot)
		nn.log.Info("📋 Metadata snapshot sent to %s", envelope.From)

	default:
		nn.log.Warn("Unknown message type: %s from %s", envelope.Type, envelope.From)
	}
}

// ==================== STATUS DISPLAY ====================

func (nn *NameNode) printStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		nn.mu.RLock()
		nn.log.Info("─── CLUSTER STATUS ───")
		if nn.safeMode {
			nn.log.Warn("  Mode: 🔒 SAFE MODE (read-only)")
		} else {
			nn.log.Success("  Mode: 🟢 ACTIVE (read-write)")
		}

		aliveCount := 0
		for nodeID, node := range nn.nodes {
			status := "🟢 ALIVE"
			if !node.Alive {
				status = "🔴 DEAD"
			} else {
				aliveCount++
			}
			nn.log.Info("  %s %s (Rack: %s, Blocks: %d, LastHB: %s)",
				status, nodeID, node.Rack, len(node.Blocks),
				node.LastHeartbeat.Format("15:04:05"))
		}
		nn.log.Info("  Files: %d | Live Nodes: %d/%d", len(nn.files), aliveCount, len(nn.nodes))
		nn.log.Info("──────────────────────")
		nn.mu.RUnlock()
	}
}

// ==================== BUILT-IN CLI ====================

func (nn *NameNode) runCLI() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println()
	nn.log.Info("📝 Interactive CLI ready. Commands: upload <file>, read <file>, status, files, exit")

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
				nn.log.Error("Cannot read file %s: %v", filePath, err)
				continue
			}
			// Create a local connection to self
			conn, err := net.Dial("tcp", "localhost"+nn.listenPort)
			if err != nil {
				nn.log.Error("Self-connect failed: %v", err)
				continue
			}
			payload, _ := json.Marshal(common.UploadPayload{FileName: filePath, Data: data})
			msg := common.Message{Type: common.MsgUploadFile, From: "CLI", Payload: json.RawMessage(payload)}
			json.NewEncoder(conn).Encode(msg)
			var resp common.ResponsePayload
			json.NewDecoder(conn).Decode(&resp)
			conn.Close()
			if resp.Success {
				nn.log.Success("CLI: %s", resp.Message)
			} else {
				nn.log.Error("CLI: %s", resp.Message)
			}

		case "read":
			if len(parts) < 2 {
				fmt.Println("Usage: read <filename>")
				continue
			}
			fileName := parts[1]
			conn, err := net.Dial("tcp", "localhost"+nn.listenPort)
			if err != nil {
				nn.log.Error("Self-connect failed: %v", err)
				continue
			}
			payload, _ := json.Marshal(common.ReadPayload{FileName: fileName})
			msg := common.Message{Type: common.MsgReadFile, From: "CLI", Payload: json.RawMessage(payload)}
			json.NewEncoder(conn).Encode(msg)
			var resp common.ResponsePayload
			json.NewDecoder(conn).Decode(&resp)
			conn.Close()
			if resp.Success {
				nn.log.Success("CLI: File read (%d bytes)", len(resp.Data))
				if len(resp.Data) < 1024 {
					fmt.Printf("Content:\n%s\n", string(resp.Data))
				} else {
					fmt.Printf("Content (first 1024 bytes):\n%s\n...\n", string(resp.Data[:1024]))
				}
			} else {
				nn.log.Error("CLI: %s", resp.Message)
			}

		case "status":
			nn.mu.RLock()
			fmt.Println("\n══════════ CLUSTER STATUS ══════════")
			if nn.safeMode {
				fmt.Println("Mode: 🔒 SAFE MODE (read-only)")
			} else {
				fmt.Println("Mode: 🟢 ACTIVE (read-write)")
			}
			fmt.Println("\nDataNodes:")
			for nodeID, node := range nn.nodes {
				status := "🟢 ALIVE"
				if !node.Alive {
					status = "🔴 DEAD"
				}
				fmt.Printf("  %s %s | Rack: %s | Blocks: %d | Storage: %d/%d bytes | Last HB: %s\n",
					status, nodeID, node.Rack, len(node.Blocks),
					node.UsedBytes, node.TotalBytes,
					node.LastHeartbeat.Format("15:04:05"))
			}
			fmt.Println("════════════════════════════════════")
			nn.mu.RUnlock()

		case "files":
			nn.mu.RLock()
			fmt.Println("\n══════════ FILE LISTING ══════════")
			if len(nn.files) == 0 {
				fmt.Println("  (no files)")
			}
			for name, meta := range nn.files {
				fmt.Printf("  📄 %s (%d bytes, %d blocks)\n", name, meta.FileSize, len(meta.BlockIDs))
				for _, bid := range meta.BlockIDs {
					nodes := meta.BlockLocations[bid]
					fmt.Printf("     └─ %s → %v\n", bid, nodes)
				}
			}
			fmt.Println("══════════════════════════════════")
			nn.mu.RUnlock()

		case "exit", "quit":
			nn.log.Info("Shutting down NameNode...")
			os.Exit(0)

		case "":
			continue

		default:
			fmt.Println("Unknown command. Available: upload <file>, read <file>, status, files, exit")
		}
	}
}

// ==================== HELPER ====================

func sendResponse(conn net.Conn, success bool, message string, data []byte) {
	resp := common.ResponsePayload{
		Success: success,
		Message: message,
		Data:    data,
	}
	json.NewEncoder(conn).Encode(resp)
}

// ==================== MAIN ====================

func main() {
	// Configuration from environment variables
	port := os.Getenv("NAMENODE_PORT")
	if port == "" {
		port = ":8000"
	} else if port[0] != ':' {
		port = ":" + port
	}

	expectedNodesStr := os.Getenv("EXPECTED_DATANODES")
	expectedNodes := 3 // default: 3 DataNodes
	if expectedNodesStr != "" {
		fmt.Sscanf(expectedNodesStr, "%d", &expectedNodes)
	}

	// Parse DataNode addresses from env: DATANODE_ADDRS="DN1=host:port,DN2=host:port"
	dataNodeAddrs := os.Getenv("DATANODE_ADDRS")

	nn := NewNameNode(port, expectedNodes)
	nn.log.BannerWithRack("NAMENODE (MASTER)", common.RackA)
	nn.log.Info("Configuration:")
	nn.log.Info("  Listen Port: %s", port)
	nn.log.Info("  Expected DataNodes: %d", expectedNodes)
	nn.log.Info("  Replication Factor: %d", common.ReplicationFactor)
	nn.log.Info("  Block Size: %d KB", common.BlockSize/1024)
	nn.log.Info("  Dead Node Timeout: %v", common.DeadNodeTimeout)
	nn.log.Info("  Heartbeat Interval: %v", common.HeartbeatInterval)

	// Parse DataNode addresses
	if dataNodeAddrs != "" {
		for _, pair := range strings.Split(dataNodeAddrs, ",") {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) == 2 {
				nn.dataNodeAddrs[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
				nn.log.Info("  DataNode %s → %s", parts[0], parts[1])
			}
		}
	}

	// Load existing metadata
	nn.loadMetadata()

	// Enter safe mode
	nn.log.Event("🔒 ENTERING SAFE MODE — Waiting for %d DataNodes to report in...", expectedNodes)

	// Start the TCP listener
	ln, err := net.Listen("tcp", port)
	if err != nil {
		nn.log.Error("FATAL: Cannot listen on %s: %v", port, err)
		os.Exit(1)
	}
	nn.log.Success("🌐 NameNode TCP listener started on %s", port)

	// Start background goroutines
	go nn.monitorHeartbeats()
	go nn.runBalancer()
	go nn.printStatus()
	go nn.runCLI()

	// Accept connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			nn.log.Error("Accept error: %v", err)
			continue
		}
		go nn.handleConnection(conn)
	}
}
