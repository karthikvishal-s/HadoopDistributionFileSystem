package common

import (
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

// ==================== CONSTANTS ====================

const (
	BlockSize          = 64 * 1024 // 64 KB blocks
	ReplicationFactor  = 3
	HeartbeatInterval  = 3 * time.Second
	DeadNodeTimeout    = 10 * time.Second
	BalancerInterval   = 15 * time.Second
	CheckpointInterval = 10 * time.Second
	SafeModeWait       = 30 * time.Second
	BalanceThreshold   = 0.3 // 30% imbalance triggers balancing
)

// Rack definitions
const (
	RackA = "RackA"
	RackB = "RackB"
)

// Message types
const (
	MsgHeartbeat    = "HEARTBEAT"
	MsgStoreBlock   = "STORE_BLOCK"
	MsgReplicate    = "REPLICATE"
	MsgDeleteBlock  = "DELETE_BLOCK"
	MsgUploadFile   = "UPLOAD_FILE"
	MsgReadFile     = "READ_FILE"
	MsgResponse     = "RESPONSE"
	MsgBlockReport  = "BLOCK_REPORT"
	MsgGetMetadata  = "GET_METADATA"
	MsgElection     = "LEADER_ELECTION"
	MsgNewLeader    = "NEW_LEADER"
	MsgReplicateCmd = "REPLICATE_CMD"
	MsgBalanceMove  = "BALANCE_MOVE"
)

// ==================== DATA STRUCTURES ====================

// Block represents a chunk of data stored in HDFS
type Block struct {
	BlockID  string `json:"block_id"`
	Data     []byte `json:"data"`
	Checksum string `json:"checksum"` // SHA-256 hex hash
}

// Heartbeat message sent from DataNode to NameNode
type HeartbeatMsg struct {
	NodeID     string    `json:"node_id"`
	Rack       string    `json:"rack"`
	Timestamp  time.Time `json:"timestamp"`
	BlockIDs   []string  `json:"block_ids"` // Block report
	UsedBytes  int64     `json:"used_bytes"`
	TotalBytes int64     `json:"total_bytes"`
}

// FileMetadata stored by the NameNode
type FileMetadata struct {
	FileName       string              `json:"file_name"`
	FileSize       int64               `json:"file_size"`
	BlockIDs       []string            `json:"block_ids"`
	BlockLocations map[string][]string `json:"block_locations"` // blockID -> [nodeIDs]
	CreatedAt      time.Time           `json:"created_at"`
}

// Message is the unified envelope for all TCP communication
type Message struct {
	Type    string      `json:"type"`
	From    string      `json:"from"`
	Payload interface{} `json:"payload,omitempty"`
}

// StoreBlockPayload is payload for STORE_BLOCK and REPLICATE commands
type StoreBlockPayload struct {
	Block       Block    `json:"block"`
	Targets     []string `json:"targets"`      // remaining pipeline targets (nodeIDs)
	TargetAddrs []string `json:"target_addrs"` // corresponding addresses
	FileName    string   `json:"file_name"`
}

// UploadPayload is payload for UPLOAD_FILE message
type UploadPayload struct {
	FileName string `json:"file_name"`
	Data     []byte `json:"data"`
}

// ReadPayload is payload for READ_FILE message
type ReadPayload struct {
	FileName string `json:"file_name"`
}

// ResponsePayload is payload for RESPONSE messages
type ResponsePayload struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    []byte `json:"data,omitempty"`
}

// NodeInfo tracks DataNode status at the NameNode
type NodeInfo struct {
	NodeID        string    `json:"node_id"`
	Address       string    `json:"address"`
	Rack          string    `json:"rack"`
	Alive         bool      `json:"alive"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	UsedBytes     int64     `json:"used_bytes"`
	TotalBytes    int64     `json:"total_bytes"`
	Blocks        []string  `json:"blocks"`
}

// MetadataSnapshot for checkpoint / fsimage
type MetadataSnapshot struct {
	Files     map[string]*FileMetadata `json:"files"`
	Nodes     map[string]*NodeInfo     `json:"nodes"`
	Timestamp time.Time                `json:"timestamp"`
}

// ElectionPayload for leader election
type ElectionPayload struct {
	CandidateID string `json:"candidate_id"`
	Address     string `json:"address"`
	Priority    int    `json:"priority"`
}

// ReplicateCommand is sent from NameNode to tell a DataNode to copy a block to targets
type ReplicateCommand struct {
	BlockID     string   `json:"block_id"`
	TargetNodes []string `json:"target_nodes"`
	TargetAddrs []string `json:"target_addrs"`
}

// BalanceMovePayload for block balancing
type BalanceMovePayload struct {
	BlockID    string `json:"block_id"`
	SourceNode string `json:"source_node"`
	TargetNode string `json:"target_node"`
	TargetAddr string `json:"target_addr"`
}

// ==================== SHA-256 CHECKSUM ====================

// ComputeChecksum calculates the SHA-256 hash of data and returns it as a hex string
func ComputeChecksum(data []byte) string {
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// VerifyChecksum checks if data matches the expected SHA-256 checksum
func VerifyChecksum(data []byte, expected string) bool {
	computed := ComputeChecksum(data)
	return computed == expected
}

// ==================== COLORED LOGGING ====================

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
	colorBold   = "\033[1m"
)

// Logger wraps the standard logger with colored, tagged output
type Logger struct {
	component string
	nodeID    string
	logger    *log.Logger
}

// NewLogger creates a new colored logger for a specific component
func NewLogger(component, nodeID string) *Logger {
	return &Logger{
		component: component,
		nodeID:    nodeID,
		logger:    log.New(os.Stdout, "", 0),
	}
}

func (l *Logger) formatPrefix(level, color string) string {
	timestamp := time.Now().Format("15:04:05.000")
	return fmt.Sprintf("%s[%s]%s %s%s%-5s%s %s[%s/%s]%s ",
		colorCyan, timestamp, colorReset,
		color, colorBold, level, colorReset,
		colorPurple, l.component, l.nodeID, colorReset)
}

// Info logs an informational message
func (l *Logger) Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("%s%s", l.formatPrefix("INFO", colorBlue), msg)
}

// Success logs a success message
func (l *Logger) Success(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("%s%s%s%s", l.formatPrefix("OK", colorGreen), colorGreen, msg, colorReset)
}

// Warn logs a warning message
func (l *Logger) Warn(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("%s%s%s%s", l.formatPrefix("WARN", colorYellow), colorYellow, msg, colorReset)
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	l.logger.Printf("%s%s%s%s", l.formatPrefix("ERR", colorRed), colorRed, msg, colorReset)
}

// Event logs a significant system event
func (l *Logger) Event(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	separator := strings.Repeat("─", 60)
	l.logger.Printf("%s%s%s", colorYellow, separator, colorReset)
	l.logger.Printf("%s%s%s%s", l.formatPrefix("EVENT", colorYellow), colorBold, msg, colorReset)
	l.logger.Printf("%s%s%s", colorYellow, separator, colorReset)
}

// Banner prints a startup banner
func (l *Logger) Banner(role string) {
	border := strings.Repeat("═", 60)
	l.logger.Printf("\n%s%s%s%s", colorCyan, colorBold, border, colorReset)
	l.logger.Printf("%s%s  HDFS SIMULATION — %s%s", colorCyan, colorBold, role, colorReset)
	l.logger.Printf("%s%s  Node: %-20s Rack: TBD%s", colorCyan, colorBold, l.nodeID, colorReset)
	l.logger.Printf("%s%s  Time: %s%s", colorCyan, colorBold, time.Now().Format("2006-01-02 15:04:05"), colorReset)
	l.logger.Printf("%s%s%s%s\n", colorCyan, colorBold, border, colorReset)
}

// BannerWithRack prints a startup banner with rack info
func (l *Logger) BannerWithRack(role, rack string) {
	border := strings.Repeat("═", 60)
	l.logger.Printf("\n%s%s%s%s", colorCyan, colorBold, border, colorReset)
	l.logger.Printf("%s%s  HDFS SIMULATION — %s%s", colorCyan, colorBold, role, colorReset)
	l.logger.Printf("%s%s  Node: %-20s Rack: %s%s", colorCyan, colorBold, l.nodeID, rack, colorReset)
	l.logger.Printf("%s%s  Time: %s%s", colorCyan, colorBold, time.Now().Format("2006-01-02 15:04:05"), colorReset)
	l.logger.Printf("%s%s%s%s\n", colorCyan, colorBold, border, colorReset)
}
