package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
)

const ChunkSize = 1024 // Extremely small chunk size for demonstration

type UploadRequest struct {
	FileName string `json:"file_name"`
	Data     []byte `json:"data"` // For simplicity, sending entire file. In reality, streamed.
}

type ChunkDataMsg struct {
	ChunkID string `json:"chunk_id"`
	Data    []byte `json:"data"`
}

type ReadMetadataReq struct {
	FileName string `json:"file_name"`
}

type ReadMetadataResp struct {
	FileMetadata FileRecord `json:"file_metadata"`
	Error        string     `json:"error,omitempty"`
}

type ReadChunkReq struct {
	ChunkID string `json:"chunk_id"`
}

// CLI command handler for uploading a file
func (n *Node) uploadFile(filePath string) {
	n.mu.Lock()
	leader := n.LeaderID
	n.mu.Unlock()

	if leader == -1 {
		log.Println("No leader currently elected. Cannot upload.")
		return
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Printf("Failed to read file: %v", err)
		return
	}

	fileName := filepath.Base(filePath)
	reqMsg := UploadRequest{
		FileName: fileName,
		Data:     data,
	}

	log.Printf("Forwarding upload request for %s to Leader %d...", fileName, leader)
	
	if leader == n.ID {
		// Process locally via logic directly
		n.processUploadRequest(reqMsg)
	} else {
		// Forward via HTTP to leader
		_, err := n.sendRPC(leader, "/upload_req", reqMsg)
		if err != nil {
			log.Printf("Upload request failed: %v", err)
		}
	}
}

// Leader HTTP handler for upload requests
func (n *Node) handleUploadRequest(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	if n.LeaderID != n.ID {
		n.mu.Unlock()
		http.Error(w, "Not the leader", http.StatusBadRequest)
		return
	}
	n.mu.Unlock()

	var req UploadRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	err := n.processUploadRequest(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (n *Node) processUploadRequest(req UploadRequest) error {
	log.Printf("[Leader] Processing upload for file: %s (%d bytes)", req.FileName, len(req.Data))

	chunksCount := len(req.Data) / ChunkSize
	if len(req.Data)%ChunkSize != 0 {
		chunksCount++
	}

	record := FileRecord{
		FileName: req.FileName,
		Size:     int64(len(req.Data)),
		Chunks:   make([]ChunkRecord, 0),
	}

	// Identify available datanodes (all peers + self)
	availableNodes := []int{n.ID}
	for id := range n.Peers {
		availableNodes = append(availableNodes, id)
	}

	for i := 0; i < chunksCount; i++ {
		start := i * ChunkSize
		end := start + ChunkSize
		if end > len(req.Data) {
			end = len(req.Data)
		}
		chunkData := req.Data[start:end]
		chunkID := fmt.Sprintf("%s_c%d", req.FileName, i)

		// Select 2 random DataNodes for replication
		rand.Shuffle(len(availableNodes), func(i, j int) {
			availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
		})
		
		numReplicas := 2
		if len(availableNodes) < 2 {
			numReplicas = len(availableNodes)
		}
		replicas := availableNodes[:numReplicas]

		chunkRec := ChunkRecord{
			ChunkID:   chunkID,
			DataNodes: replicas,
		}
		record.Chunks = append(record.Chunks, chunkRec)

		msg := ChunkDataMsg{
			ChunkID: chunkID,
			Data:    chunkData,
		}

		// Distribute chunks
		for _, dn := range replicas {
			if dn == n.ID {
				n.storeChunkLocally(msg)
			} else {
				go func(targetDN int, targetMsg ChunkDataMsg) {
					_, err := n.sendRPC(targetDN, "/receive_chunk", targetMsg)
					if err != nil {
						log.Printf("[Leader] Failed to send chunk %s to DataNode %d: %v", targetMsg.ChunkID, targetDN, err)
					}
				}(dn, msg)
			}
		}
	}


	n.mu.Lock()
	n.FileMetadata[req.FileName] = record
	n.mu.Unlock()

	n.saveFSImage()

	log.Printf("[Leader] File %s successfully chunked and distributed.", req.FileName)
	return nil
}

func (n *Node) saveFSImage() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Save NameNode metadata to a shared cluster fsimage file
	data, err := json.MarshalIndent(n.FileMetadata, "", "  ")
	if err != nil {
		log.Printf("[Leader] Failed to marshal fsimage: %v", err)
		return
	}

	err = os.WriteFile("fsimage.json", data, 0644)
	if err != nil {
		log.Printf("[Leader] Failed to save fsimage.json: %v", err)
		return
	}
	log.Printf("[Leader] fsimage.json updated.")
}

func (n *Node) loadFSImage() {
	n.mu.Lock()
	defer n.mu.Unlock()

	data, err := os.ReadFile("fsimage.json")
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("[Leader] Failed to read existing fsimage.json: %v", err)
		}
		return
	}

	err = json.Unmarshal(data, &n.FileMetadata)
	if err != nil {
		log.Printf("[Leader] Failed to parse fsimage.json: %v", err)
		return
	}
	
	log.Printf("[Leader] Successfully loaded %d records from fsimage.json.", len(n.FileMetadata))
}


// DataNode HTTP handler for receiving a chunk
func (n *Node) handleReceiveChunk(w http.ResponseWriter, r *http.Request) {
	var msg ChunkDataMsg
	_ = json.NewDecoder(r.Body).Decode(&msg)

	err := n.storeChunkLocally(msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (n *Node) storeChunkLocally(msg ChunkDataMsg) error {
	chunkPath := filepath.Join(n.StorageDir, msg.ChunkID)
	err := os.WriteFile(chunkPath, msg.Data, 0644)
	if err != nil {
		log.Printf("Failed to store chunk %s: %v", msg.ChunkID, err)
		return err
	}
	log.Printf("Stored chunk: %s", msg.ChunkID)
	return nil
}

// CLI command handler for reading a file
func (n *Node) readFile(fileName string) {
	n.mu.Lock()
	leader := n.LeaderID
	n.mu.Unlock()

	if leader == -1 {
		log.Println("No leader currently elected. Cannot read.")
		return
	}

	reqMsg := ReadMetadataReq{FileName: fileName}
	
	var metaResp ReadMetadataResp

	log.Printf("Requesting metadata for %s from Leader %d...", fileName, leader)

	if leader == n.ID {
		n.mu.Lock()
		rec, exists := n.FileMetadata[fileName]
		n.mu.Unlock()
		if !exists {
			log.Printf("File %s not found in metadata", fileName)
			return
		}
		metaResp.FileMetadata = rec
	} else {
		respData, err := n.sendRPC(leader, "/read_req", reqMsg)
		if err != nil {
			log.Printf("Failed to request metadata: %v", err)
			return
		}
		json.Unmarshal(respData, &metaResp)
		if metaResp.Error != "" {
			log.Printf("Error from NameNode: %s", metaResp.Error)
			return
		}
	}

	fileData := make([]byte, 0, metaResp.FileMetadata.Size)

	// Fetch all chunks
	for _, chunkRec := range metaResp.FileMetadata.Chunks {
		chunkFetched := false
		for _, dn := range chunkRec.DataNodes {
			reqC := ReadChunkReq{ChunkID: chunkRec.ChunkID}
			
			log.Printf("Fetching chunk %s from DataNode %d...", chunkRec.ChunkID, dn)
			
			if dn == n.ID {
				data, err := os.ReadFile(filepath.Join(n.StorageDir, chunkRec.ChunkID))
				if err == nil {
					fileData = append(fileData, data...)
					chunkFetched = true
					break
				}
			} else {
				data, err := n.sendRPC(dn, "/read_chunk", reqC)
				if err == nil {
					fileData = append(fileData, data...)
					chunkFetched = true
					break
				}
			}
		}

		if !chunkFetched {
			log.Printf("Failed to fetch chunk %s from any replica. File corrupted.", chunkRec.ChunkID)
			return
		}
	}

	outPath := "downloaded_" + fileName
	os.WriteFile(outPath, fileData, 0644)
	log.Printf("Successfully reconstructed file -> %s", outPath)
}

// Leader HTTP handler for reading metadata
func (n *Node) handleReadRequest(w http.ResponseWriter, r *http.Request) {
	var req ReadMetadataReq
	var resp ReadMetadataResp
	
	_ = json.NewDecoder(r.Body).Decode(&req)
	
	n.mu.Lock()
	if n.LeaderID != n.ID {
		n.mu.Unlock()
		resp.Error = "Not the leader"
		json.NewEncoder(w).Encode(resp)
		return
	}
	
	rec, exists := n.FileMetadata[req.FileName]
	n.mu.Unlock()

	if !exists {
		resp.Error = "File not found"
	} else {
		resp.FileMetadata = rec
	}
	json.NewEncoder(w).Encode(resp)
}

// DataNode HTTP handler for serving physical chunk
func (n *Node) handleReadChunk(w http.ResponseWriter, r *http.Request) {
	var req ReadChunkReq
	_ = json.NewDecoder(r.Body).Decode(&req)

	chunkPath := filepath.Join(n.StorageDir, req.ChunkID)
	data, err := os.ReadFile(chunkPath)
	if err != nil {
		http.Error(w, "Chunk not found", http.StatusNotFound)
		return
	}
	w.Write(data)
}
