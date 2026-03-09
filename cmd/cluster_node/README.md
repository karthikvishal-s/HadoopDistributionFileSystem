# Distributed HDFS Demonstration

This module implements a minimal distributed file system across **4 nodes**. It showcases:
1. **Bully Algorithm** for Leader Election (Highest ID becomes Leader/NameNode).
2. **HDFS File Chunking and Distribution** (NameNode routes file chunks to DataNodes).
3. **Chandy-Lamport Inspired Distributed Snapshot** (NameNode initiates a cluster-wide state capture).

## Setup & Compilation

You can run this on 4 different computers in the same network, or on 4 different terminal windows on the same computer.

1. First, make sure you have Go installed on the machines.
2. Compile the binary on the machine (or compile once and distribute the `cluster_node` executable to all 4 computers):
   ```sh
   go build -o cluster_node
   ```
3. Update `config.json`. The included `config.json` is set to `localhost`. If running on 4 separate computers, change `localhost` to the actual IP addresses of those computers:
   ```json
   [
     { "id": 1, "address": "http://<IP_OF_PC1>:8001" },
     { "id": 2, "address": "http://<IP_OF_PC2>:8002" },
     { "id": 3, "address": "http://<IP_OF_PC3>:8003" },
     { "id": 4, "address": "http://<IP_OF_PC4>:8004" }
   ]
   ```
   *Make sure all 4 computers have the same `config.json` file in the same directory as the executable.*

## Execution Steps

### Step 1: Start the Nodes
On each of the 4 computers (or terminal windows), start the respective node by giving it its ID (1, 2, 3, or 4).

**Computer 1:**
```sh
./cluster_node -id 1 -config config.json
```
**Computer 2:**
```sh
./cluster_node -id 2 -config config.json
```
**Computer 3:**
```sh
./cluster_node -id 3 -config config.json
```
**Computer 4:**
```sh
./cluster_node -id 4 -config config.json
```

---

### Step 2: Bully Algorithm Evaluation
Once started, the nodes will wait a couple seconds and start the Bully Election.
Watch the terminal outputs.
- Nodes 1, 2, and 3 will send election messages.
- Node 4 (having the highest ID) will announce `I am the new Leader (NameNode)`.
- The other nodes will print `Acknowledged Node 4 as the new Leader (NameNode)`.
- You can run the `status` command on any node's terminal to verify who the leader is.

---

### Step 3: HDFS Upload and Read Evaluation
Let's upload a file from Node 1.

1. First, create a dummy file on Node 1's computer:
   ```sh
   echo "This is some test data for our distributed HDFS simulation." > mydata.txt
   ```
2. In **Node 1's interactive terminal**, type:
   ```
   upload mydata.txt
   ```
3. Watch the terminals:
   - Node 1 will say it's forwarding the upload to leader (Node 4).
   - Node 4 will say it's chunking the file and distributing it to 2 DataNodes.
   - The selected DataNodes will print `Stored chunk: mydata.txt_c0`.
4. Now, go to **Node 2's terminal** and read the file:
   ```
   read mydata.txt
   ```
5. Node 2 will ask the Leader (Node 4) for metadata, contact the target DataNodes, fetch the chunks, and reconstruct it into `downloaded_mydata.txt`.

---

### Step 4: Distributed Snapshot (Chandy-Lamport Inspired)
1. In the **Leader's terminal (Node 4)**, run the snapshot command:
   ```
   snapshot
   ```
2. What happens:
   - Node 4 records its own local state (HDFS metadata and local blocks).
   - Node 4 broadcasts a Snapshot Marker to Nodes 1, 2, and 3.
   - Nodes 1, 2, and 3 instantly record their local storage state and reply to leader.
   - The Leader aggregates this and prints a beautifully formatted **GLOBAL SNAPSHOT** to its terminal, showing exactly which files exist and which chunks are on which nodes.

---

### Step 5: Fault Tolerance (Leader Death & Re-Election)
1. Go to the **Leader's terminal (Node 4)** and type:
   ```
   kill
   ```
   (This forcefully exits the program).
2. Watch Nodes 1, 2, and 3:
   - They have a background ping heartbeat to the leader.
   - Within 3 seconds, they will notice Node 4 is dead.
   - A new Bully Election is triggered.
   - Node 3 (the new highest ID) will declare itself the Leader.
   - Nodes 1 and 2 will acknowledge Node 3.
