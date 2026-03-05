# Laptop 3 — DataNode 1 — Rack B
# Replace <LAPTOP1_IP> with Laptop 1's IP address

$env:NODE_ID = "DataNode1"
$env:MY_RACK = "RackB"
$env:LISTEN_PORT = "9002"
$env:NAMENODE_ADDR = "<LAPTOP1_IP>:8000"

Write-Host "Environment set for Laptop 3 (DataNode 1). Starting..."
go run ./cmd/datanode1/
